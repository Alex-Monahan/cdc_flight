#!/usr/bin/env python3
"""Own one disposable runtime with one retained, cooperative authority.

The authority is a lock on the physical project directory plus descriptor-relative
project, runtime-parent and instance handles. Every mutator holds that lock for its full
lifetime. ``run`` passes the project descriptor to its child, so wrapper death cannot
release the authority while the child still owns the runtime.

This is intentionally a cooperative-writer guarantee. Ordinary POSIX descriptors and
pre-operation ``stat`` calls cannot prove that an out-of-band same-user rename stays
under a directory after the last check and before a syscall. The binding checks remain
fail-closed diagnostics; the enforceable boundary is the inherited lock.
"""

# ruff: noqa: E402, I001 -- standalone helper adds sibling and project src roots.

from __future__ import annotations

import argparse
import fcntl
import os
import re
import secrets
import subprocess
import sys
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from runtime_state_fs import (
    DIR_FLAGS,
    RUNTIME_PARENT_NAME,
    Identity,
    Authority as _Authority,
    close_root as _close_root,
    delete_tree as _delete_tree_impl,
    identity as _identity,
    read as _read,
    read_at as _read_at,
    rename_root as _rename_root,
    root_fd as _root_fd,
    scan as _scan,
    set_root as _set_root,
    validate_tree as _validate_tree_impl,
    write as _write,
    write_exclusive as _write_exclusive,
)
from runtime_state_publish import rename_noreplace

from cdc_flight.machines import (
    ROOT_ABSENT,
    ROOT_ACTIVE,
    ROOT_COMPLETION_RECORDED,
    ROOT_DELETED_RECORDED,
    ROOT_PROVISIONING,
    ROOT_QUARANTINING,
    RUNTIME_ROOT_LIFECYCLE,
)

SENTINEL_NAME = ".cdc_flight_disposable_runtime"
QUARANTINE_NAME = ".cdc_flight_runtime_quarantining"
COMPLETION_PREFIX = ".cdc_flight_runtime_completion."
META = {SENTINEL_NAME, QUARANTINE_NAME}
Refusal = RuntimeError


@dataclass
class _Lifecycle:
    """The sole writer of one classified runtime-root lifecycle state."""

    state: str

    def __post_init__(self) -> None:
        self.state = RUNTIME_ROOT_LIFECYCLE.parse(self.state)

    def to(self, target: str) -> None:
        RUNTIME_ROOT_LIFECYCLE.check(self.state, target)
        self.state = target


def _checkpoint(name: str, path: Path) -> None:
    pass


def _validate_tree(fd: int, display: Path, authority: _Authority) -> None:
    _validate_tree_impl(
        fd, display, authority, metadata=META, checkpoint=_checkpoint
    )


def _delete_tree(fd: int, display: Path, authority: _Authority) -> None:
    _delete_tree_impl(
        fd, display, authority, metadata=META, checkpoint=_checkpoint
    )


def _sentinel(instance_id: str) -> bytes:
    return (
        "cdc_flight disposable runtime state\n" f"instance={instance_id}\n"
    ).encode()


def _provision(
    authority: _Authority,
    lifecycle: _Lifecycle,
    instance_id: str,
    display: Path,
) -> None:
    authority.check("before provisioning")
    _checkpoint("before_target_mkdir", display)
    private_name = (
        f".{instance_id}.provision.{os.getpid()}.{secrets.token_hex(8)}"
    )
    try:
        os.mkdir(private_name, 0o700, dir_fd=authority.parent_fd)
    except FileExistsError as exc:
        raise Refusal(f"cannot allocate private runtime directory for {display}") from exc
    lifecycle.to(ROOT_PROVISIONING)

    # mkdir has no portable fd-returning form. The retained project lock makes the
    # mkdir -> open -> fstat sequence one authority critical section for cooperating
    # writers; the fd, not a later public-name stat, owns all marking and publication.
    try:
        fd = os.open(private_name, DIR_FLAGS, dir_fd=authority.parent_fd)
    except BaseException:
        with suppress(OSError):
            os.rmdir(private_name, dir_fd=authority.parent_fd)
        raise
    _set_root(authority, private_name, fd, _identity(fd), display)
    try:
        authority.check("while opening")
        if _scan(_root_fd(authority)):
            raise Refusal(f"created runtime directory was replaced before marking: {display}")
        _write(authority, SENTINEL_NAME, _sentinel(instance_id))
        os.fsync(_root_fd(authority))
        _checkpoint("before_target_publish", display)
        try:
            rename_noreplace(authority.parent_fd, private_name, instance_id)
        except FileExistsError as exc:
            raise Refusal(
                f"cannot provision {display}: existing root is not adopted; "
                "verify its sentinel"
            ) from exc
        _rename_root(authority, instance_id, display)
        os.fsync(authority.parent_fd)
        _checkpoint("after_target_mkdir", display)
        authority.check("after publication")
        lifecycle.to(ROOT_ACTIVE)
    except BaseException as original:
        try:
            authority.check("during rollback")
        except BaseException:
            _close_root(authority)
            raise original from None
        for metadata in META:
            with suppress(FileNotFoundError):
                os.unlink(metadata, dir_fd=_root_fd(authority))
        with suppress(OSError):
            os.rmdir(authority.root_name, dir_fd=authority.parent_fd)
        if lifecycle.state == ROOT_PROVISIONING:
            lifecycle.to(ROOT_ABSENT)
        _close_root(authority)
        raise


def _record_name(instance_id: str) -> str:
    return f"{COMPLETION_PREFIX}{instance_id}"


def _record_data(authority: _Authority) -> bytes:
    if authority.root_name is None or authority.root_identity is None:
        raise Refusal("cannot record an unselected runtime root")
    dev, ino = authority.root_identity
    return f"version=1\ninstance={authority.root_name}\ndev={dev}\nino={ino}\n".encode()


def _parse_record(data: bytes, instance_id: str, display: Path) -> Identity:
    try:
        fields = dict(line.split("=", 1) for line in data.decode().splitlines())
        if fields != {
            "version": "1",
            "instance": instance_id,
            "dev": fields["dev"],
            "ino": fields["ino"],
        }:
            raise ValueError("unexpected completion fields")
        return int(fields["dev"]), int(fields["ino"])
    except (KeyError, UnicodeError, ValueError) as exc:
        raise Refusal(f"invalid parent completion record for {display}") from exc


def _ensure_completion_record(authority: _Authority) -> str:
    if authority.root_name is None:
        raise Refusal("cannot complete an unselected runtime root")
    name = _record_name(authority.root_name)
    expected = _record_data(authority)
    current = _read_at(authority.parent_fd, name, len(expected) + 1, missing=True)
    if current is None:
        _write_exclusive(authority.parent_fd, name, expected)
        os.fsync(authority.parent_fd)
    elif current != expected:
        display = authority.root_display or authority.parent_display
        raise Refusal(f"invalid parent completion record for {display}")
    return name


def _finish_recorded(
    authority: _Authority, lifecycle: _Lifecycle, record_name: str
) -> None:
    if authority.root_name is None or authority.root_display is None:
        raise Refusal("cannot finish an unselected runtime root")
    _checkpoint("before_target_rmdir", authority.root_display)
    authority.check("before removal")
    leftovers = set(_scan(_root_fd(authority))) - META
    if leftovers:
        raise Refusal(
            f"completion record for {authority.root_display} has unexpected payload: "
            f"{', '.join(sorted(leftovers))}"
        )
    for name in META:
        with suppress(FileNotFoundError):
            os.unlink(name, dir_fd=_root_fd(authority))
    os.fsync(_root_fd(authority))
    _checkpoint("after_terminal_markers_removed", authority.root_display)
    authority.check("before removal")
    os.rmdir(authority.root_name, dir_fd=authority.parent_fd)
    os.fsync(authority.parent_fd)
    lifecycle.to(ROOT_DELETED_RECORDED)
    _checkpoint("after_target_rmdir", authority.root_display)
    os.unlink(record_name, dir_fd=authority.parent_fd)
    os.fsync(authority.parent_fd)
    lifecycle.to(ROOT_ABSENT)


def _finish(authority: _Authority, lifecycle: _Lifecycle) -> None:
    record_name = _ensure_completion_record(authority)
    lifecycle.to(ROOT_COMPLETION_RECORDED)
    display = authority.root_display or authority.parent_display
    _checkpoint("after_parent_completion", display)
    _finish_recorded(authority, lifecycle, record_name)


def _clean(
    authority: _Authority, lifecycle: _Lifecycle, instance_id: str
) -> None:
    expected = _sentinel(instance_id)
    quarantining = lifecycle.state == ROOT_QUARANTINING
    if _read(authority, SENTINEL_NAME, len(expected) + 1) != expected:
        display = authority.root_display or authority.parent_display
        raise Refusal(f"invalid sentinel {display / SENTINEL_NAME}")
    display = authority.root_display or authority.parent_display
    if not quarantining:
        _validate_tree(_root_fd(authority), display, authority)
        _checkpoint("after_validation", display)
        authority.check("before quarantine")
        _write(authority, QUARANTINE_NAME, b"")
        os.fsync(_root_fd(authority))
        lifecycle.to(ROOT_QUARANTINING)
        _checkpoint("after_quarantine", display)
    else:
        RUNTIME_ROOT_LIFECYCLE.check(ROOT_QUARANTINING, ROOT_QUARANTINING)
    _checkpoint("before_delete_tree", display)
    authority.check("before deletion")
    _validate_tree(_root_fd(authority), display, authority)
    _delete_tree(_root_fd(authority), display, authority)
    _finish(authority, lifecycle)


def _open_root(authority: _Authority, instance_id: str, display: Path) -> bool:
    try:
        fd = os.open(instance_id, DIR_FLAGS, dir_fd=authority.parent_fd)
    except FileNotFoundError:
        return False
    _set_root(authority, instance_id, fd, _identity(fd), display)
    try:
        authority.check("while opening")
        return True
    except BaseException:
        _close_root(authority)
        raise


def _open_owned_root(authority: _Authority, instance_id: str, display: Path) -> bool:
    """Reopen only the exact sentinel-owned root; never adopt a directory by name."""
    if not _open_root(authority, instance_id, display):
        return False
    expected = _sentinel(instance_id)
    try:
        if _read(authority, SENTINEL_NAME, len(expected) + 1) != expected:
            raise Refusal(f"invalid sentinel {display / SENTINEL_NAME}")
        return True
    except BaseException:
        _close_root(authority)
        raise


def _private_pattern(instance_id: str) -> re.Pattern[str]:
    return re.compile(
        rf"\.{re.escape(instance_id)}\.provision\.[0-9]+\.[0-9a-f]{{16}}"
    )


def _classify_lifecycle(
    authority: _Authority, instance_id: str, display: Path
) -> _Lifecycle:
    """Classify every durable marker combination or refuse it loudly."""
    _close_root(authority)
    names = set(_scan(authority.parent_fd))
    record_name = _record_name(instance_id)
    private_prefix = f".{instance_id}.provision."
    private_observations = sorted(
        name for name in names if name.startswith(private_prefix)
    )
    staged = [
        name
        for name in private_observations
        if _private_pattern(instance_id).fullmatch(name)
    ]
    malformed = sorted(set(private_observations) - set(staged))
    public_present = instance_id in names
    record_data = _read_at(authority.parent_fd, record_name, 256, missing=True)

    if malformed:
        raise Refusal(
            f"malformed private provisioning observations exist for {display}: "
            f"{malformed}; refusing an unenumerated lifecycle observation"
        )
    if len(staged) > 1:
        raise Refusal(
            f"multiple private provisioning roots exist for {display}: {staged}; "
            "refusing an unenumerated lifecycle observation"
        )
    if staged:
        if public_present or record_data is not None:
            raise Refusal(
                f"private provisioning root {staged[0]} coexists with public or "
                f"completion state for {display}; refusing an unenumerated lifecycle"
            )
        name = staged[0]
        staged_display = authority.parent_display / name
        fd = os.open(name, DIR_FLAGS, dir_fd=authority.parent_fd)
        _set_root(authority, name, fd, _identity(fd), staged_display)
        authority.check("while reconciling private provisioning")
        entries = set(_scan(fd))
        sentinel = _read_at(fd, SENTINEL_NAME, len(_sentinel(instance_id)) + 1, missing=True)
        if entries == set():
            return _Lifecycle(ROOT_PROVISIONING)
        if entries == {SENTINEL_NAME} and sentinel == _sentinel(instance_id):
            return _Lifecycle(ROOT_PROVISIONING)
        raise Refusal(
            f"private provisioning root {staged_display} has unexpected content "
            f"{sorted(entries)}; recovery only owns an empty or exact sentinel root"
        )

    if public_present:
        if record_data is not None:
            _open_root(authority, instance_id, display)
            expected_identity = _parse_record(record_data, instance_id, display)
            if authority.root_identity != expected_identity:
                raise Refusal(f"completion record no longer owns {display}")
            return _Lifecycle(ROOT_COMPLETION_RECORDED)
        _open_owned_root(authority, instance_id, display)
        if _read(authority, QUARANTINE_NAME, 1, missing=True) is not None:
            return _Lifecycle(ROOT_QUARANTINING)
        return _Lifecycle(ROOT_ACTIVE)

    if record_data is not None:
        _parse_record(record_data, instance_id, display)
        return _Lifecycle(ROOT_DELETED_RECORDED)
    return _Lifecycle(ROOT_ABSENT)


def _reconcile_private(authority: _Authority, lifecycle: _Lifecycle) -> None:
    if lifecycle.state != ROOT_PROVISIONING or authority.root is None:
        raise Refusal("private-root reconciliation requires provisioning state")
    name = authority.root.name
    with suppress(FileNotFoundError):
        os.unlink(SENTINEL_NAME, dir_fd=authority.root.fd)
    os.fsync(authority.root.fd)
    authority.check("before private-root removal")
    os.rmdir(name, dir_fd=authority.parent_fd)
    os.fsync(authority.parent_fd)
    lifecycle.to(ROOT_ABSENT)
    _close_root(authority)


def _reconcile_completion(
    authority: _Authority,
    lifecycle: _Lifecycle,
    instance_id: str,
) -> None:
    name = _record_name(instance_id)
    if lifecycle.state == ROOT_COMPLETION_RECORDED:
        try:
            _finish_recorded(authority, lifecycle, name)
        finally:
            _close_root(authority)
        return
    if lifecycle.state != ROOT_DELETED_RECORDED:
        raise Refusal(
            f"completion reconciliation cannot consume {lifecycle.state!r} state"
        )
    authority.check("before completion-record removal")
    os.unlink(name, dir_fd=authority.parent_fd)
    os.fsync(authority.parent_fd)
    lifecycle.to(ROOT_ABSENT)


def _open_authority(project_fd: int, project: Path, command: str) -> _Authority | None:
    try:
        parent_fd = os.open(RUNTIME_PARENT_NAME, DIR_FLAGS, dir_fd=project_fd)
    except FileNotFoundError:
        if command == "clean":
            return None
        os.mkdir(RUNTIME_PARENT_NAME, 0o700, dir_fd=project_fd)
        parent_fd = os.open(RUNTIME_PARENT_NAME, DIR_FLAGS, dir_fd=project_fd)
    authority = _Authority(
        project_fd=project_fd,
        parent_fd=parent_fd,
        parent_identity=_identity(parent_fd),
        parent_display=project / RUNTIME_PARENT_NAME,
    )
    try:
        _checkpoint("after_runtime_parent_open", authority.parent_display)
        authority.check("while opening runtime parent")
        return authority
    except BaseException:
        os.close(parent_fd)
        raise


def _run(command: str, instance_id: str, child_command: list[str] | None = None) -> int:
    project = Path(__file__).resolve(strict=True).parent.parent
    project_fd = os.open(project, DIR_FLAGS)
    authority = None
    try:
        # This lock is the authority, not an advisory hint: every cooperating runtime
        # writer holds it for its whole mutation lifetime, and run passes this fd to its
        # child so wrapper death cannot release it early.
        fcntl.flock(project_fd, fcntl.LOCK_EX)
        authority = _open_authority(project_fd, project, command)
        if authority is None:
            return 0
        display = authority.parent_display / instance_id
        if command == "run" and not child_command:
            raise Refusal("run requires a command after --")
        lifecycle = _classify_lifecycle(authority, instance_id, display)
        if lifecycle.state == ROOT_PROVISIONING:
            _reconcile_private(authority, lifecycle)
        if command in {"prepare", "run"}:
            if lifecycle.state in {
                ROOT_QUARANTINING,
                ROOT_COMPLETION_RECORDED,
                ROOT_DELETED_RECORDED,
            }:
                raise Refusal(
                    f"runtime root {display} is {lifecycle.state} and cannot be "
                    f"reactivated by {command}; run `scripts/runtime_state.sh clean` "
                    "to complete destructive recovery, then retry"
                )
            if lifecycle.state == ROOT_ABSENT:
                _provision(authority, lifecycle, instance_id, display)
            elif lifecycle.state == ROOT_ACTIVE:
                lifecycle.to(ROOT_ACTIVE)
            else:  # every other state was handled above
                raise Refusal(
                    f"persistent command observed unhandled lifecycle {lifecycle.state!r}"
                )
        elif lifecycle.state in {ROOT_ACTIVE, ROOT_QUARANTINING}:
            _clean(authority, lifecycle, instance_id)
        elif lifecycle.state in {ROOT_COMPLETION_RECORDED, ROOT_DELETED_RECORDED}:
            _reconcile_completion(authority, lifecycle, instance_id)
        elif lifecycle.state != ROOT_ABSENT:
            raise Refusal(f"clean observed unhandled lifecycle {lifecycle.state!r}")
        if command == "run":
            if lifecycle.state != ROOT_ACTIVE:
                raise Refusal(
                    f"child launch requires active runtime root, got {lifecycle.state!r}"
                )
            return subprocess.run(
                child_command,
                cwd=project,
                check=False,
                pass_fds=(project_fd,),
            ).returncode
        return 0
    finally:
        if authority is not None:
            _close_root(authority)
            with suppress(OSError):
                os.close(authority.parent_fd)
        with suppress(OSError):
            os.close(project_fd)


def main() -> int:
    """Parse the fixed instance selector and invoke the descriptor-owned operation."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("prepare", "clean", "run"))
    parser.add_argument("child_command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if "CDC_INSTANCE_RUNTIME_ROOT" in os.environ:
        raise Refusal(
            "cleanup from CDC_INSTANCE_RUNTIME_ROOT; select CDC_TEST_INSTANCE_ID instead"
        )
    instance_id = os.environ.get(
        "CDC_TEST_INSTANCE_ID", f"pg{os.environ.get('CDC_TEST_PGPORT', '15432')}"
    )
    if re.fullmatch(r"[a-z0-9][a-z0-9_]*", instance_id) is None:
        raise Refusal(f"path for invalid CDC_TEST_INSTANCE_ID {instance_id!r}")
    child_command = args.child_command
    if child_command[:1] == ["--"]:
        child_command = child_command[1:]
    return _run(args.command, instance_id, child_command)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Refusal, OSError) as exc:
        print(f"ERROR: refusing runtime-state operation: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
