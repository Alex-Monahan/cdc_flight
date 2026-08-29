from __future__ import annotations

import os
import subprocess
import sys

import pytest

_LOCAL_RESOLVE = r'''
from cdc_flight.config import DestinationConfig
from cdc_flight.destination import connect, ensure_control_schema, ensure_dataset
import sys

dest = DestinationConfig(
    kind="duckdb",
    pipeline_name="physical-identity-proof",
    duckdb_path=sys.argv[1],
    dataset_name="cdc_raw",
    control_schema="_cdc_flight",
)
con = connect(dest)
try:
    ensure_control_schema(con, dest.control_schema)
    ensure_dataset(con, dest.dataset_name)
    print(dest.resolve_physical_lease_key(con), flush=True)
finally:
    con.close()
'''

_MOTHERDUCK_LEASE = r'''
import sys
import time
from cdc_flight.config import DestinationConfig
from cdc_flight.destination import connect, ensure_control_schema, ensure_dataset
from cdc_flight.destination_lease import Lease
from cdc_flight.errors import LeaseLost

role, database, dataset, control = sys.argv[1:5]
dest = DestinationConfig(
    kind="motherduck",
    pipeline_name="physical-identity-proof",
    motherduck_database=database,
    dataset_name=dataset,
    control_schema=control,
)
con = connect(dest)
try:
    ensure_control_schema(con, dest.control_schema)
    ensure_dataset(con, dest.dataset_name)
    key = dest.resolve_physical_lease_key(con)
    try:
        Lease(key, owner_id=role, ttl_seconds=30, control_schema=dest.control_schema).acquire(con)
    except LeaseLost:
        print("CDC_LEASE_RESULT LEASE_LOST " + key, flush=True)
    else:
        print("CDC_LEASE_RESULT ACQUIRED " + key, flush=True)
        if role == "holder":
            time.sleep(6)
finally:
    con.close()
'''

_LEASE_RESULT_PREFIX = "CDC_LEASE_RESULT "


def _find_lease_result(output: str) -> tuple[str, str] | None:
    """Find the child's sentinel anywhere in merged client output."""
    for line in output.splitlines():
        marker = line.find(_LEASE_RESULT_PREFIX)
        if marker < 0:
            continue
        payload = line[marker + len(_LEASE_RESULT_PREFIX) :].strip()
        kind, separator, key = payload.partition(" ")
        if separator and kind in {"ACQUIRED", "LEASE_LOST"} and key:
            return kind, key
    return None


def _parse_lease_result(output: str, *, label: str) -> tuple[str, str]:
    """Parse the child's sentinel without assuming it is the first output line.

    MotherDuck may write a progress display before the child can publish its
    result.  The sentinel is the output contract; line position is not.
    """
    result = _find_lease_result(output)
    if result is not None:
        return result
    pytest.fail(f"{label} did not publish a lease result sentinel: output={output!r}")


def _read_lease_result(process: subprocess.Popen, *, label: str) -> tuple[str, str]:
    """Read until the sentinel, without assuming it is the first output line."""
    assert process.stdout is not None
    output: list[str] = []
    while True:
        line = process.stdout.readline()
        if not line:
            pytest.fail(
                f"{label} exited before publishing its lease: rc={process.poll()} "
                f"output={''.join(output)!r}"
            )
        output.append(line)
        parsed = _find_lease_result("".join(output))
        if parsed is not None:
            return parsed


def _child_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    return env


def test_local_path_aliases_resolve_to_one_lease_key_in_two_processes(tmp_path):
    """Real processes prove inode identity across relative/absolute/symlink paths."""
    real = tmp_path / "destination.duckdb"
    alias = tmp_path / "Alias.duckdb"
    (tmp_path / "nested").mkdir()
    alias.symlink_to(real)
    first = subprocess.run(
        [sys.executable, "-c", _LOCAL_RESOLVE, str(real)],
        capture_output=True,
        text=True,
        env=_child_env(),
        check=True,
    )
    second = subprocess.run(
        [
            sys.executable,
            "-c",
            _LOCAL_RESOLVE,
            str(tmp_path / "nested" / ".." / alias.name),
        ],
        capture_output=True,
        text=True,
        env=_child_env(),
        check=True,
    )
    assert first.stdout.strip() == second.stdout.strip(), (first, second)
    assert '"file":"inode:' in first.stdout


@pytest.mark.motherduck
def test_motherduck_aliases_share_one_lease_and_duplicate_is_refused():
    from cdc_flight.config import motherduck_token
    from cdc_flight.naming import quote

    token = motherduck_token()
    if not token:
        pytest.skip("motherduck_token is not set")
    import uuid

    import duckdb

    database = f"cdc_alias_identity_{uuid.uuid4().hex[:10]}"
    env = _child_env()
    env["motherduck_token"] = token
    holder = None
    try:
        holder = subprocess.Popen(
            [
                sys.executable,
                "-c",
                _MOTHERDUCK_LEASE,
                "holder",
                f'  "{database.upper()}"  ',
                '  "MiXeD_Data"  ',
                '  "MiXeD_Control"  ',
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        assert holder.stdout is not None
        holder_kind, holder_key = _read_lease_result(holder, label="holder")
        assert holder_kind == "ACQUIRED", holder_kind
        assert holder_key.startswith("destination:"), holder_key

        contender = subprocess.run(
            [
                sys.executable,
                "-c",
                _MOTHERDUCK_LEASE,
                "contender",
                database.lower(),
                "mixed_data",
                "mixed_control",
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )
        assert contender.returncode == 0, contender.stderr
        contender_kind, contender_key = _parse_lease_result(
            contender.stdout, label="contender"
        )
        assert contender_kind == "LEASE_LOST", contender.stdout
        assert contender_key == holder_key
    finally:
        if holder is not None and holder.poll() is None:
            holder.terminate()
            holder.wait(timeout=20)
        cleanup = duckdb.connect(f"md:?motherduck_token={token}")
        try:
            cleanup.execute(f"DROP DATABASE IF EXISTS {quote(database)}")
        finally:
            cleanup.close()
        verify = duckdb.connect(f"md:?motherduck_token={token}")
        try:
            names = {str(row[0]) for row in verify.execute("SHOW DATABASES").fetchall()}
        finally:
            verify.close()
        assert database not in names, f"MotherDuck scratch database left behind: {database}"
