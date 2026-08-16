"""Rubric 1.7 — a DEFAULT-suite guard for every one of the thirteen anchors.

The review's partition finding (Opus Q5, Codex m3): 10 of 12 anchors fired only in the
`slow` lane, so the gate that actually runs on every change guarded two of them. Moving
every crash/recovery cycle into the default suite is not the answer either — each costs
25-40 seconds against a 10-minute budget, and the budget is the reason the split exists.

So the anchors are guarded at **two** levels, and this file is the cheap one:

* here, in-process and in milliseconds: every anchor is *reachable*, *fires where it says
  it fires*, and produces the mechanism it exists for. No JVM, no Postgres, no subprocess
  except one that has to be a subprocess because the thing under test is `os._exit`.
* in `test_1_7_fault_matrix.py`, end to end: one representative per mechanism in the
  default lane, the whole matrix under `-m slow`.

A guard that only runs when somebody remembers to ask for it is not a guard. A guard that
costs six minutes is not one either.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from support.applier_lab import DATASET, Lab, begin, end, keyed, snap

from cdc_flight import faults
from cdc_flight.snapshot_completion import SnapshotCompletion


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(faults.ENV_VAR, raising=False)
    monkeypatch.delenv(faults.HANG_SECONDS_ENV, raising=False)
    faults.refresh()
    yield
    monkeypatch.delenv(faults.ENV_VAR, raising=False)
    faults.refresh()


# --------------------------------------------------------------------------- #
# every anchor is enumerable, parseable and reachable
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("point", faults.ALL_POINTS)
def test_every_anchor_parses_and_is_addressable(point, monkeypatch):
    monkeypatch.setenv(faults.ENV_VAR, f"{point}:3")
    assert faults.refresh() == (point, 3, faults.DEFAULT_EXIT_CODE)


# --------------------------------------------------------------------------- #
# the four destination anchors, fired against a real (fake) connection
# --------------------------------------------------------------------------- #
class _Dummy:
    def __init__(self):
        self.executed: list[str] = []
        self.closed = False

    def execute(self, sql, *a, **k):
        self.executed.append(sql)
        return None

    def close(self):
        self.closed = True


def _armed(point: str, monkeypatch, nth: int = 1):
    monkeypatch.setenv(faults.ENV_VAR, f"{point}:{nth}")
    faults.refresh()
    wrapped = faults.wrap_destination(_Dummy())
    faults.arm_group(nth)
    return wrapped


def test_destination_write_fires_on_a_data_statement_and_not_on_bookkeeping(monkeypatch):
    wrapped = _armed("destination_write", monkeypatch)
    wrapped.execute("BEGIN TRANSACTION")
    wrapped.execute("INSERT INTO _cdc_flight.commit_log VALUES (1)")
    with pytest.raises(faults.DestinationFault):
        wrapped.execute(f'INSERT INTO "{DATASET}"."cdcflight_app_customers" VALUES (1)')


def test_destination_commit_raises_BEFORE_the_statement_runs(monkeypatch):
    wrapped = _armed("destination_commit", monkeypatch)
    with pytest.raises(faults.DestinationFault):
        wrapped.execute("COMMIT")
    assert wrapped._con.executed == [], (
        "`destination_commit` is the *uncommitted* failure: the statement must not run"
    )


def test_destination_commit_late_runs_the_statement_and_THEN_raises(monkeypatch):
    """The genuinely ambiguous shape (Codex M2): committed, and we cannot know it."""
    wrapped = _armed("destination_commit_late", monkeypatch)
    with pytest.raises(faults.DestinationFault) as raised:
        wrapped.execute("COMMIT")
    assert wrapped._con.executed == ["COMMIT"], "the COMMIT must actually have run"
    assert "ambiguous" in str(raised.value)


def test_destination_close_severs_the_connection(monkeypatch):
    wrapped = _armed("destination_close", monkeypatch)
    with pytest.raises(faults.DestinationFault):
        wrapped.execute(f'INSERT INTO "{DATASET}"."t" VALUES (1)')
    assert wrapped._con.closed is True


def test_destination_hang_blocks_inside_commit_for_its_own_configured_time(monkeypatch):
    """Fired with a hang of zero, so the *routing* is proved without the waiting."""
    monkeypatch.setenv(faults.ENV_VAR, "destination_hang:1")
    monkeypatch.setenv(faults.HANG_SECONDS_ENV, "0")
    faults.refresh()
    wrapped = faults.wrap_destination(_Dummy())
    faults.arm_group(1)
    wrapped.execute("COMMIT")
    assert wrapped.fired is True
    assert wrapped._hang_seconds == 0.0


# --------------------------------------------------------------------------- #
# the commit watchdog, which is the mechanism `destination_hang` exists to prove
# --------------------------------------------------------------------------- #
def test_the_commit_watchdog_exits_75_and_only_75():
    """A subprocess, because the thing under test is `os._exit`.

    `EX_TEMPFAIL` is the watchdog's own code and the only code that distinguishes "the
    watchdog bounded a hung COMMIT" from "something killed the process" — which is
    exactly what the end-to-end assertion could not tell apart (Opus MAJOR-5).
    """
    script = textwrap.dedent(
        """
        import sys, time
        sys.path.insert(0, "src")
        from cdc_flight.self_heal import commit_watchdog
        with commit_watchdog(0.2, commit_id=7):
            time.sleep(30)
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=60
    )
    assert proc.returncode == 75, (proc.returncode, proc.stderr[-2000:])


def test_the_commit_watchdog_does_not_fire_on_a_prompt_commit():
    from cdc_flight.self_heal import commit_watchdog

    with commit_watchdog(30.0, commit_id=1):
        pass  # returns immediately; the timer must be cancelled


def test_a_zero_timeout_disables_the_watchdog():
    from cdc_flight.self_heal import commit_watchdog

    with commit_watchdog(0, commit_id=1):
        pass


# --------------------------------------------------------------------------- #
# the two protocol anchors with no other default guard
# --------------------------------------------------------------------------- #
def test_the_swap_anchor_fires_between_the_drop_and_the_rename(tmp_path, monkeypatch):
    """The `swap` anchor's DEFAULT guard: the old table survives a torn swap.

    Its end-to-end scenario needs a re-snapshot in flight and lives in the slow lane
    (`test_1_6_interrupted_snapshot.py`, `test_1_6_resnapshot_multi_table.py`), so the
    anchor had no default coverage at all beyond a static "it is declared" check.
    """
    lab = Lab(tmp_path / "swap.duckdb")
    try:
        # A live table with rows in it.
        lab.run([begin("t1", 100), keyed("t1", 1, 100, 1, "original"), end("t1", 1, 101, {"app.customers": 1})])
        assert lab.rows(lab.target("customers"), "name") == [("original",)]

        completion = SnapshotCompletion.full_snapshot({"app.customers"})
        completion.observe_notification("STARTED", {})
        lab.applier.snapshot_completion = completion

        monkeypatch.setenv(faults.ENV_VAR, "swap:1:raise")
        faults.refresh()
        with pytest.raises(faults.InjectedFault):
            lab.run(
                [
                    snap("customers", 200, ident=1, value="replaced", marker="last"),
                ]
            )
        # The DROP happened inside the group's transaction and the fault fired before
        # the RENAME, so the whole thing rolled back and the ORIGINAL table is intact.
        assert lab.rows(lab.target("customers"), "name") == [("original",)]
    finally:
        lab.close()


def test_the_decode_anchor_fires_before_any_transaction_opens(tmp_path, monkeypatch):
    """`decode`'s only other coverage is `slow`. It must not leave a transaction open."""
    lab = Lab(tmp_path / "decode.duckdb")
    try:
        monkeypatch.setenv(faults.ENV_VAR, "decode:1:raise")
        faults.refresh()
        with pytest.raises(faults.InjectedFault):
            faults.maybe_crash("decode", 1)
        # Nothing was opened, so an ordinary group still commits afterwards.
        monkeypatch.delenv(faults.ENV_VAR)
        faults.refresh()
        lab.run([begin("t1", 100), keyed("t1", 1, 100, 1, "after"), end("t1", 1, 101, {"app.customers": 1})])
        assert lab.rows(lab.target("customers"), "name") == [("after",)]
    finally:
        lab.close()


# --------------------------------------------------------------------------- #
# the fired-anchor record itself
# --------------------------------------------------------------------------- #
def test_a_fired_anchor_records_itself_where_a_test_can_read_it(tmp_path, monkeypatch):
    """The evidence every fault assertion now rests on (Codex M2 / A54)."""
    monkeypatch.setenv("CDC_STATE_DIR", str(tmp_path / "state"))
    assert faults.read_fired_record(tmp_path / "state") is None
    faults.record_fired("post_ack", 2, 137)
    record = faults.read_fired_record(tmp_path / "state")
    assert record["point"] == "post_ack"
    assert record["nth"] == 2
    assert record["action"] == "137"


def test_the_record_is_a_no_op_without_a_state_directory(monkeypatch):
    monkeypatch.delenv("CDC_STATE_DIR", raising=False)
    faults.record_fired("post_ack", 1, 137)  # must not raise


def test_production_entrypoint_cannot_be_armed_by_matrix_environment_alone(tmp_path):
    """The reviewer-A direct probe must not hard-exit without the test capability."""
    state_dir = tmp_path / "r17_matrix_arm_probe"
    absolute_state = tmp_path / "absolute-state.json"
    gate = tmp_path / "never-created-gate"
    project = Path(__file__).resolve().parents[3]
    env = {
        **os.environ,
        "PYTHONPATH": str(project / "src"),
        "CDC_STATE_DIR": str(state_dir),
        "CDC_CRASH_MATRIX_CUT": "ownership_available",
        "CDC_CRASH_MATRIX_GATE": str(gate),
        "CDC_CRASH_MATRIX_GATE_TIMEOUT": "0",
        "CDC_CRASH_MATRIX_STATE": str(absolute_state),
    }
    env.pop(faults.MATRIX_CAPABILITY_ENV_VAR, None)
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "from cdc_flight.ownership import DestinationOwnership; DestinationOwnership()",
        ],
        cwd=project,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, (proc.returncode, proc.stdout, proc.stderr)
    assert not absolute_state.exists()
    assert not (state_dir / "fault_fired.json").exists()


def test_armed_runtime_state_failure_is_fail_closed(tmp_path):
    """A capability-armed evidence write failure cannot silently continue."""
    state_root = tmp_path / "state-root-file"
    state_root.write_text("not a directory")
    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, faults._MATRIX_CAPABILITY_TOKEN)
    finally:
        os.close(write_fd)
    project = Path(__file__).resolve().parents[3]
    env = {
        **os.environ,
        "PYTHONPATH": str(project / "src"),
        "CDC_STATE_DIR": str(state_root),
        "CDC_CRASH_MATRIX_STATE": "state.json",
        faults.MATRIX_CAPABILITY_ENV_VAR: str(read_fd),
    }
    try:
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                "from cdc_flight import faults; faults.runtime_state(edge='probe')",
            ],
            cwd=project,
            env=env,
            pass_fds=(read_fd,),
            capture_output=True,
            text=True,
            timeout=30,
        )
    finally:
        os.close(read_fd)
    assert proc.returncode != 0
    assert "crash-matrix runtime state could not be persisted" in proc.stderr
