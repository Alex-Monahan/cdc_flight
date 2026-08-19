"""Rubric 1.7 — faults in the DESTINATION, not just in our own protocol.

What kept 1.7 at 3 was stated plainly in RUBRIC_STATUS: "the injection is robust for
the commit protocol and nothing else. Nothing injects a fault into the destination or
network layer (a MotherDuck write that fails mid-transaction, a severed connection, a
hung `COMMIT`)". These are those.

Each case asserts the outcome *class* the item requires:

| fault | required outcome |
|---|---|
| `destination_write` | transaction rolls back, run exits non-zero, ledger unchanged, recovery exact |
| `destination_close` | as above, through a connection that no longer exists |
| `destination_commit` | **ambiguous** commit: exit non-zero, and recovery is exact whichever way it went |
| `destination_hang` | bounded by `CDC_COMMIT_TIMEOUT` -> non-zero exit, never a hang |
| `swap` | the backfill swap is atomic: no table is left missing |

"Ledger unchanged" is measured against Postgres, not against a previous destination
read: the source's own row count is the only number that cannot be wrong.
"""

from __future__ import annotations

import pytest
from support.fixtures import Sandbox

CUSTOMERS = 40


@pytest.fixture(scope="module")
def dest_fault_box(tmp_path_factory, postgres_cluster):
    """One healthy baseline run, then each test injects into its own extra run."""
    box = Sandbox("dest_faults", tmp_path_factory.mktemp("sbx_dest_faults"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        yield box
    finally:
        box.cleanup()
        box.reseed()


def _insert(box: Sandbox, tag: str, rows: int = CUSTOMERS) -> None:
    box.sql(
        "INSERT INTO app.customers (name, email) SELECT "
        f"'{tag}-' || i, '{tag}-' || i || '@example.com' "
        f"FROM generate_series(1, {rows}) i",
        one_transaction=True,
    )


def _source_count(box: Sandbox, tag: str) -> int:
    return box.pg_query(
        "SELECT count(*) FROM app.customers WHERE name LIKE %s", (f"{tag}-%",)
    )[0][0]


def _dest_count(box: Sandbox, tag: str) -> int:
    return box.scalar(
        f"SELECT count(*) FROM {box.table('cdcflight_app_customers')} WHERE name LIKE ?",
        [f"{tag}-%"],
    )


def _assert_the_named_anchor_fired(box: Sandbox, point: str) -> None:
    """The run must say WHICH anchor fired, and it must be the one that was armed.

    Without this, every assertion in this file is conditional on an unproven premise:
    a run that fails to start for an unrelated reason also exits non-zero (Codex M2).
    """
    fired = box.fired_fault()
    assert fired is not None, (
        f"no fault-fired record was written, so nothing establishes that {point!r} is "
        "what ended this run"
    )
    assert fired["point"] == point, fired


def _assert_exact_after_recovery(box: Sandbox, tag: str) -> None:
    """The recovery run must land every row of the tagged batch exactly once."""
    recovered = box.run(max_seconds=150)
    assert recovered["ok"] is True, recovered
    assert _dest_count(box, tag) == _source_count(box, tag) == CUSTOMERS
    # And the destination must not hold a *second* copy of any of them: the identity
    # column is the ledger, so counting distinct identities is the duplication check.
    total, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM "
        f"{box.table('cdcflight_app_customers')}"
    )[0]
    assert total == distinct, f"{total - distinct} duplicated change events"


@pytest.mark.parametrize(
    "point",
    [
        "destination_write",
        pytest.param("destination_close", marks=pytest.mark.slow),
    ],
)
def test_a_destination_error_mid_transaction_loses_nothing(dest_fault_box, point):
    box = dest_fault_box
    tag = point.replace("_", "")
    box.clear_fired_fault()
    _insert(box, tag)
    failed = box.run(
        max_seconds=120,
        expect_success=False,
        extra_env={"CDC_FAULT_INJECT": f"{point}:1"},
    )
    assert failed["returncode"] != 0, (
        f"{point} produced a successful run: "
        f"{ {k: v for k, v in failed.items() if k != 'output'} }"
    )
    assert failed.get("ok") is not True, failed
    # The run summary has to name the failure. A non-zero exit with a summary that
    # says nothing is not "accurate last_run.json" - and an exit that came from
    # something *other* than the injected fault would make this test vacuous.
    assert "injected" in (failed.get("error") or "").lower(), failed.get("error")
    _assert_the_named_anchor_fired(box, point)
    _assert_exact_after_recovery(box, tag)


@pytest.mark.parametrize(
    "point",
    [
        # The genuinely ambiguous one: `COMMIT` EXECUTES and then the client loses the
        # answer, so the destination really did commit and we really cannot know it.
        # It is in the DEFAULT suite because it is the only anchor that exercises
        # recovery from a *durable* group the offset store never heard about, which is
        # the state Invariant O exists for.
        "destination_commit_late",
        # The easy one, kept: `COMMIT` raises before running. Codex M2 is right that
        # this is an ordinary uncommitted failure wearing an ambiguous name, and the
        # recovery still has to be exact for it.
        pytest.param("destination_commit", marks=pytest.mark.slow),
    ],
)
def test_an_ambiguous_commit_recovers_exactly(dest_fault_box, point):
    """`COMMIT` fails. The transaction may or may not be durable - both are safe.

    This is ADR 0001 §4.6 F5, and it is the case Invariant O exists for: nothing has
    entered Debezium's offset store, so the next run reads whichever resume point the
    destination actually holds and continues from exactly there.
    """
    box = dest_fault_box
    tag = point.replace("_", "")
    box.clear_fired_fault()
    _insert(box, tag)
    failed = box.run(
        max_seconds=120,
        expect_success=False,
        extra_env={"CDC_FAULT_INJECT": f"{point}:1"},
    )
    assert failed["returncode"] != 0, failed
    assert "injected" in (failed.get("error") or "").lower(), failed.get("error")
    _assert_the_named_anchor_fired(box, point)
    landed = _dest_count(box, tag)
    assert landed in (0, CUSTOMERS), (
        f"an ambiguous commit left a PARTIAL batch of {landed} rows; the group is "
        "supposed to be all-or-nothing"
    )
    _assert_exact_after_recovery(box, tag)


@pytest.mark.slow
def test_a_hung_commit_is_bounded_and_never_reports_success(dest_fault_box):
    """The commit watchdog, and ONLY the commit watchdog, must end this run.

    The assertion used to be `returncode in (75, -9, 137, 1)`, which passes if the run
    dies of anything at all: `-9` is the harness giving up, `137` is the fault
    injector's default, `1` is any unrelated failure (Opus MAJOR-5). Only 75
    (`EX_TEMPFAIL`) is the watchdog's own code and only the watchdog is the mechanism
    this test exists for.

    It also used to depend on a second undocumented accident: `hang_seconds` was
    `<action>` reinterpreted as seconds, so a bare `destination_hang:1` hung for 137 s -
    *shorter* than the shipped 300 s watchdog, which would have let the COMMIT succeed.
    The duration now has its own variable and is set longer than the watchdog on purpose.
    """
    box = dest_fault_box
    tag = "hungcommit"
    box.clear_fired_fault()
    _insert(box, tag)
    failed = box.run(
        max_seconds=200,
        timeout=180,
        expect_success=False,
        extra_env={
            "CDC_FAULT_INJECT": "destination_hang:1",
            # The watchdog, wound right down. Without it this run never ends.
            "CDC_COMMIT_TIMEOUT": "5",
            # ... and the hang is much longer than the watchdog, so only the watchdog
            # can be what ended the run.
            "CDC_FAULT_HANG_SECONDS": "600",
        },
    )
    # 75 = EX_TEMPFAIL, the watchdog's own exit code: a supervisor should retry.
    assert failed["returncode"] == 75, (
        f"expected the commit watchdog's EX_TEMPFAIL, got {failed['returncode']}; "
        "any other code means something other than the watchdog bounded this run"
    )
    fired = box.fired_fault()
    assert fired is not None and fired["point"] == "destination_hang", fired
    assert fired["action"].startswith("hang:600"), fired
    # No `last_run.json` here, and that is correct: the watchdog `os._exit`s, which runs
    # no atexit hook - the same hard-death shape a `kill -9` produces. The evidence is
    # the fsynced fired record above, the exact exit code, and the pre-armed durable
    # timeout alert. The watchdog callback itself performs no alert or logging I/O while
    # COMMIT_ACK is active; the alert text is emitted before that exclusion opens.
    output = failed["output"].lower()
    assert "commit" in output and "did not return within" in output, output[-2000:]
    assert "ambiguous" in output, "the log must say the commit may already be durable"
    timeout_alerts = box.duck_query(
        "SELECT severity, code FROM _cdc_flight.alerts "
        "WHERE code = 'commit_timeout'"
    )
    assert any(
        severity == "critical" and code == "commit_timeout"
        for severity, code in timeout_alerts
    ), timeout_alerts
    assert len(timeout_alerts) == 1, timeout_alerts
    _assert_exact_after_recovery(box, tag)


def test_the_hang_duration_is_its_own_setting_not_the_exit_code(monkeypatch):
    """Opus MAJOR-5's second half, as a unit test.

    `wrap_destination` used to read `<action>` as a duration, so `destination_hang:1`
    hung for **137** seconds - the default exit code - which is neither documented nor
    what anybody writing that spec means.
    """
    from cdc_flight import faults

    class Dummy:
        def execute(self, sql, *a, **k):
            return None

    monkeypatch.setenv(faults.ENV_VAR, "destination_hang:1")
    monkeypatch.delenv(faults.HANG_SECONDS_ENV, raising=False)
    faults.refresh()
    assert faults.wrap_destination(Dummy())._hang_seconds == faults.DEFAULT_HANG_SECONDS

    monkeypatch.setenv(faults.HANG_SECONDS_ENV, "12.5")
    assert faults.wrap_destination(Dummy())._hang_seconds == 12.5

    monkeypatch.setenv(faults.ENV_VAR, "destination_hang:1:99")
    faults.refresh()
    wrapped = faults.wrap_destination(Dummy())
    assert wrapped._hang_seconds == 12.5, "the exit code is not a duration"


def test_the_commit_watchdog_is_off_by_default_for_nothing():
    """A watchdog nobody arms is not a guarantee; assert the default is finite."""
    from cdc_flight.config import RunConfig

    assert RunConfig().commit_timeout > 0


def test_destination_points_are_validated_like_every_other_point():
    from cdc_flight import faults

    assert faults.parse_spec("destination_commit:2") == ("destination_commit", 2, 137)
    with pytest.raises(faults.FaultSpecError):
        faults.parse_spec("destination_typo:1")


def test_a_destination_fault_spec_wraps_the_connection(monkeypatch):
    """The wrapper must be armed by the spec and inert without one."""
    from cdc_flight import faults

    class Dummy:
        def __init__(self):
            self.seen = []

        def execute(self, sql, *a, **k):
            self.seen.append(sql)

    monkeypatch.delenv(faults.ENV_VAR, raising=False)
    faults.refresh()
    plain = Dummy()
    assert faults.wrap_destination(plain) is plain

    monkeypatch.setenv(faults.ENV_VAR, "destination_write:1")
    faults.refresh()
    wrapped = faults.wrap_destination(Dummy())
    assert isinstance(wrapped, faults.FaultyConnection)
    faults.arm_group(1)
    # Bookkeeping passes through; a data write does not.
    wrapped.execute("BEGIN TRANSACTION")
    wrapped.execute("SELECT 1")
    wrapped.execute("INSERT INTO _cdc_flight.commit_log VALUES (1)")
    with pytest.raises(faults.DestinationFault):
        wrapped.execute('INSERT INTO "cdc_raw"."cdcflight_app_customers" VALUES (1)')


def test_a_destination_fault_does_not_fire_in_the_wrong_group(monkeypatch):
    from cdc_flight import faults

    class Dummy:
        def execute(self, sql, *a, **k):
            return None

    monkeypatch.setenv(faults.ENV_VAR, "destination_write:3")
    faults.refresh()
    wrapped = faults.wrap_destination(Dummy())
    for group in (1, 2):
        faults.arm_group(group)
        wrapped.execute('INSERT INTO "cdc_raw"."t" VALUES (1)')
    faults.arm_group(3)
    with pytest.raises(faults.DestinationFault):
        wrapped.execute('INSERT INTO "cdc_raw"."t" VALUES (1)')
