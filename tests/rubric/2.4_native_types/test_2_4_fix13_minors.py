"""FIX ROUND 13 regressions for the round-12 MINOR/NIT findings.

R12-7   unbounded `schema_change_deferred_for_refusal` alerting
R12-11  a NULL quarantine fingerprint reactivating the table once per run
R12-12  a non-`SchemaEvolutionRefused` `AdmissionError` losing its durable path
R12-15  `money` refused by the opaque-type OID allowlist before its own branch
"""

from __future__ import annotations

import duckdb
import pytest

from cdc_flight import catalog_poll, destination
from cdc_flight.catalog import CHANGE_SCHEMA, CatalogChange, CatalogWatcher
from cdc_flight.catalog_apply import CatalogCoordinator
from cdc_flight.errors import SchemaEvolutionRefused
from cdc_flight.typed_types import (
    InvalidTypedValue,
    SourceTypeDescriptor,
    UnsupportedType,
    adapt_value,
    encode_value,
    native_type,
)


def _control_connection(tmp_path, name: str):
    con = duckdb.connect(str(tmp_path / f"{name}.duckdb"))
    destination.ensure_control_schema(con)
    return con


def _quarantine(con, *, pipeline: str, schema: str, table: str, fingerprint=None):
    """Drive the declared pending -> quarantined edge with one identical input."""
    for lsn in (100, 101):
        destination.record_schema_refusal(
            con,
            pipeline=pipeline,
            source_schema=schema,
            source_table=table,
            target_table=f"cdcflight_{schema}_{table}",
            detected_lsn=lsn,
            reason="value refusal",
            input_fingerprint="identical-durable-input",
            source_fingerprint=fingerprint,
        )


# --------------------------------------------------------------------------- #
# R12-7: one alert per (relation, blocking condition), not one per poll
# --------------------------------------------------------------------------- #
def test_deferred_schema_change_alerts_once_not_once_per_poll(tmp_path):
    """A permanently quarantined table must not grow `alerts` without bound.

    Three catalog polls of ONE quarantined relation used to append three identical
    `critical` rows (measured 39 -> 78 -> 117 across three runs).  The blocking
    condition is standing, so the alert is a single durable fact.
    """
    con = _control_connection(tmp_path, "deferral")
    sink = destination.AlertSink(con, pipeline="round13")
    try:
        _quarantine(con, pipeline="round13", schema="app", table="stuck")
        watcher = CatalogWatcher(
            dsn="",
            publication="cdc_flight_pub",
            schema="app",
            auto_discover=True,
            include=set(),
            poll_seconds=0,
        )
        change = CatalogChange(
            kind=CHANGE_SCHEMA, schema="app", table="stuck", detected_lsn=100
        )
        watcher.queue(change)
        coordinator = CatalogCoordinator(
            catalog=watcher,
            pipeline="round13",
            topic_prefix="cdcflight",
            drop_mode="replicate",
            registry_of=lambda: None,
            lifecycle_con=con,
        )

        emitted = []
        for _poll in range(3):
            plan = coordinator.plan(100)
            deferrals = [
                alert for alert in plan.alerts
                if alert["code"] == "schema_change_deferred_for_refusal"
            ]
            emitted.append(len(deferrals))
            # The real flush path: applier._flush_alerts -> AlertSink.raise_alert.
            for alert in plan.alerts:
                sink.raise_alert(
                    severity=alert["severity"],
                    code=alert["code"],
                    message=alert["message"],
                    context=alert.get("context"),
                )

        # The change stays deferred and re-planned every poll - that is the input
        # the finding describes; only the ALERT is deduplicated.
        assert emitted == [1, 0, 0]
        rows = con.execute(
            "SELECT count(*) FROM _cdc_flight.alerts "
            "WHERE pipeline = 'round13' "
            "AND code = 'schema_change_deferred_for_refusal'"
        ).fetchone()
        assert rows[0] == 1
    finally:
        sink.close()
        con.close()


def test_a_different_blocked_change_still_gets_its_own_alert(tmp_path):
    """Deduplication is per (relation, condition), never a global mute."""
    con = _control_connection(tmp_path, "deferral_second")
    sink = destination.AlertSink(con, pipeline="round13")
    try:
        _quarantine(con, pipeline="round13", schema="app", table="stuck")
        watcher = CatalogWatcher(
            dsn="",
            publication="cdc_flight_pub",
            schema="app",
            auto_discover=True,
            include=set(),
            poll_seconds=0,
        )
        watcher.queue(
            CatalogChange(
                kind=CHANGE_SCHEMA, schema="app", table="stuck", detected_lsn=100
            )
        )
        coordinator = CatalogCoordinator(
            catalog=watcher,
            pipeline="round13",
            topic_prefix="cdcflight",
            drop_mode="replicate",
            registry_of=lambda: None,
            lifecycle_con=con,
        )

        def flush(plan):
            for alert in plan.alerts:
                sink.raise_alert(
                    severity=alert["severity"],
                    code=alert["code"],
                    message=alert["message"],
                    context=alert.get("context"),
                )

        flush(coordinator.plan(100))
        # A LATER, different observation on the same quarantined relation is a new
        # blocking condition and must still be visible.
        watcher.queue(
            CatalogChange(
                kind=CHANGE_SCHEMA,
                schema="app",
                table="stuck",
                detected_lsn=200,
                old_identity="gen-1",
                new_identity="gen-2",
            )
        )
        second = coordinator.plan(200)
        flush(second)

        assert [
            alert["code"] for alert in second.alerts
        ] == ["schema_change_deferred_for_refusal"]
        assert con.execute(
            "SELECT count(*) FROM _cdc_flight.alerts "
            "WHERE pipeline = 'round13' "
            "AND code = 'schema_change_deferred_for_refusal'"
        ).fetchone()[0] == 2
    finally:
        sink.close()
        con.close()


# --------------------------------------------------------------------------- #
# R12-11: a NULL source fingerprint is adopted, not re-authorised every run
# --------------------------------------------------------------------------- #
def test_null_quarantine_fingerprint_is_adopted_and_bounds_the_retry(tmp_path):
    """Run 1 adopts, run 2 is quiet, run 3 (a real change) retries.

    The r11 fix returned True for a NULL stored fingerprint so a NULL quarantine
    could not be a permanent dead end, but nothing wrote the observed fingerprint
    back: every later run re-read the same NULL, reactivated the table and
    re-refused it.  Adoption keeps the dead end closed while making the retry
    bounded, exactly as the docstring already promised.
    """
    con = _control_connection(tmp_path, "quarantine_fp")
    try:
        _quarantine(con, pipeline="round13", schema="app", table="nullfp")
        assert con.execute(
            "SELECT state, source_fingerprint FROM _cdc_flight.schema_refusals "
            "WHERE pipeline='round13' AND source_table='nullfp'"
        ).fetchone() == ("quarantined", None)

        def run(fingerprint):
            return destination.quarantine_retry_allowed(
                con,
                pipeline="round13",
                source_schema="app",
                source_table="nullfp",
                source_exists=True,
                source_fingerprint=fingerprint,
            )

        # Run 1: the first successful source read becomes the baseline and by
        # itself authorises nothing.
        assert run("descriptor-v1") is False
        assert con.execute(
            "SELECT source_fingerprint FROM _cdc_flight.schema_refusals "
            "WHERE pipeline='round13' AND source_table='nullfp'"
        ).fetchone()[0] == "descriptor-v1"
        # Run 2: an unchanged source is not a trigger - this is the unbounded
        # per-run reactivation the finding reported.
        assert run("descriptor-v1") is False
        # Run 3: an actually CHANGED descriptor is still an automatic trigger, so
        # the r11 "NULL is a permanent dead end" defect stays fixed.
        assert run("descriptor-v2") is True
        # Adoption never moves the state itself; reactivation remains the declared
        # quarantined -> pending writer.
        assert con.execute(
            "SELECT state FROM _cdc_flight.schema_refusals "
            "WHERE pipeline='round13' AND source_table='nullfp'"
        ).fetchone()[0] == "quarantined"
    finally:
        con.close()


def test_adoption_needs_a_real_source_read(tmp_path):
    """An unavailable catalog read must not be adopted as a baseline."""
    con = _control_connection(tmp_path, "quarantine_unread")
    try:
        _quarantine(con, pipeline="round13", schema="app", table="unread")
        assert destination.quarantine_retry_allowed(
            con,
            pipeline="round13",
            source_schema="app",
            source_table="unread",
            source_exists=True,
            source_fingerprint=None,
        ) is False
        assert con.execute(
            "SELECT source_fingerprint FROM _cdc_flight.schema_refusals "
            "WHERE pipeline='round13' AND source_table='unread'"
        ).fetchone()[0] is None
        # Positive absence remains a trigger; the drop policy has to discharge it.
        assert destination.quarantine_retry_allowed(
            con,
            pipeline="round13",
            source_schema="app",
            source_table="unread",
            source_exists=False,
            source_fingerprint=None,
        ) is True
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# R12-12: the durable path is chosen by the exception CLASS
# --------------------------------------------------------------------------- #
def _watcher_for_poll(error):
    watcher = CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        auto_discover=True,
        include=set(),
        poll_seconds=0,
    )

    def failing_poll():
        raise error

    watcher.poll = failing_poll  # type: ignore[method-assign]
    return watcher


@pytest.mark.parametrize(
    "error",
    [
        UnsupportedType("no verified native destination representation"),
        InvalidTypedValue("'x' is not an integer"),
    ],
    ids=["unsupported_type", "invalid_typed_value"],
)
def test_any_admission_error_from_a_poll_takes_the_durable_refusal_path(error):
    """A sibling admission error must not evaporate into `last_error`.

    The durable path used to be gated on `isinstance(exc, SchemaEvolutionRefused)`
    INSIDE a bare `except Exception`, so every other `AdmissionError` was
    downgraded to an in-memory string that a restart forgot.
    """
    watcher = _watcher_for_poll(error)

    assert catalog_poll.poll_quietly(watcher) == []

    refusals = watcher.schema_refusals()
    assert len(refusals) == 1
    remembered = refusals[0]
    assert isinstance(remembered, SchemaEvolutionRefused)
    assert remembered.refusal_origin == "catalog_poll"
    assert str(error) in str(remembered)


def test_a_schema_refusal_from_a_poll_keeps_its_own_richer_instance():
    """Normalisation must not replace an already-durable refusal's context."""
    original = SchemaEvolutionRefused(
        "catalog descriptor authority is incomplete",
        source_schema="app",
        source_table="widgets",
        refusal_origin="catalog_poll",
    )
    watcher = _watcher_for_poll(original)

    assert catalog_poll.poll_quietly(watcher) == []

    assert watcher.schema_refusals() == (original,)


def test_a_non_admission_failure_is_still_only_an_in_memory_error():
    """The bare handler keeps its meaning for genuinely unrelated failures."""
    watcher = _watcher_for_poll(RuntimeError("connection reset"))

    assert catalog_poll.poll_quietly(watcher) == []

    assert watcher.schema_refusals() == ()
    assert watcher.last_error == "RuntimeError: connection reset"


# --------------------------------------------------------------------------- #
# R12-15: money never refuses, under any OID
# --------------------------------------------------------------------------- #
def test_money_with_a_non_790_oid_flows_into_varchar_unchanged():
    """`money` must NEVER refuse, raise, block or quarantine a table.

    `encode_value` resolves `native_type` first, and the opaque OID allowlist
    (`money -> {790}`) raised `UnsupportedType` there before the money branch that
    claims "no path out of this branch that can raise" could ever run.
    """
    descriptor = SourceTypeDescriptor(918273, "pg_catalog.money", "money")

    assert native_type(descriptor).sql == "VARCHAR"
    assert native_type(descriptor).kind == "VARCHAR"
    assert encode_value("whatever", descriptor) == "whatever"
    # Nothing is synthesized or reformatted, under any lc_monetary spelling.
    for value in ("1234.56", "$1,234.56", "£1.234,56", "-1.00", "100,50 €", ""):
        assert encode_value(value, descriptor) == value
        assert adapt_value(value, native_type(descriptor)) == value
    assert encode_value(None, descriptor) is None


def test_the_money_carve_out_does_not_widen_the_other_opaque_allowlists():
    """Names alone remain no authority for every OTHER opaque kind."""
    for kind, real_oid in (("xml", 142), ("inet", 869), ("macaddr", 829)):
        assert native_type(
            SourceTypeDescriptor(real_oid, f"pg_catalog.{kind}", kind)
        ).sql == "VARCHAR"
        with pytest.raises(UnsupportedType):
            native_type(SourceTypeDescriptor(918273, f"evil.{kind}", kind))
