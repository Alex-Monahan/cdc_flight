"""MotherDuck proof for the retention-backed acquisition recovery handoff.

The logical-message durability test in ``test_7_4_message_motherduck.py`` is closed and
intentionally unchanged. This separate destination test covers the structural recovery
contract introduced for B7-1: a rebuilt image may be handed to the still-retained main
slot, and that handoff must not be represented by a fabricated empty offset row.
"""

from __future__ import annotations

import duckdb
import pytest

from cdc_flight import destination as dest_mod
from cdc_flight import recovery as recovery_mod
from cdc_flight import table_lifecycle

pytestmark = [
    pytest.mark.motherduck,
    pytest.mark.e2e,
    pytest.mark.xdist_group("md_7_4_retained_recovery"),
]


def test_retained_recovery_clears_without_a_synthetic_offset(
    motherduck_module_case, tmp_path
):
    """MotherDuck accepts a durable image-to-retained-slot handoff without fake LSNs."""
    md = motherduck_module_case
    pipeline = "p74_retained_recovery"
    namespace = "p74_retained_recovery_ns"
    table = ("app", "customers", "cdcflight_app_customers")
    dsn = f"md:{md['database']}?motherduck_token={md['token']}"

    # Use the fixture's default MotherDuck configuration so the module cleanup can
    # reconnect with the same DuckDB catalog settings.
    with duckdb.connect(dsn) as con:
        dest_mod.ensure_control_schema(con, control_schema=md["control_schema"])
        dest_mod.ensure_dataset(con, md["dataset"])
        control = f'"{md["control_schema"]}"'

        con.execute(
            f"INSERT INTO {control}.table_state "
            "(pipeline, source_schema, source_table, target_table, snapshot_state) "
            "VALUES (?, ?, ?, ?, 'complete')",
            [pipeline, table[0], table[1], table[2]],
        )
        slot_receipt = dest_mod.write_slot_state(
            con,
            pipeline=pipeline,
            slot_name="p74_main_slot",
            observation={},
            verdict="slot_ahead_of_destination",
            verdict_message="the source slot outran the destination",
            control_schema=md["control_schema"],
        )
        record = recovery_mod.begin(
            con,
            pipeline=pipeline,
            namespace=namespace,
            decision="slot_ahead_of_destination",
            message="the source slot outran the destination",
            slot_name="p74_main_slot",
            offset_path=tmp_path / "offsets.dat",
            captured_tables=[table],
            forget_catalog=False,
            slot_receipt=slot_receipt,
            logical_message_dataset=md["dataset"],
            control_schema=md["control_schema"],
        )
        resumed = recovery_mod.resume(
            con,
            pipeline=pipeline,
            namespace=namespace,
            record=record,
            dsn="postgresql://unused",
            drop_slot=lambda _dsn, _slot: "retained",
            logical_message_dataset=md["dataset"],
            control_schema=md["control_schema"],
        )
        assert resumed["slot"] == "retained"

        table_lifecycle.transition(
            con,
            pipeline=pipeline,
            source_schema=table[0],
            source_table=table[1],
            to=table_lifecycle.COMPLETE,
            reason="the throwaway image completed while the main slot was retained",
            snapshot_lsn=12345,
            control_schema=md["control_schema"],
        )
        completion = recovery_mod.complete_if_ready(
            con,
            pipeline=pipeline,
            namespace=namespace,
            record=record,
            retained_slot=True,
            control_schema=md["control_schema"],
        )

        assert completion.cleared is True
        assert completion.has_resume_point is False
        assert con.execute(
            f"SELECT count(*) FROM {control}.recovery_state WHERE pipeline = ?",
            [pipeline],
        ).fetchone()[0] == 0
        assert con.execute(
            f"SELECT count(*) FROM {control}.debezium_offsets WHERE pipeline = ?",
            [pipeline],
        ).fetchone()[0] == 0, "retirement must not fabricate an empty resume offset"
        assert con.execute(
            f"SELECT snapshot_state, snapshot_lsn FROM {control}.table_state "
            "WHERE pipeline = ? AND source_table = ?",
            [pipeline, table[1]],
        ).fetchone() == ("complete", 12345)
