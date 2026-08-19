from __future__ import annotations


def test_source_dark_episode_identity_reopens_after_recovery():
    import duckdb

    from cdc_flight.control_schema import ensure_control_schema
    from cdc_flight.destination import observe_source_health

    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        assert observe_source_health(con, pipeline="p", state="reachable")["episode_id"] == 0
        assert observe_source_health(con, pipeline="p", state="dark")["episode_id"] == 1
        assert observe_source_health(con, pipeline="p", state="dark")["episode_id"] == 1
        assert observe_source_health(con, pipeline="p", state="reachable")["episode_id"] == 1
        assert observe_source_health(con, pipeline="p", state="dark")["episode_id"] == 2
    finally:
        con.close()


def test_pipeline_source_dark_alert_is_once_per_episode(tmp_path):
    import duckdb

    from cdc_flight.config import DestinationConfig
    from cdc_flight.control_schema import ensure_control_schema
    from cdc_flight.destination import observe_source_health
    from cdc_flight.pipeline import _record_run_failure_alert

    con = duckdb.connect(":memory:")
    dest = DestinationConfig(
        kind="duckdb",
        pipeline_name="dark-pipeline",
        duckdb_path=tmp_path / "dest.duckdb",
    )
    try:
        ensure_control_schema(con)
        observe_source_health(con, pipeline=dest.pipeline_name, state="reachable")
        summary = {"stop_reason": "source_dark"}
        _record_run_failure_alert(
            con, dest=dest, runner_id="r1", exc=RuntimeError("blackhole"), summary=summary
        )
        _record_run_failure_alert(
            con, dest=dest, runner_id="r2", exc=RuntimeError("blackhole"), summary=summary
        )
        observe_source_health(con, pipeline=dest.pipeline_name, state="reachable")
        _record_run_failure_alert(
            con, dest=dest, runner_id="r3", exc=RuntimeError("blackhole"), summary=summary
        )
        rows = con.execute(
            'SELECT context FROM "_cdc_flight".alerts WHERE code = ? ORDER BY raised_at',
            ["source_dark"],
        ).fetchall()
        assert len(rows) == 2
        assert '"episode_id": 1' in rows[0][0]
        assert '"episode_id": 2' in rows[1][0]
    finally:
        con.close()
