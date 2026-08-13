"""FIX17 reproductions for the two repeated architectural findings."""

from __future__ import annotations

from types import SimpleNamespace

import pytest


def test_raw_control_read_failure_cannot_be_contained_as_a_source_table(monkeypatch):
    """A raw planner control read must fail the run without table attribution."""
    import duckdb

    from cdc_flight import destination, planner

    item = SimpleNamespace(snapshot=False, target="raw_control_bad")
    event = SimpleNamespace(
        schema="app",
        table="raw_control_bad",
        qualified_table="app.raw_control_bad",
        key_descriptors={},
        before_descriptors={},
        after_descriptors={},
        lsn=101,
        txn_id="raw-control",
    )
    contained = []
    plan = planner.GroupPlan(
        con=None,
        commit_id=1,
        registry_of=lambda: None,
        snapshots=None,
        spill=None,
        truncate_mode="replicate",
        created_in_txn=set(),
        pipeline="probe",
        contain_table_failure=lambda refused, original: contained.append((refused, original)),
    )

    def collect(_self, _event, *, snapshot, target=None, event_id=None):
        plan.keyless_event_applied(item, "raw-control-event")

    def fail(*_args, **_kwargs):
        raise duckdb.TransactionException("probe control failure before materialization")

    monkeypatch.setattr(planner.GroupPlan, "_collect", collect)
    monkeypatch.setattr(destination, "read_keyless_event_state", fail)

    with pytest.raises(duckdb.TransactionException, match="probe control failure"):
        plan._collect_contained(event, snapshot=None, target="raw_control_bad")

    assert not plan._contained_tables
    assert not plan.blocked_tables
    assert contained == []


def test_descriptor_control_read_failure_cannot_be_contained_as_a_source_table():
    """Catalog authority is a control read until destination DML actually starts."""
    import duckdb

    from cdc_flight import planner

    event = SimpleNamespace(
        schema="app",
        table="descriptor_control_bad",
        qualified_table="app.descriptor_control_bad",
    )
    plan = object.__new__(planner.GroupPlan)
    plan.descriptor_provider = lambda _qualified: (_ for _ in ()).throw(
        duckdb.TransactionException("descriptor control read failed")
    )
    plan._catalog_descriptor_cache = {}

    with pytest.raises(duckdb.TransactionException, match="descriptor control read"):
        plan._enrich_descriptors(event)
    assert plan._catalog_descriptor_cache == {}


def test_spill_and_xml_control_reads_fail_run_without_table_attribution():
    """Neighboring pre-DML source reads cannot manufacture table refusals."""
    import duckdb

    from cdc_flight import planner, spill_protocol

    event = SimpleNamespace(
        schema="app",
        table="pre_dml_control_bad",
        qualified_table="app.pre_dml_control_bad",
        key_descriptors={},
        before_descriptors={},
        after_descriptors={},
        key={},
        before={},
        after={"id": 1},
        op="c",
        lsn=101,
    )
    applier = SimpleNamespace(
        descriptor_provider=lambda _qualified: (_ for _ in ()).throw(
            duckdb.TransactionException("spill catalog control failed")
        ),
        catalog=None,
    )
    with pytest.raises(duckdb.TransactionException, match="spill catalog control"):
        spill_protocol._enrich_descriptors(applier, event)

    plan = object.__new__(planner.GroupPlan)
    plan.descriptor_provider = None

    def reader(_event, _columns):
        raise duckdb.TransactionException("xml source session failed")

    with pytest.raises(duckdb.TransactionException, match="xml source session"):
        plan._hydrate_omitted_xml_arrays(
            event,
            ("xmls",),
            {"xmls": object()},
            SimpleNamespace(read_event_columns=reader),
        )


def test_raw_control_read_failure_fails_the_real_run_without_a_source_relation(
    tmp_path, monkeypatch
):
    """The raw planner seam is run-level in the real Applier, not just in a unit plan."""
    import duckdb
    from support.applier_lab import Lab, data, end

    from cdc_flight import destination

    def fail(*_args, **_kwargs):
        raise duckdb.TransactionException("raw control read failed")

    monkeypatch.setattr(destination, "read_keyless_event_state", fail)
    box = Lab(tmp_path / "raw-control-run.duckdb")
    try:
        with pytest.raises(duckdb.TransactionException, match="raw control read failed"):
            box.run(
                [
                    data(
                        "raw-control-run",
                        1,
                        100,
                        table="raw_control_bad",
                        after={"id": 1, "name": "bad"},
                    ),
                    data(
                        "raw-control-run",
                        2,
                        101,
                        table="raw_control_peer",
                        key={"id": 1},
                        after={"id": 1, "name": "healthy"},
                    ),
                    end(
                        "raw-control-run",
                        2,
                        102,
                        {
                            "app.raw_control_bad": 1,
                            "app.raw_control_peer": 1,
                        },
                    ),
                ]
            )
        assert box.q("SELECT count(*) FROM _cdc_flight.schema_refusals") == [(0,)]
        assert box.q(
            "SELECT count(*) FROM _cdc_flight.alerts WHERE code = 'table_exception_contained'"
        ) == [(0,)]
        assert box.applier._contained_failures == []
    finally:
        box.close()


def test_control_ledger_write_failure_is_not_wrapped_as_table_data():
    """A duplicate in the control ledger must retain its run-level driver error."""
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA _cdc_flight")
        con.execute(
            "CREATE TABLE _cdc_flight.keyless_events "
            "(event_id VARCHAR PRIMARY KEY, payload VARCHAR)"
        )
        statement = "INSERT INTO _cdc_flight.keyless_events VALUES (?, ?)"
        con.execute(statement, ["same-event", "first"])

        with pytest.raises(duckdb.ConstraintException):
            con.execute(statement, ["same-event", "duplicate"])
    finally:
        con.close()


def test_control_ledger_failure_fails_the_real_run_without_a_source_relation(tmp_path, monkeypatch):
    """The production keyless ledger uses the raw connection, not the DML facade."""
    import duckdb
    from support.applier_lab import Lab, data, end

    from cdc_flight import destination

    box = Lab(tmp_path / "control-ledger-run.duckdb")
    try:
        event = data(
            "ledger-seed",
            1,
            100,
            table="ledger_bad",
            after={"id": 1, "name": "bad"},
        )
        box.run([event, end("ledger-seed", 1, 101, {"app.ledger_bad": 1})])

        # Re-admit the exact event while making the planner believe the durable
        # ledger is absent. Remove only the physical row first: otherwise the
        # table's own cdcf_event_id key would reject the replay before the
        # control-ledger INSERT, which would test the wrong seam.
        box.con.execute(
            'DELETE FROM "cdc_raw"."cdcflight_app_ledger_bad" WHERE cdcf_event_id = ?',
            ["100:ledger-seed:1"],
        )
        box.applier.resume_point.last_lsn = 0
        monkeypatch.setattr(destination, "read_keyless_event_state", lambda *_a, **_k: None)
        with pytest.raises(duckdb.ConstraintException):
            box.run([event, end("ledger-seed", 1, 101, {"app.ledger_bad": 1})])

        assert box.q("SELECT count(*) FROM _cdc_flight.schema_refusals") == [(0,)]
        assert box.q(
            "SELECT count(*) FROM _cdc_flight.alerts WHERE code = 'table_exception_contained'"
        ) == [(0,)]
        assert box.applier._contained_failures == []
    finally:
        box.close()


def test_huge_integer_table_binding_is_a_containable_data_rejection():
    """A table DML binding rejection must not escape the data boundary."""
    import duckdb

    from cdc_flight import destination_failure
    from cdc_flight.destination_failure import (
        DestinationDataRejection,
        MaterializationConnection,
        execute_table_dml,
    )

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE t_int (value INTEGER)")
        facade = MaterializationConnection(
            con,
            destination_failure._mint_table_data_provenance("app", "t_int", "t_int"),
        )
        with pytest.raises(DestinationDataRejection) as rejected:
            execute_table_dml(facade, "INSERT INTO t_int VALUES (?)", [10**100])
        assert isinstance(rejected.value.original, duckdb.InvalidInputException)
    finally:
        con.close()


def test_unmarked_large_production_module_is_in_measurement_set(tmp_path):
    """The ownership discovery probe must not disappear for lacking a marker."""
    import importlib.util
    from pathlib import Path

    source_path = (
        Path(__file__).resolve().parents[1]
        / "rubric"
        / "2.3_new_table_discovery"
        / "test_2_3_new_table_discovery.py"
    )
    spec = importlib.util.spec_from_file_location("ownership_guard_probe", source_path)
    assert spec is not None and spec.loader is not None
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)

    package = tmp_path / "cdc_flight"
    package.mkdir()
    (package / "__init__.py").write_text("\n", encoding="utf-8")
    empty = package / "empty_unmarked.py"
    empty.write_text("", encoding="utf-8")
    probe = package / "r16_unmarked_owner_probe.py"
    probe.write_text("x = 0\n" * 1001, encoding="utf-8")

    assert empty in guard._ownership_modules(package)
    assert probe in guard._ownership_modules(package)
    with pytest.raises(AssertionError, match=r"r16_unmarked_owner_probe\.py"):
        guard._assert_ownership_boundaries(package)
