"""FIX17 reproductions for the two repeated architectural findings."""

from __future__ import annotations

import ast
from pathlib import Path
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

    from cdc_flight.destination_failure import (
        DestinationDataRejection,
        execute_table_dml,
    )
    from cdc_flight.table_writer import _table_dml_connection

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE t_int (value INTEGER)")
        facade = _table_dml_connection(con, "t_int")
        with pytest.raises(DestinationDataRejection) as rejected:
            execute_table_dml(facade, "INSERT INTO t_int VALUES (?)", [10**100])
        assert isinstance(rejected.value.original, duckdb.InvalidInputException)
    finally:
        con.close()


def test_table_data_scope_rejects_a_statement_for_another_relation():
    """A source-A DML scope must not execute or attribute target-B SQL."""
    import duckdb

    from cdc_flight.destination_failure import execute_table_dml, executemany_table_dml
    from cdc_flight.table_writer import _table_dml_connection

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE target_a (value INTEGER)")
        con.execute("CREATE TABLE target_b (value INTEGER)")
        facade = _table_dml_connection(con, "target_a")

        with pytest.raises(ValueError, match=r"target_a|target_b"):
            execute_table_dml(facade, "INSERT INTO target_b VALUES (?)", [10**100])

        with pytest.raises(ValueError, match=r"target_a|target_b"):
            executemany_table_dml(facade, "INSERT INTO target_b VALUES (?)", [[10**100]])

        assert con.execute("SELECT count(*) FROM target_b").fetchone() == (0,)
    finally:
        con.close()


def test_table_data_scope_has_no_source_claim_to_forge():
    """A DML scope guards its target; source attribution remains with the plan."""
    import duckdb

    from cdc_flight import destination_failure
    from cdc_flight.destination_failure import (
        DestinationDataRejection,
        execute_table_dml,
    )
    from cdc_flight.table_writer import _table_dml_connection

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE target_a (value INTEGER)")
        con.execute("CREATE TABLE target_b (value INTEGER)")
        facade = _table_dml_connection(con, "target_a")
        assert not hasattr(destination_failure, "TableDataProvenance")
        assert not hasattr(destination_failure, "_mint_table_data_provenance")

        with pytest.raises(ValueError, match=r"target_b"):
            execute_table_dml(facade, "INSERT INTO target_b VALUES (?)", [10**100])
        assert con.execute("SELECT count(*) FROM target_b").fetchone() == (0,)

        with pytest.raises(DestinationDataRejection) as rejected:
            execute_table_dml(
                _table_dml_connection(con, "target_b"),
                "INSERT INTO target_b VALUES (?)",
                [10**100],
            )
        assert rejected.value.target == "target_b"
    finally:
        con.close()


def test_control_helper_failure_after_table_dml_stays_run_level(tmp_path, monkeypatch):
    """A control-plane helper must not inherit table-DML containment scope."""
    from support.applier_lab import Lab, data, end

    from cdc_flight import destination

    def fail(*_args, **_kwargs):
        raise RuntimeError("synthetic control-ledger serializer failure")

    monkeypatch.setattr(destination, "write_keyless_events", fail)
    box = Lab(tmp_path / "control-helper-run.duckdb")
    try:
        with pytest.raises(RuntimeError, match="synthetic control-ledger serializer failure"):
            box.run(
                [
                    data(
                        "control-helper-run",
                        1,
                        100,
                        table="control_helper_bad",
                        after={"id": 1, "name": "bad"},
                    ),
                    end("control-helper-run", 1, 101, {"app.control_helper_bad": 1}),
                ]
            )
        assert box.q("SELECT count(*) FROM _cdc_flight.schema_refusals") == [(0,)]
        assert box.q(
            "SELECT count(*) FROM _cdc_flight.alerts WHERE code = 'table_exception_contained'"
        ) == [(0,)]
        assert box.applier._contained_failures == []
    finally:
        box.close()


def test_containment_entry_set_is_closed_by_package_ast():
    """Every production route into containment is enumerated and named here."""
    root = Path(__file__).resolve().parents[2] / "src" / "cdc_flight"
    symbols = {
        "contain_table_failure",
        "contain_destination_failure",
        "as_contained_refusal",
        "mark_blocked_event",
    }
    found = []

    class Calls(ast.NodeVisitor):
        def __init__(self, path, module_aliases, function_aliases):
            self.path = path
            self.module_aliases = module_aliases
            self.function_aliases = function_aliases
            self.functions = []

        def visit_FunctionDef(self, node):
            self.functions.append(node.name)
            self.generic_visit(node)
            self.functions.pop()

        visit_AsyncFunctionDef = visit_FunctionDef

        def visit_Call(self, node):
            symbol = None
            if isinstance(node.func, ast.Attribute):
                if (
                    isinstance(node.func.value, ast.Name)
                    and node.func.value.id in self.module_aliases
                    and node.func.attr in symbols
                ):
                    symbol = node.func.attr
            elif isinstance(node.func, ast.Name):
                symbol = self.function_aliases.get(node.func.id)
            if symbol is not None:
                found.append((self.path.name, self.functions[-1], symbol, node.lineno))
            self.generic_visit(node)

    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        module_aliases = {"failure_containment"}
        function_aliases = {}
        for node in tree.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.endswith(".failure_containment"):
                        module_aliases.add(alias.asname or alias.name.rsplit(".", 1)[-1])
                continue
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.module and node.module.endswith("failure_containment"):
                for alias in node.names:
                    if alias.name in symbols:
                        function_aliases[alias.asname or alias.name] = alias.name
            if node.module in {None, "cdc_flight"}:
                for alias in node.names:
                    if alias.name == "failure_containment":
                        module_aliases.add(alias.asname or alias.name)
        Calls(path, module_aliases, function_aliases).visit(tree)

    assert sorted(row[:3] for row in found) == sorted(
        [
            ("applier.py", "_contain_destination_failure", "contain_destination_failure"),
            ("applier.py", "_contain_table_failure", "contain_table_failure"),
            ("planner.py", "_collect", "mark_blocked_event"),
            ("planner.py", "_materialization_refusal", "as_contained_refusal"),
        ],
        key=lambda row: row[:3],
    ), found
