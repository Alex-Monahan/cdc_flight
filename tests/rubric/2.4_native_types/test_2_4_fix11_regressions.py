"""FIX ROUND 11 regressions for the output-function and refusal boundaries."""

from __future__ import annotations

import ast
import os
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from cdc_flight import destination, errors
from cdc_flight.typed_types import (
    SourceTypeDescriptor,
    adapt_value,
    native_type,
)

ROOT = Path(__file__).resolve().parents[3]


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def test_xml_values_use_the_postgresql_output_function_boundary():
    """The connector value is admitted, including xml_out's known normalization."""
    target = native_type(_source("xml", 142))
    assert target.sql == "VARCHAR"
    cases = (
        ("<a/>", "<a/>"),
        ("<a>fat</a>", "<a>fat</a>"),
        ('<?xml version="1.0" standalone="yes"?><b/>',
         '<?xml version="1.0" standalone="yes"?><b/>'),
        ('<?xml version="1.1"?><c/>', '<?xml version="1.1"?><c/>'),
    )
    for wire, expected_output_function_value in cases:
        assert adapt_value(wire, target) == expected_output_function_value


def test_money_carries_connector_text_without_locale_logic():
    c_source = SourceTypeDescriptor(790, "pg_catalog.money", "money")
    gb_source = SourceTypeDescriptor(
        790,
        "pg_catalog.money",
        "money",
        metadata=(("lc_monetary", "en_GB.UTF-8"),),
    )
    assert adapt_value("1234.56", native_type(c_source)) == "1234.56"
    assert adapt_value("1234.56", native_type(gb_source)) == "1234.56"
    assert adapt_value("-1.00", native_type(gb_source)) == "-1.00"
    assert adapt_value("£1,234.56", native_type(gb_source)) == "£1,234.56"
    assert adapt_value("$1,234.56", native_type(gb_source)) == "$1,234.56"


def test_refusal_identity_does_not_change_with_origin_class(tmp_path):
    """The durable writer owns one class; observing seams cannot supply another."""
    con = duckdb.connect(str(tmp_path / "refusal.duckdb"))
    try:
        destination.ensure_control_schema(con)
        first = destination.record_schema_refusal(
            con,
            pipeline="round11",
            source_schema="app",
            source_table="changing_bad",
            target_table="cdcflight_app_changing_bad",
            detected_lsn=100,
            reason="value refusal",
            input_fingerprint="same-descriptor",
            source_fingerprint="same-descriptor",
        )
        second = destination.record_schema_refusal(
            con,
            pipeline="round11",
            source_schema="app",
            source_table="changing_bad",
            target_table="cdcflight_app_changing_bad",
            detected_lsn=101,
            reason="catalog refusal after the row image changed",
            input_fingerprint="same-descriptor",
            source_fingerprint="same-descriptor",
        )
        assert first == "pending"
        assert second == "quarantined"
        assert con.execute(
            "SELECT state, refusal_class FROM _cdc_flight.schema_refusals "
            "WHERE pipeline='round11' AND source_table='changing_bad'"
        ).fetchall() == [("quarantined", "SchemaEvolutionRefused")]
        assert not destination.quarantine_retry_allowed(
            con,
            pipeline="round11",
            source_schema="app",
            source_table="changing_bad",
            source_exists=True,
            source_fingerprint="same-descriptor",
        )
        assert destination.quarantine_retry_allowed(
            con,
            pipeline="round11",
            source_schema="app",
            source_table="changing_bad",
            source_exists=True,
            source_fingerprint="repaired-descriptor",
        )
    finally:
        con.close()


def test_refusal_context_does_not_guess_the_first_relation_in_a_commit_group():
    """A multi-table refusal remains unscoped unless its origin names a table."""
    from cdc_flight.applier import Applier

    applier = object.__new__(Applier)
    applier.group = SimpleNamespace(
        units=[SimpleNamespace(events=[
            SimpleNamespace(
                schema="app", table="healthy", qualified_table="app.healthy", lsn=10
            ),
            SimpleNamespace(
                schema="app", table="bad", qualified_table="app.bad", lsn=11
            ),
        ])]
    )
    refused = errors.SchemaEvolutionRefused(
        "descriptor batch failed", refusal_origin="catalog_poll"
    )
    applier._contextualize_schema_refusal(refused)
    assert (refused.source_schema, refused.source_table, refused.target) == (
        None, None, None
    )
    assert refused.detected_lsn == 11


def test_every_production_refusal_raise_declares_a_registered_origin():
    """A new refusal site must opt into the central authority explicitly."""
    source_root = ROOT / "src" / "cdc_flight"
    refusal_names = {"SchemaEvolutionRefused", "SchemaBackfillRefused", "SchemaShapeUnexplained"}
    seen_modules: set[str] = set()

    def callee_name(node):
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return None

    def exception_names(node):
        if isinstance(node, (ast.Tuple, ast.List)):
            return {name for child in node.elts for name in exception_names(child)}
        name = callee_name(node)
        return {name} if name else set()

    for path in sorted(source_root.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        module = path.stem
        for node in ast.walk(tree):
            if not isinstance(node, ast.Raise) or not isinstance(node.exc, ast.Call):
                continue
            if callee_name(node.exc.func) not in refusal_names:
                continue
            seen_modules.add(module)
            origin = next(
                (keyword.value.value for keyword in node.exc.keywords
                 if keyword.arg == "refusal_origin" and isinstance(keyword.value, ast.Constant)),
                None,
            )
            declarations = getattr(errors, "REFUSAL_ORIGIN_BY_MODULE", {})
            assert module in declarations, module
            assert origin == declarations[module], (module, origin)
            assert not any(keyword.arg == "refusal_class" for keyword in node.exc.keywords)
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler):
                continue
            caught = exception_names(node.type)
            if caught & refusal_names:
                assert "SchemaEvolutionRefused" in caught, (path.name, node.lineno, caught)
                assert not caught & (refusal_names - {"SchemaEvolutionRefused"}), (
                    path.name, node.lineno, caught
                )
    declarations = getattr(errors, "REFUSAL_ORIGIN_BY_MODULE", {})
    assert seen_modules == set(declarations), (
        seen_modules,
        set(declarations),
    )


@pytest.mark.slow
@pytest.mark.e2e
def test_live_unsupported_add_column_contains_the_table_and_advances_both_slot_positions(
    sandbox,
):
    """A live unsupported DDL must not starve a healthy co-published table."""
    box = sandbox
    box.reseed()
    box.sql(
        [
            "CREATE TABLE app.fix11_live (id integer PRIMARY KEY, name text)",
            "CREATE TABLE app.fix11_peer (id integer PRIMARY KEY, name text)",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.fix11_live, app.fix11_peer",
        ],
        one_transaction=True,
    )
    box.env["CDC_TABLES"] = "fix11_live,fix11_peer"
    box.env["CDC_AUTO_DISCOVERY"] = "0"
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    try:
        baseline = box.run(reset_state=True, max_seconds=20)
        assert baseline["ok"] is True, baseline

        box.sql(
            [
                "ALTER TABLE app.fix11_live "
                "ADD COLUMN v_box box DEFAULT '((0,0),(1,1))'::box, "
                "ADD COLUMN v_oidvector oidvector DEFAULT '1 2'::oidvector, "
                "ADD COLUMN v_xid8 xid8 DEFAULT '1'::xid8",
                "INSERT INTO app.fix11_live (id, name) VALUES (1, 'bad')",
                "INSERT INTO app.fix11_peer VALUES (1, 'peer-1')",
            ],
            one_transaction=True,
        )
        metrics = []
        runs = []
        for attempt in range(4):
            box.sql(
                [
                    f"UPDATE app.fix11_live SET name='bad-image-{attempt + 2}' WHERE id=1",
                    f"INSERT INTO app.fix11_peer VALUES ({attempt + 2}, "
                    f"'peer-{attempt + 2}')",
                ],
                one_transaction=True,
            )
            runs.append(box.run(max_seconds=20, min_records=1, expect_success=False))
            box.sql("CHECKPOINT")
            metrics.append(
                box.pg_query(
                    "SELECT restart_lsn::text, confirmed_flush_lsn::text, "
                    "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint "
                    "FROM pg_replication_slots WHERE slot_name=%s",
                    (box.slot,),
                )[0]
            )

        assert all(run["ok"] is False for run in runs), runs
        assert box.duck_query(
            "SELECT state, refusal_class FROM _cdc_flight.schema_refusals "
            "WHERE source_table='fix11_live'"
        ) == [("quarantined", "SchemaEvolutionRefused")]
        assert box.duck_query(
            "SELECT count(*) FROM _cdc_flight.alerts "
            "WHERE code='schema_table_quarantined' "
            "AND context LIKE '%\"source_table\": \"fix11_live\"%'"
        ) == [(1,)]
        assert box.duck_query(
            f"SELECT id, name FROM {box.table('cdcflight_app_fix11_peer')} ORDER BY id"
        ) == [(1, "peer-1"), (2, "peer-2"), (3, "peer-3"), (4, "peer-4"), (5, "peer-5")]
        assert len({row[0] for row in metrics}) > 1
        assert len({row[1] for row in metrics}) > 1
        assert all(int(row[2]) >= 0 for row in metrics)
        # The source cluster is shared by xdist workers, so pg_current_wal_lsn()
        # includes unrelated workers' traffic under the full lane.  The standalone
        # probe is the strict retention proof; the parallel lane still records every
        # slot metric and enforces non-negative retention.
        if "PYTEST_XDIST_WORKER" not in os.environ:
            assert max(int(row[2]) for row in metrics) < 1_000_000
        print(f"round11 add-column box/oidvector/xid8 metrics: {metrics}")
    finally:
        box.reseed()
