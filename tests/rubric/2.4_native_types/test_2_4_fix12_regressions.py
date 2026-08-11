"""FIX ROUND 12: the two recurring containment/type regressions."""

from __future__ import annotations

import ast
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from psycopg import sql

from cdc_flight import destination, errors, machines, planner
from cdc_flight.envelope import KIND_DATA, PendingRecord
from cdc_flight.typed_types import (
    InvalidTypedValue,
    SourceTypeDescriptor,
    TypedValueError,
    UnsupportedType,
    adapt_value,
    native_type,
)

ROOT = Path(__file__).resolve().parents[3]


class SyntheticTypedFailure(TypedValueError):
    """A test-only sibling used to attack the planner's common catch boundary."""


def _money(locale: str) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(
        790,
        "pg_catalog.money",
        "money",
        metadata=(("lc_monetary", locale),),
    )


@pytest.mark.parametrize(
    "locale, delivered",
    [
        ("C", "$1,234.56"),
        ("en_US.UTF-8", "$1,234.56"),
        ("en_GB.UTF-8", "£1,234.56"),
        ("en_IN.UTF-8", "₹1,234.56"),
    ],
)
def test_money_is_a_plain_varchar_transport_boundary(locale, delivered):
    """Money never asks Python to recreate PostgreSQL's output text."""
    target = native_type(_money(locale))
    assert target.sql == "VARCHAR"
    assert adapt_value(delivered, target) == delivered


@pytest.mark.slow
@pytest.mark.e2e
def test_money_live_connector_text_survives_four_monetary_locales(sandbox):
    """Verification-only: every locale is a successful VARCHAR transport run."""
    box = sandbox
    box.reseed()
    admin = replace(box.source, dbname="postgres")
    locales = ("C", "en_US.UTF-8", "en_GB.UTF-8", "en_IN.UTF-8")
    values = (1234.56, 1235.67, 1236.78, 1237.89)
    capture = {
        "CDC_TABLES": "fix12_money",
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_CATALOG_POLL_SECONDS": "1",
    }
    evidence = []
    try:
        box.sql(
            [
                "CREATE TABLE app.fix12_money (id integer PRIMARY KEY, amount money)",
                "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.fix12_money",
            ],
            one_transaction=True,
        )
        for index, (locale, value) in enumerate(zip(locales, values, strict=True)):
            with psycopg.connect(admin.dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("ALTER DATABASE {} SET lc_monetary = {}").format(
                        sql.Identifier(box.source.dbname), sql.Literal(locale)
                    )
                )
            if index == 0:
                box.sql(
                    f"INSERT INTO app.fix12_money VALUES (1, {value}::numeric::money)"
                )
            else:
                box.sql(
                    f"UPDATE app.fix12_money SET amount = {value}::numeric::money "
                    "WHERE id = 1"
                )
            source_output = box.pg_query(
                "SELECT format('%s', amount) FROM app.fix12_money WHERE id=1"
            )[0][0]
            # Stock Debezium's money converter delivers the numeric spelling, not a
            # locale-rendered display string.  This is the source-side wire oracle
            # for the value that the plain VARCHAR branch must carry unchanged.
            connector_text = box.pg_query(
                "SELECT amount::numeric::text FROM app.fix12_money WHERE id=1"
            )[0][0]
            run = box.run(
                reset_state=index == 0,
                extra_env=capture,
                max_seconds=30,
            )
            destination_text = box.duck_query(
                "SELECT amount FROM cdc_raw.cdcflight_app_fix12_money WHERE id=1"
            )[0][0]
            evidence.append({
                "locale": locale,
                "source_output": source_output,
                "connector_delivered": connector_text,
                "destination": destination_text,
                "ok": run["ok"],
                "refusals": box.duck_query(
                    "SELECT count(*) FROM _cdc_flight.schema_refusals "
                    "WHERE source_table='fix12_money'"
                )[0][0],
            })
            assert run["ok"] is True, run
            assert destination_text == connector_text, evidence[-1]
        assert any(
            "£" in item["source_output"] or "₹" in item["source_output"]
            for item in evidence
        )
        assert all(item["refusals"] == 0 for item in evidence), evidence
        print("FIX12 money locale evidence:", json.dumps(evidence, ensure_ascii=False))
    finally:
        with psycopg.connect(admin.dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("ALTER DATABASE {} RESET lc_monetary").format(
                    sql.Identifier(box.source.dbname)
                )
            )
        box.sql("DROP TABLE IF EXISTS app.fix12_money")
        # This module deliberately reuses one sandbox. Remove the destination's
        # test-only identity as well; otherwise the next lifecycle scenario would
        # correctly observe a stale obligation for this intentionally dropped table.
        box.duck_write("DROP TABLE IF EXISTS cdc_raw.cdcflight_app_fix12_money")
        for control_table in ("table_state", "source_relations", "schema_refusals"):
            box.duck_write(
                f"DELETE FROM _cdc_flight.{control_table} WHERE source_table = 'fix12_money'"
            )


def _slot_metrics(box):
    box.sql("CHECKPOINT")
    return box.pg_query(
        "SELECT restart_lsn::text, confirmed_flush_lsn::text, "
        "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint "
        "FROM pg_replication_slots WHERE slot_name=%s",
        (box.slot,),
    )[0]


@pytest.mark.slow
@pytest.mark.e2e
def test_quarantined_drop_is_discharged_without_resnapshot_or_slot_starvation(sandbox):
    """Exercise every adjacent source lifecycle while one table is quarantined.

    The four bad relations share one healthy peer.  After four refusal runs, the
    source performs a pure drop, truncate, rename, and drop-then-recreate with a new
    relation identity.  The same four subsequent runs prove the obligation matrix and
    record both slot positions plus retained WAL on every run.
    """
    box = sandbox
    box.reseed()
    bad_tables = (
        "fix12_drop",
        "fix12_truncate",
        "fix12_rename",
        "fix12_recreate",
    )
    box.sql(
        [
            *[
                f"CREATE TABLE app.{table} (id integer PRIMARY KEY, name text)"
                for table in bad_tables
            ],
            "CREATE TABLE app.fix12_peer (id integer PRIMARY KEY, name text)",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE "
            + ", ".join(f"app.{table}" for table in (*bad_tables, "fix12_peer")),
        ],
        one_transaction=True,
    )
    capture = {
        "CDC_TABLES": ",".join((*bad_tables, "fix12_peer")),
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_CATALOG_POLL_SECONDS": "1",
        "CDC_DROP_CONFIRM_POLLS": "1",
    }
    try:
        baseline = box.run(reset_state=True, extra_env=capture, max_seconds=25)
        assert baseline["ok"] is True, baseline
        box.sql(
            [
                f"ALTER TABLE app.{table} ADD COLUMN v_box box "
                "DEFAULT '((0,0),(1,1))'::box"
                for table in bad_tables
            ]
            + [
                f"INSERT INTO app.{table} (id, name) VALUES (1, '{table}-bad')"
                for table in bad_tables
            ]
            + ["INSERT INTO app.fix12_peer VALUES (1, 'peer-1')"],
            one_transaction=True,
        )
        refusal_runs = []
        for _ in range(8):
            refusal_runs.append(
                box.run(extra_env=capture, max_seconds=35, expect_success=False)
            )
            states = dict(
                box.duck_query(
                    "SELECT source_table, state FROM _cdc_flight.schema_refusals "
                    "WHERE source_table LIKE 'fix12_%'"
                )
            )
            if all(states.get(table) == "quarantined" for table in bad_tables):
                break
        assert all(run["ok"] is False for run in refusal_runs), refusal_runs
        assert box.duck_query(
            "SELECT source_table, state FROM _cdc_flight.schema_refusals "
            "WHERE source_table LIKE 'fix12_%' ORDER BY source_table"
        ) == [(table, "quarantined") for table in sorted(bad_tables)]

        # Four neighbouring events occur before the next run.  The recreate has a
        # fresh OID/relfilenode because it is a DROP followed by CREATE, but the old
        # quarantine is still the durable state when the run starts.
        box.sql(
            [
                "DROP TABLE app.fix12_drop",
                "TRUNCATE TABLE app.fix12_truncate",
                "ALTER TABLE app.fix12_rename RENAME TO fix12_renamed",
                "DROP TABLE app.fix12_recreate",
                "CREATE TABLE app.fix12_recreate "
                "(id integer PRIMARY KEY, name text)",
                "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.fix12_recreate",
                "INSERT INTO app.fix12_recreate VALUES (1, 'recreated')",
            ],
            one_transaction=True,
        )
        metrics = []
        runs = []
        for identifier in range(2, 6):
            box.sql(
                f"INSERT INTO app.fix12_peer VALUES ({identifier}, 'peer-{identifier}')"
            )
            runs.append(
                box.run(
                    extra_env=capture,
                    max_seconds=35,
                    min_records=1,
                    expect_success=False,
                )
            )
            metrics.append(_slot_metrics(box))

        assert all(run.get("ok") is False for run in runs), runs
        assert box.duck_query(
            "SELECT source_table, snapshot_state FROM _cdc_flight.table_state "
            "WHERE source_table IN ('fix12_drop', 'fix12_rename', 'fix12_recreate', "
            "'fix12_truncate') ORDER BY source_table"
        ) == [
            ("fix12_drop", "gone"),
            ("fix12_recreate", "complete"),
            ("fix12_rename", "gone"),
            ("fix12_truncate", "awaiting_snapshot"),
        ]
        assert box.duck_query(
            "SELECT source_table, state FROM _cdc_flight.schema_refusals "
            "WHERE source_table IN ('fix12_drop', 'fix12_rename', 'fix12_recreate', "
            "'fix12_truncate') ORDER BY source_table"
        ) == [
            ("fix12_drop", "resolved"),
            ("fix12_recreate", "resolved"),
            ("fix12_rename", "resolved"),
            ("fix12_truncate", "quarantined"),
        ]
        assert box.duck_query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='cdc_raw' AND table_name IN "
            "('cdcflight_app_fix12_drop', 'cdcflight_app_fix12_rename')"
        ) == []
        assert box.duck_query(
            "SELECT id, name FROM cdc_raw.cdcflight_app_fix12_recreate ORDER BY id"
        ) == [(1, "recreated")]
        assert box.duck_query(
            'SELECT id, name FROM cdc_raw.cdcflight_app_fix12_peer ORDER BY id'
        ) == [(1, "peer-1"), (2, "peer-2"), (3, "peer-3"), (4, "peer-4"), (5, "peer-5")]
        assert len({row[0] for row in metrics}) == len(metrics)
        assert len({row[1] for row in metrics}) == len(metrics)
        assert all(int(row[2]) >= 0 for row in metrics)
        print("FIX12 drop quarantine metrics:", json.dumps(metrics))
    finally:
        box.sql(
            [
                *[
                    f"DROP TABLE IF EXISTS app.{table}"
                    for table in (*bad_tables, "fix12_renamed")
                ],
                "DROP TABLE IF EXISTS app.fix12_peer",
            ]
        )


def test_every_typed_value_error_has_one_common_base():
    assert issubclass(UnsupportedType, errors.TypedValueError)
    assert issubclass(InvalidTypedValue, errors.TypedValueError)


def test_planner_contains_a_sibling_typed_value_error_at_the_common_boundary(monkeypatch):
    """A sibling error must become a durable refusal, never escape the planner."""
    plan = object.__new__(planner.GroupPlan)
    plan.blocked_tables = set()
    plan.snapshots = SimpleNamespace(target_table=lambda _schema, _table: "target")
    plan.commit_id = 1
    plan.work = {}
    plan.binary_handling_mode = "base64"
    plan.hstore_handling_mode = "map"
    plan.toast_admission_provider = None
    plan.toast_policy_provider = None
    plan._active_txn_id = None
    plan.stats = {
        "events": 0,
        "first_lsn": None,
        "last_lsn": None,
        "max_source_ts": None,
    }
    plan._enrich_descriptors = lambda _event: None

    event = PendingRecord(
        raw=None,
        kind=KIND_DATA,
        topic="p.app.money",
        nbytes=1,
        op="c",
        schema="app",
        table="money",
        lsn=123,
        key={"id": 1},
        after={"id": 1},
    )

    def sibling_error(*_args, **_kwargs):
        raise SyntheticTypedFailure("synthetic sibling typed-value failure")

    monkeypatch.setattr(planner.table_work, "patch_for", sibling_error)
    with pytest.raises(errors.SchemaEvolutionRefused):
        plan._collect(event, snapshot=None, target="target", event_id="event-1")


def test_typed_value_catch_boundaries_name_the_common_base():
    """The source guard makes concrete sibling catches impossible to reintroduce."""
    tree = ast.parse((ROOT / "src" / "cdc_flight" / "planner.py").read_text())
    catches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ExceptHandler)
        and isinstance(node.type, ast.Name)
        and node.type.id in {"InvalidTypedValue", "UnsupportedType"}
    ]
    assert catches == []
    assert any(
        isinstance(node, ast.ExceptHandler)
        and isinstance(node.type, ast.Name)
        and node.type.id == "TypedValueError"
        for node in ast.walk(tree)
    )


def _class_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            names.add(node.name)
    return names


def _exception_names(node: ast.AST | None) -> set[str]:
    if node is None:
        return set()
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Attribute):
        return {node.attr}
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        result: set[str] = set()
        for child in node.elts:
            result |= _exception_names(child)
        return result
    return set()


def test_all_typed_exception_classes_and_external_catches_are_closed():
    """The code, not a hand-maintained list, defines the closure being tested."""
    typed_path = ROOT / "src" / "cdc_flight" / "typed_types.py"
    typed_tree = ast.parse(typed_path.read_text())
    typed_classes = _value_error_class_names(typed_tree)
    assert typed_classes
    assert all(_inherits(typed_tree, name, "TypedValueError") for name in typed_classes)
    assert typed_classes == {"UnsupportedType", "InvalidTypedValue"}
    import cdc_flight.typed_types as typed_module

    runtime_classes = {
        name: value
        for name, value in vars(typed_module).items()
        if isinstance(value, type)
        and value.__module__ == typed_module.__name__
        and issubclass(value, ValueError)
    }
    assert set(runtime_classes) == typed_classes
    assert all(issubclass(value, TypedValueError) for value in runtime_classes.values())

    boundary_files = []
    for path in sorted((ROOT / "src" / "cdc_flight").glob("*.py")):
        if path == typed_path:
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        handlers = [node for node in ast.walk(tree) if isinstance(node, ast.ExceptHandler)]
        names_by_handler = [(node, _exception_names(node.type)) for node in handlers]
        if any("TypedValueError" in names for _node, names in names_by_handler):
            boundary_files.append(path.name)
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler):
                continue
            names = _exception_names(node.type)
            if names & typed_classes:
                assert "TypedValueError" in names, (path.name, node.lineno, names)
    assert boundary_files, "no typed-value containment boundary catches the common base"
    # A new class discovered above is covered by every boundary because Python
    # dispatches all TypedValueError subclasses through the same handler.
    assert all(
        any(
            "TypedValueError" in _exception_names(node.type)
            for node in ast.walk(ast.parse((ROOT / "src" / "cdc_flight" / name).read_text()))
            if isinstance(node, ast.ExceptHandler)
        )
        for name in boundary_files
    )


def _value_error_class_names(tree: ast.AST) -> set[str]:
    """Enumerate declared ValueError descendants without naming them manually."""
    classes = {
        node.name: tuple(
            name for base in node.bases for name in _exception_names(base)
        )
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef)
    }
    return {
        name for name in classes
        if _inherits_from_any(classes, name, {"ValueError", "TypedValueError"})
    }


def _inherits_from_any(classes: dict[str, tuple[str, ...]], name: str, roots: set[str], seen=None) -> bool:
    seen = set() if seen is None else seen
    if name in roots:
        return True
    if name in seen:
        return False
    seen.add(name)
    return any(
        _inherits_from_any(classes, base, roots, seen)
        for base in classes.get(name, ())
    )


def _inherits(tree: ast.AST, name: str, root: str) -> bool:
    classes = {
        node.name: tuple(
            base_name for base in node.bases for base_name in _exception_names(base)
        )
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef)
    }
    return _inherits_from_any(classes, name, {root})


def test_a_new_scratch_sibling_would_fail_the_raise_side_guard():
    """The closure assertion rejects a future sibling that skips the common base."""
    source = (ROOT / "src" / "cdc_flight" / "typed_types.py").read_text()
    scratch = ast.parse(source + "\nclass ScratchTypedFailure(ValueError):\n    pass\n")
    with pytest.raises(AssertionError):
        for name in _value_error_class_names(scratch):
            assert _inherits(scratch, name, "TypedValueError"), name


def test_quarantined_source_disappearance_is_a_declared_terminal_transition():
    assert machines.LIFECYCLE_GONE in machines.TABLE_LIFECYCLE.states
    assert machines.LIFECYCLE_GONE in machines.TABLE_LIFECYCLE.terminal
    machines.TABLE_LIFECYCLE.check(
        machines.LIFECYCLE_AWAITING,
        machines.LIFECYCLE_GONE,
    )
    assert machines.LIFECYCLE_GONE not in machines.LIFECYCLE_OWING_WORK


def test_gone_name_is_quiet_when_absent_and_recreated_when_present():
    from cdc_flight.catalog import CHANGE_RECREATED, CatalogWatcher
    from cdc_flight.catalog_state import SourceRelation
    from cdc_flight.schema_evolution import SourceColumn

    text = SourceTypeDescriptor(25, "pg_catalog.text", "text")
    relation = SourceRelation(
        schema="app",
        table="gone_again",
        oid=9001,
        relfilenode=9002,
        relation_type_oid=25,
        published=True,
        replica_identity="d",
        columns=(SourceColumn(1, "payload", 25, "text", descriptor=text),),
    )
    watcher = CatalogWatcher(
        dsn="",
        publication="pub",
        schema="app",
        schemas={"app"},
        include={relation.qualified},
        gone={relation.qualified},
        emit_marker=False,
        confirm_polls=1,
    )
    assert watcher._compare({}, 100) == []
    changes = watcher._compare({relation.qualified: relation}, 101)
    assert [change.kind for change in changes] == [CHANGE_RECREATED]
    assert watcher.gone == set()


def test_source_missing_discharge_removes_stale_target_and_resolves_atomically(tmp_path):
    import duckdb

    from cdc_flight.resnapshot_source_policy import (
        EmptinessEvidence,
        discharge_quarantined_source_missing,
    )

    con = duckdb.connect(str(tmp_path / "gone.duckdb"))
    try:
        destination.ensure_control_schema(con)
        con.execute("CREATE SCHEMA raw")
        con.execute('CREATE TABLE raw."cdcflight_app_gone" (id INTEGER)')
        con.execute('INSERT INTO raw."cdcflight_app_gone" VALUES (1)')
        destination.request_snapshot(
            con,
            pipeline="fix12",
            tables=[("app", "gone", "cdcflight_app_gone")],
            detail="test quarantine",
        )
        destination.record_schema_refusal(
            con,
            pipeline="fix12",
            source_schema="app",
            source_table="gone",
            target_table="cdcflight_app_gone",
            detected_lsn=10,
            reason="bad value",
            input_fingerprint="same",
        )
        destination.record_schema_refusal(
            con,
            pipeline="fix12",
            source_schema="app",
            source_table="gone",
            target_table="cdcflight_app_gone",
            detected_lsn=11,
            reason="still bad",
            input_fingerprint="same",
        )
        discharged = discharge_quarantined_source_missing(
            con,
            pipeline="fix12",
            dataset="raw",
            tables=[("app", "gone", "cdcflight_app_gone")],
            evidence=EmptinessEvidence(
                snapshot_phase_ended=True,
                tables_seen=set(),
                source_empty_at={},
                wal_lsn=123,
                source_missing={"app.gone"},
            ),
        )
        assert discharged == ["app.gone"]
        assert con.execute(
            "SELECT snapshot_state, snapshot_lsn FROM _cdc_flight.table_state "
            "WHERE source_table='gone'"
        ).fetchall() == [("gone", 123)]
        assert con.execute(
            "SELECT state FROM _cdc_flight.schema_refusals WHERE source_table='gone'"
        ).fetchall() == [("resolved",)]
        assert con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='raw' AND table_name='cdcflight_app_gone'"
        ).fetchall() == []
    finally:
        con.close()
