"""Round-3 regressions for relation-generation activation fencing and matrix safety."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cdc_flight.catalog_poll import _ensure_toast_policies
from cdc_flight.catalog_state import SourceRelation
from cdc_flight.schema_evolution import SourceColumn
from cdc_flight.typed_types import SourceTypeDescriptor


class _Result:
    def __init__(self, value):
        self.value = value

    def fetchone(self):
        return self.value


class _Connection:
    def __init__(self):
        self.calls = []

    def execute(self, sql, _params=None):
        self.calls.append(sql)
        if "SELECT relreplident" in sql:
            return _Result(("f",))
        return _Result((200,))


class _AlterFailsConnection(_Connection):
    def execute(self, sql, _params=None):
        if "ALTER TABLE" in sql:
            self.calls.append(sql)
            raise RuntimeError("external identity change won the race")
        return super().execute(sql, _params)


def _relation(*, oid, relfilenode, relation_type_oid, replica_identity="f", boundary=None):
    residual = SourceTypeDescriptor(
        9600,
        "app.residual_type",
        "composite",
        composite_fields=(("payload", SourceTypeDescriptor(25, "pg_catalog.text", "text")),),
    )
    return SourceRelation(
        schema="app",
        table="generation_fence",
        oid=oid,
        relfilenode=relfilenode,
        relation_type_oid=relation_type_oid,
        published=True,
        replica_identity=replica_identity,
        full_activation_lsn=boundary,
        columns=(SourceColumn(1, "payload", residual.oid, residual.qualified_name, descriptor=residual),),
    )


def test_activation_boundary_is_not_copied_to_a_new_relation_generation():
    previous = _relation(oid=42, relfilenode=1000, relation_type_oid=100, boundary=100)
    current = _relation(oid=99, relfilenode=2000, relation_type_oid=100, boundary=None)
    watcher = SimpleNamespace(
        known={current.qualified: previous},
        binary_handling_mode="base64",
        hstore_handling_mode="map",
    )
    con = _Connection()
    observed = _ensure_toast_policies(
        watcher, con, {current.qualified: current}, activation_lsn=101
    )
    assert len(con.calls) >= 3
    assert observed[current.qualified].full_activation_lsn == 200


def test_activation_boundary_is_copied_only_for_the_same_complete_generation():
    previous = _relation(oid=42, relfilenode=1000, relation_type_oid=100, boundary=100)
    current = _relation(oid=42, relfilenode=1000, relation_type_oid=100, boundary=None)
    watcher = SimpleNamespace(
        known={current.qualified: previous},
        binary_handling_mode="base64",
        hstore_handling_mode="map",
    )
    con = _Connection()
    observed = _ensure_toast_policies(
        watcher, con, {current.qualified: current}, activation_lsn=101
    )
    assert observed[current.qualified].full_activation_lsn == 100
    assert con.calls == []


def test_activation_boundary_is_invalidated_when_full_is_lost():
    previous = _relation(oid=42, relfilenode=1000, relation_type_oid=100, boundary=100)
    current = _relation(
        oid=42, relfilenode=1000, relation_type_oid=100,
        replica_identity="d", boundary=None,
    )
    watcher = SimpleNamespace(
        known={current.qualified: previous},
        binary_handling_mode="base64",
        hstore_handling_mode="map",
    )
    con = _AlterFailsConnection()
    observed = _ensure_toast_policies(
        watcher, con, {current.qualified: current}, activation_lsn=101
    )
    assert observed[current.qualified].full_activation_lsn is None
    assert any("ALTER TABLE" in call for call in con.calls)


def test_unexpected_physical_matrix_exceptions_are_not_classified_as_unreachable(monkeypatch):
    from cdc_flight import physical_row_matrix

    def explode(*_args, **_kwargs):
        raise RuntimeError("unexpected matrix bug")

    monkeypatch.setattr(physical_row_matrix, "_base_table", explode)
    cell = next(
        cell for cell in physical_row_matrix.declared_cells()
        if cell.operation == "insert"
        and cell.field_state == "value"
        and cell.base_state == "start"
        and cell.storage == "memory"
        and cell.outcome == "commit"
        and cell.identity == "keyed"
        and cell.schema_epoch == "pre"
    )
    with pytest.raises(RuntimeError, match="unexpected matrix bug"):
        physical_row_matrix.exercise_cell(cell)
