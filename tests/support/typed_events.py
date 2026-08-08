"""Schema-bearing Debezium JSON fixtures for type-handling tests."""

from __future__ import annotations

import json


def _field(name: str, schema: dict, value):
    return {"name": name, "schema": schema, "value": value}


def schema_enabled_event(*, value: object, key: object = {"id": 1}, op: str = "c") -> str:
    integer = {"type": "int32", "optional": False}
    text = {"type": "string", "optional": True}
    row = {
        "type": "struct",
        "name": "io.debezium.connector.postgresql.Source",
        "fields": [
            _field("id", integer, 1),
            _field("payload", text, value),
        ],
    }
    envelope = {
        "type": "struct",
        "name": "io.debezium.connector.postgresql.Envelope",
        "fields": [
            _field("before", {"type": "struct", "fields": row["fields"], "optional": True}, None),
            _field("after", {"type": "struct", "fields": row["fields"], "optional": True}, {"id": 1, "payload": value}),
            _field("source", {"type": "struct", "fields": []}, {"schema": "app", "table": "typed_rows"}),
            _field("op", {"type": "string"}, op),
        ],
    }
    return json.dumps({"schema": envelope, "payload": {
        "before": None,
        "after": {"id": 1, "payload": value},
        "source": {"schema": "app", "table": "typed_rows"},
        "op": op,
    }})


def schema_enabled_key() -> str:
    return json.dumps({
        "schema": {
            "type": "struct",
            "name": "app.typed_rows.Key",
            "fields": [{"name": "id", "type": {"type": "int32"}}],
        },
        "payload": {"id": 1},
    })

