"""Exercise PostgreSQL's type OUTPUT-function boundary for every §8.3 family.

The source query deliberately uses ``format(chr(37) || 's', value)``.  PostgreSQL
then supplies the value through its catalog-resolved type OUTPUT function; this
probe never uses a ``::text`` cast and never renders a Python value into policy
input.  Only type identities, lengths, digests, and booleans are printed.
"""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from pathlib import Path

import psycopg

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR / "src"))

from cdc_flight.catalog_descriptors import CatalogDescriptorReader  # noqa: E402
from cdc_flight.config import SourceConfig  # noqa: E402
from cdc_flight.naming import quote  # noqa: E402
from cdc_flight.policy import PIIPolicy, PolicyGate, PostgreSQLOutputText  # noqa: E402
from cdc_flight.typed_types import native_type  # noqa: E402

TABLE = "p8_output_oracle_probe"
COMPOSITE = "p8_output_oracle_pair"
COLUMNS = (
    "money_value",
    "xml_value",
    "int_array",
    "int_range",
    "composite_value",
    "inet_value",
    "cidr_value",
    "json_value",
    "jsonb_value",
    "toast_value",
)


def _source() -> SourceConfig:
    source = SourceConfig()
    if source.port != 15432:
        raise RuntimeError("p8 output probe must use CDC_TEST_PGPORT/PGPORT 15432")
    return source


def _setup(con) -> None:
    con.execute(f"DROP TABLE IF EXISTS app.{TABLE} CASCADE")
    con.execute(f"DROP TYPE IF EXISTS app.{COMPOSITE} CASCADE")
    con.execute(
        f"CREATE TYPE app.{COMPOSITE} AS (left_value integer, right_value text)"
    )
    con.execute(
        f"CREATE TABLE app.{TABLE} ("
        "id integer PRIMARY KEY, money_value money, xml_value xml, "
        "int_array integer[], int_range int4range, "
        f"composite_value app.{COMPOSITE}, "
        "inet_value inet, cidr_value cidr, json_value json, jsonb_value jsonb, "
        "toast_value text)"
    )
    con.execute(
        f"INSERT INTO app.{TABLE} VALUES ("
        "1, '1234.56'::money, '<p8>oracle</p8>'::xml, "
        "ARRAY[2, 3, 5, 7], int4range(10, 20), "
        "ROW(42, 'composite-output')::app.p8_output_oracle_pair, "
        "'192.0.2.7/32'::inet, '192.0.2.0/24'::cidr, "
        "'{\"family\":\"json\",\"n\":7}'::json, "
        "'{\"family\":\"jsonb\",\"n\":[7,8]}'::jsonb, "
        "repeat('P8_TOAST_SENTINEL', 12000))"
    )


def _catalog_rows(con) -> list[tuple[str, int]]:
    return [
        (str(name), int(oid))
        for name, oid in con.execute(
            "SELECT a.attname, a.atttypid::bigint "
            "FROM pg_attribute a "
            "JOIN pg_class c ON c.oid = a.attrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'app' AND c.relname = %s "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum",
            (TABLE,),
        ).fetchall()
        if str(name) in COLUMNS
    ]


def _output_query() -> str:
    # Do not replace this with a cast or a Python formatter.  The expression is
    # the source-side OUTPUT-function oracle used by catalog_support.py.
    expressions = []
    for name in COLUMNS:
        column = quote(name)
        expressions.extend(
            [
                f"format(chr(37) || 's', {column})",
                f"format(chr(37) || 's', {column})",
            ]
        )
    return f"SELECT {', '.join(expressions)} FROM app.{quote(TABLE)} WHERE id = 1"


def main() -> None:
    source = _source()
    findings: dict[str, object] = {"probe": "p8_output_oracle"}
    with tempfile.TemporaryDirectory(prefix="p8-output-oracle-") as temp:
        salt_path = Path(temp) / "salt"
        salt_path.write_bytes(b"p8-output-oracle-private-salt")
        salt_path.chmod(0o600)
        try:
            with psycopg.connect(source.dsn, autocommit=True) as con:
                _setup(con)
                catalog = _catalog_rows(con)
                reader = CatalogDescriptorReader(con)
                resolved = reader.resolve(oid for _name, oid in catalog)
                descriptors = {
                    name: resolved[oid]
                    for name, oid in catalog
                    if oid in resolved
                }
                query = _output_query()
                row = con.execute(query).fetchone()
                if row is None:
                    raise RuntimeError("oracle probe source row disappeared")

                outputs: dict[str, str] = {}
                repeated: dict[str, str] = {}
                for index, name in enumerate(COLUMNS):
                    outputs[name] = row[index * 2]
                    repeated[name] = row[index * 2 + 1]

                rules = [
                    {
                        "column_regex": rf"^app\.{TABLE}\.{name}$",
                        "action": "hash",
                        "algorithm": "HMAC-SHA-256",
                        "salt_id": "p8-output-v1",
                    }
                    for name in COLUMNS
                ]
                policy = PIIPolicy.from_manifest(
                    rules,
                    unmatched="exclude",
                    salt_file=salt_path,
                    epoch=7,
                )
                gate = PolicyGate(policy)
                output_texts = {
                    name: PostgreSQLOutputText(
                        outputs[name], descriptors[name].output_function_oid
                    )
                    for name in COLUMNS
                }
                sanitized = gate.sanitize_mapping(
                    f"app.{TABLE}",
                    outputs,
                    descriptors,
                    output_texts=output_texts,
                )

                findings["catalog_columns"] = len(descriptors) == len(COLUMNS)
                findings["output_function_identity"] = {
                    name: bool(
                        descriptors[name].output_function_oid
                        and descriptors[name].output_function_schema
                        and descriptors[name].output_function_name
                    )
                    for name in COLUMNS
                }
                findings["direct_output_is_text"] = {
                    name: isinstance(outputs[name], str) for name in COLUMNS
                }
                findings["repeated_output_matches"] = {
                    name: hashlib.sha256(outputs[name].encode()).digest()
                    == hashlib.sha256(repeated[name].encode()).digest()
                    for name in COLUMNS
                }
                findings["output_lengths"] = {
                    name: len(outputs[name]) for name in COLUMNS
                }
                findings["event_adapter_oid_proof"] = {
                    name: isinstance(output_texts[name], PostgreSQLOutputText)
                    and output_texts[name].output_function_oid
                    == descriptors[name].output_function_oid
                    for name in COLUMNS
                }
                findings["policy_accepts_every_family"] = (
                    set(sanitized) == set(COLUMNS)
                    and all(isinstance(value, str) and len(value) == 64 for value in sanitized.values())
                )
                findings["transformed_source_plaintext_absent"] = not any(
                    "P8_TOAST_SENTINEL" in str(value) for value in sanitized.values()
                )
                findings["money_native_varchar"] = native_type(
                    descriptors["money_value"]
                ).kind == "VARCHAR"
                findings["xml_native_varchar"] = native_type(
                    descriptors["xml_value"]
                ).kind == "VARCHAR"
                findings["projection_uses_output_not_text_cast"] = (
                    "format(chr(37) || 's'" in query
                    and "::text" not in query
                )
                findings["source_families"] = list(COLUMNS)
        finally:
            with psycopg.connect(source.dsn, autocommit=True) as con:
                con.execute(f"DROP TABLE IF EXISTS app.{TABLE} CASCADE")
                con.execute(f"DROP TYPE IF EXISTS app.{COMPOSITE} CASCADE")

    # This serialization contains no source output or salt; it is safe to emit as
    # the probe's machine-readable evidence.
    print(json.dumps(findings, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
