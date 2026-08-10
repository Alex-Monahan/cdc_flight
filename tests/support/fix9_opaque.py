"""PostgreSQL-generated opaque corpus shared by the local and MotherDuck lanes."""

from __future__ import annotations

import psycopg

from cdc_flight.config import ReplicationConfig

CORPUS = {
    "tsquery": (
        "tsquery",
        [
            "to_tsquery('fat:*AB')",
            "to_tsquery('fat:B*')",
            "''::tsquery",
            "to_tsquery('fat & rat')",
        ],
    ),
    "jsonpath": (
        "jsonpath",
        [
            "'strict $.a'::jsonpath",
            "'1 + 2'::jsonpath",
            "'$.a + $.b'::jsonpath",
            "'-$.a'::jsonpath",
        ],
    ),
    "pg_lsn": (
        "pg_lsn",
        ["'0/16B6A0'::pg_lsn", "'0/16B6A1'::pg_lsn", "'1/0'::pg_lsn", "'F/F'::pg_lsn"],
    ),
    "tsvector": (
        "tsvector",
        [
            "to_tsvector('simple', 'fat rat')",
            "to_tsvector('simple', '')",
            "setweight(to_tsvector('simple', 'blue'), 'A')",
            "to_tsvector('english', 'the quick brown fox')",
        ],
    ),
    "xml": (
        "xml",
        [
            "xmlelement(name a, 'fat')",
            "xmlparse(document '<a/>')",
            "xmlparse(document '<?xml version=\"1.0\"?><prolog/>')",
            "xmlparse(document '<?xml version=\"1.0\" encoding=\"UTF-8\"?><unicode>é</unicode>')",
        ],
    ),
    "money": (
        "money",
        ["12.34::money", "(-0.01)::money", "0::money", "999999.99::money"],
    ),
    "inet": (
        "inet",
        [
            "'192.0.2.1'::inet",
            "'192.0.2.1/24'::inet",
            "'2001:db8::1'::inet",
            "'2001:db8::1/64'::inet",
            "'198.51.100.2/24'::inet",
            "'::1/64'::inet",
        ],
    ),
    "cidr": (
        "cidr",
        [
            "'192.0.2.0/24'::cidr",
            "'2001:db8::/32'::cidr",
            "'198.51.100.0/25'::cidr",
            "'::/0'::cidr",
        ],
    ),
    "macaddr": (
        "macaddr",
        [
            "'08:00:2b:01:02:03'::macaddr",
            "'ff:ff:ff:ff:ff:ff'::macaddr",
            "'00:00:00:00:00:00'::macaddr",
            "'12:34:56:78:9a:bc'::macaddr",
        ],
    ),
    "macaddr8": (
        "macaddr8",
        [
            "'08:00:2b:01:02:03:04:05'::macaddr8",
            "'ff:ff:ff:ff:ff:ff:ff:ff'::macaddr8",
            "'00:00:00:00:00:00:00:00'::macaddr8",
            "'12:34:56:78:9a:bc:de:f0'::macaddr8",
        ],
    ),
    "int2vector": (
        "int2vector",
        [
            "'1 2 3'::int2vector",
            "'0 -1 32767'::int2vector",
            "''::int2vector",
            "'4 5'::int2vector",
        ],
    ),
}

EXACT_CORPUS = dict(CORPUS)
# XML declarations are stripped by stock Debezium.  Since the value at the
# connector boundary cannot distinguish a source document that had a declaration
# from one that never had one, the safe contract is refuse-all XML.
UNDELIVERABLE_TEXT_TYPES = frozenset({"xml"})


def capture_environment(tables: list[str]) -> dict[str, str]:
    return {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": ",".join(tables),
        "CDC_INCLUDE_UNKNOWN_DATATYPES": "true",
    }


def create_corpus(sandbox, corpus: dict = CORPUS) -> list[str]:
    publication = ReplicationConfig().publication_name
    tables: list[str] = []
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        for name, (type_name, _expressions) in corpus.items():
            table = f"app.p2b_r9_{name}"
            tables.append(table.removeprefix("app."))
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            conn.execute(f"CREATE TABLE {table} (id integer PRIMARY KEY, value {type_name})")
            conn.execute(f"ALTER PUBLICATION {publication} ADD TABLE {table}")
    return tables


def populate_corpus(sandbox, corpus: dict = EXACT_CORPUS) -> None:
    statements: list[str] = []
    for name, (_type_name, expressions) in corpus.items():
        if name == "int2vector":
            continue
        values = ", ".join(
            f"({index}, {expression})" for index, expression in enumerate(expressions, 1)
        )
        statements.append(f"INSERT INTO app.p2b_r9_{name} (id, value) VALUES {values}")
    sandbox.sql(statements, one_transaction=True)
    if "int2vector" not in corpus:
        return
    _type_name, expressions = corpus["int2vector"]
    values = ", ".join(
        f"({index}, {expression})" for index, expression in enumerate(expressions, 1)
    )
    # PostgreSQL/stock Debezium exposes this system type as an empty Connect
    # array.  It is a separate source transaction so its refusal cannot roll
    # back the otherwise deliverable corpus.
    sandbox.sql(
        f"INSERT INTO app.p2b_r9_int2vector (id, value) VALUES {values}",
        one_transaction=True,
    )


def drop_corpus(sandbox, corpus: dict = CORPUS) -> None:
    publication = ReplicationConfig().publication_name
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        for name in corpus:
            table = f"app.p2b_r9_{name}"
            conn.execute(f"ALTER PUBLICATION {publication} DROP TABLE {table}")
            conn.execute(f"DROP TABLE IF EXISTS {table}")


def source_connector_text(sandbox, name: str) -> list[tuple]:
    """Read the source value through PostgreSQL's type output function.

    ``value::text`` is a cast, not the type output function. In particular,
    ``inet_out`` omits the redundant host mask for a host address. ``format``
    with ``%s`` invokes the same output-function rendering used by SELECT/COPY.
    """
    return sandbox.pg_query(
        f"SELECT id, format('%s', value) FROM app.p2b_r9_{name} ORDER BY id"
    )
