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
            "to_tsquery('fat | rat')",
            "to_tsquery('!fat')",
            "to_tsquery('(fat & rat) | cat')",
            "to_tsquery('fat <-> rat')",
            "to_tsquery('fat <2> rat')",
            "to_tsquery('fat:* & rat:*')",
            "plainto_tsquery('simple', 'fat rat')",
            "phraseto_tsquery('simple', 'fat rat')",
            "websearch_to_tsquery('simple', 'fat or rat')",
            "to_tsquery('simple', 'blue:*')",
            "to_tsquery('english', 'fat & rat')",
        ],
    ),
    "jsonpath": (
        "jsonpath",
        [
            "'strict $.a'::jsonpath",
            "'1 + 2'::jsonpath",
            "'$.a + $.b'::jsonpath",
            "'-$.a'::jsonpath",
            "'lax $.a'::jsonpath",
            "'$.a[*]'::jsonpath",
            "'$.a[0 to 2]'::jsonpath",
            "'$.a.*'::jsonpath",
            "'$.**.a'::jsonpath",
            "'$ ? (@ == 1)'::jsonpath",
            "'$.a ? (@ > 1)'::jsonpath",
            "'$.a.type()'::jsonpath",
            "'$.a.size()'::jsonpath",
            "'$.a ? (@ like_regex \"foo\")'::jsonpath",
            "'$.a[0]'::jsonpath",
        ],
    ),
    "pg_lsn": (
        "pg_lsn",
        [
            "'0/16B6A0'::pg_lsn",
            "'0/16B6A1'::pg_lsn",
            "'1/0'::pg_lsn",
            "'F/F'::pg_lsn",
            "'0/0'::pg_lsn",
            "'0/FFFFFFFF'::pg_lsn",
            "'10/20'::pg_lsn",
            "'ABC/DEF'::pg_lsn",
        ],
    ),
    "tsvector": (
        "tsvector",
        [
            "to_tsvector('simple', 'fat rat')",
            "to_tsvector('simple', '')",
            "setweight(to_tsvector('simple', 'blue'), 'A')",
            "to_tsvector('english', 'the quick brown fox')",
            "to_tsvector('simple', 'one two three')",
            "to_tsvector('simple', 'Café naïve')",
            "to_tsvector('english', 'PostgreSQL database')",
            "to_tsvector('simple', 'foo-bar')",
            "setweight(to_tsvector('simple', 'title'), 'B')",
            "to_tsvector('english', 'the and or')",
        ],
    ),
    "xml": (
        "xml",
        [
            "xmlelement(name a, 'fat')",
            "xmlparse(document '<a/>')",
            "xmlparse(document '<?xml version=\"1.0\"?><prolog/>')",
            "xmlparse(document '<?xml version=\"1.0\" encoding=\"UTF-8\"?><unicode>é</unicode>')",
            "xmlcomment('round11')",
            "xmlelement(name root, xmlelement(name child, 'x'))",
            "xmlparse(document '<?xml version=\"1.0\" standalone=\"yes\"?><standalone/>')",
            "xmlparse(document '<?xml version=\"1.1\"?><v11/>')",
        ],
    ),
    "money": (
        "money",
        [
            "12.34::money",
            "(-0.01)::money",
            "0::money",
            "999999.99::money",
            "1234.56::money",
            "1000000.00::money",
        ],
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
            "'10.0.0.0/8'::cidr",
            "'172.16.0.0/12'::cidr",
            "'2001:db8:1::/48'::cidr",
            "'192.0.2.128/25'::cidr",
        ],
    ),
    "macaddr": (
        "macaddr",
        [
            "'08:00:2b:01:02:03'::macaddr",
            "'ff:ff:ff:ff:ff:ff'::macaddr",
            "'00:00:00:00:00:00'::macaddr",
            "'12:34:56:78:9a:bc'::macaddr",
            "'01:23:45:67:89:ab'::macaddr",
        ],
    ),
    "macaddr8": (
        "macaddr8",
        [
            "'08:00:2b:01:02:03:04:05'::macaddr8",
            "'ff:ff:ff:ff:ff:ff:ff:ff'::macaddr8",
            "'00:00:00:00:00:00:00:00'::macaddr8",
            "'12:34:56:78:9a:bc:de:f0'::macaddr8",
            "'01:23:45:67:89:ab:cd:ef'::macaddr8",
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
# The corpus is generated and compared against PostgreSQL's output function.  A
# default XML declaration is removed by ``xml_out`` itself (not by Debezium), so
# both the declaration-bearing source expression and the connector value compare
# to the same output-function bytes. ``int2vector`` remains the one deliberate
# value-shape refusal: stock Debezium exposes it as a Connect array rather than
# the PostgreSQL text payload represented by this corpus.
UNDELIVERABLE_TEXT_TYPES = frozenset()


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
    """Read the value spelling stock Debezium delivers for this corpus.

    Most opaque values are delivered by their PostgreSQL output function, so
    ``format('%s', value)`` is the independent oracle.  Stock Debezium's built-in
    PostgreSQL money converter delivers the numeric spelling rather than the
    locale-decorated ``money_out`` display; the dedicated Round-12 live probe
    cross-checks that wire spelling under four locales.
    """
    expression = "value::numeric::text" if name == "money" else "format('%s', value)"
    return sandbox.pg_query(
        f"SELECT id, {expression} FROM app.p2b_r9_{name} ORDER BY id"
    )
