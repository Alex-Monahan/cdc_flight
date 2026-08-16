"""PostgreSQL equality classes used by the range identity probes."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg


@dataclass(frozen=True)
class RangeEqualityClass:
    name: str
    left_text: str
    right_text: str
    postgres_equal: bool


def postgres_range_equality_classes(dsn: str) -> tuple[RangeEqualityClass, ...]:
    """Read canonical ``range_out`` text and equality from the real source server."""

    query = """
        WITH values AS (
            SELECT
                tsrange(
                    '-infinity'::timestamp,
                    'infinity'::timestamp,
                    '[]'
                ) AS timestamp_special,
                tsrange(NULL::timestamp, NULL::timestamp, '()') AS timestamp_unbounded,
                '{[10,20),[2,10)}'::nummultirange AS numeric_reordered,
                '{[2,20)}'::nummultirange AS numeric_merged,
                '{[1,3),[2,4)}'::nummultirange AS numeric_overlap,
                '{[1,4)}'::nummultirange AS numeric_overlap_merged,
                '{[1,2),[2,3)}'::nummultirange AS numeric_touching,
                int4range(1, 3, '[]') AS discrete_closed,
                int4range(1, 4, '[)') AS discrete_canonical,
                int4range(1, 2, '()') AS discrete_empty,
                'empty'::int4range AS discrete_empty_literal
        )
        SELECT
            timestamp_special::text,
            timestamp_unbounded::text,
            timestamp_special = timestamp_special,
            timestamp_special = timestamp_unbounded,
            numeric_reordered::text,
            numeric_merged::text,
            numeric_reordered = numeric_merged,
            numeric_overlap::text,
            numeric_overlap_merged::text,
            numeric_overlap = numeric_overlap_merged,
            numeric_touching::text,
            numeric_touching = '{[1,2),[2,3)}'::nummultirange,
            numeric_touching = '{[1,3)}'::nummultirange,
            discrete_closed::text,
            discrete_canonical::text,
            discrete_closed = discrete_canonical,
            discrete_empty::text,
            discrete_empty_literal::text,
            discrete_empty = discrete_empty_literal
        FROM values
    """
    with psycopg.connect(dsn, autocommit=True) as connection:
        row = connection.execute(query).fetchone()
    assert row is not None

    def cls(name: str, left: int, right: int, equal: int) -> RangeEqualityClass:
        return RangeEqualityClass(
            name,
            str(row[left]),
            str(row[right]),
            bool(row[equal]),
        )

    return (
        cls("timestamp special endpoint", 0, 0, 2),
        cls("timestamp special versus unbounded", 0, 1, 3),
        cls("continuous multirange reordered and merged", 4, 5, 6),
        cls("continuous multirange overlapping merge", 7, 8, 9),
        cls("continuous multirange adjacent merge", 10, 10, 12),
        cls("discrete range closed versus canonical", 13, 14, 15),
        cls("discrete empty spelling", 16, 17, 18),
    )


__all__ = ["RangeEqualityClass", "postgres_range_equality_classes"]
