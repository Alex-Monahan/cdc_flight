"""The at-least-ten-million-row bounded-load measurement for §3.1."""

from __future__ import annotations

import pytest
from support.backfill_lab import require_backfill

pytestmark = pytest.mark.slow


def test_ten_million_rows_stay_chunk_bounded_and_report_real_metrics():
    """Proves the large benchmark consumes ten million rows without one giant result set."""
    backfill = require_backfill()
    result = backfill.measure_chunked_load(
        ((index, f"source-{index}") for index in range(10_000_000)),
        chunk_size=10_000,
        workers=4,
    )
    print(f"P3 §3.1 bounded benchmark: {result.to_dict()}")
    assert result.rows == 10_000_000
    assert result.workers == 4
    assert result.elapsed_seconds > 0
    assert result.rss_bytes > 0
    assert result.spill_bytes == 0
    assert result.motherduck_memory_bytes is None
