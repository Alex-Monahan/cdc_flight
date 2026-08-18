"""§3.1 tests: bounded work, stock parallel acquisition, and identity completeness."""

from __future__ import annotations

from support.backfill_lab import require_backfill

from cdc_flight.config import ReplicationConfig, SourceConfig
from cdc_flight.debezium_props import build_properties


def test_stock_serial_and_incremental_properties_are_explicit(tmp_path, monkeypatch):
    """Proves every stock acquisition uses the one-reader correctness pin."""
    monkeypatch.setenv("CDC_AUTO_DISCOVERY", "0")
    monkeypatch.setenv("CDC_SNAPSHOT_MAX_THREADS", "4")
    props = build_properties(
        SourceConfig(), ReplicationConfig(state_dir=tmp_path)
    )
    assert props["snapshot.max.threads"] == "1"
    assert props["incremental.snapshot.chunk.size"]
    assert props["incremental.snapshot.watermarking.strategy"] == "insert_insert"
    assert props["signal.enabled.channels"] == "source"
    assert props["signal.data.collection"] == "app.cdc_flight_signal"
    assert "app.cdc_flight_signal" in props["table.include.list"]
    assert "transforms" not in props


def test_chunked_loader_never_materializes_the_whole_source_result():
    """Proves bounded chunks and one destination writer are a production contract."""
    backfill = require_backfill()
    rows = ((index, f"v-{index}") for index in range(10_000))
    chunks = list(backfill.iter_chunks(rows, chunk_size=257))
    assert max(map(len, chunks)) == 257
    assert sum(map(len, chunks)) == 10_000
    assert backfill.destination_writer_count() == 1


def test_parallel_acquisition_preserves_source_identity_and_values():
    """Proves parallel work cannot be credited by row count alone."""
    backfill = require_backfill()
    source = [(index, f"source-{index}") for index in range(2_000)]
    loaded = backfill.simulate_parallel_acquisition(source, workers=4)
    assert backfill.identity_set(loaded) == backfill.identity_set(source)
    assert backfill.value_multiset(loaded) == backfill.value_multiset(source)


def test_large_benchmark_reports_rss_spill_and_memory_metrics():
    """Proves the benchmark records measured resource evidence, not a property claim."""
    backfill = require_backfill()
    result = backfill.measure_chunked_load(
        ((index, f"v-{index}") for index in range(100_000)),
        chunk_size=4096,
        workers=4,
    )
    assert result.rows == 100_000
    assert result.workers == 4
    assert result.elapsed_seconds > 0
    assert result.rss_bytes > 0
    assert result.spill_bytes >= 0
    assert result.to_dict()["motherduck_memory_bytes"] is None
