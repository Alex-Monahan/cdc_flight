"""Real MotherDuck observer proof for the §3 shadow rename boundary."""

from __future__ import annotations

import threading
import time

import duckdb
import pytest

pytestmark = [pytest.mark.motherduck, pytest.mark.xdist_group("md_3_backfill")]


def test_separate_motherduck_reader_sees_old_or_new_complete_image(motherduck_case):
    """A real reader samples while DROP/RENAME/state update are in one transaction."""
    token = motherduck_case["token"]
    database = motherduck_case["database"]
    schema = motherduck_case["dataset"]
    dsn = f"md:{database}?motherduck_token={token}"
    writer = duckdb.connect(dsn)
    reader = duckdb.connect(dsn)
    observations: list[tuple[str, str]] = []
    stop = threading.Event()
    thread: threading.Thread | None = None

    def observe() -> None:
        while not stop.is_set():
            try:
                reader.execute("FORCE CHECKPOINT")
                # Observe the image and its lifecycle marker in one reader
                # snapshot. Separate autocommit statements can legitimately
                # straddle the writer's commit and manufacture an old-data/new-
                # state pair even when the writer committed both atomically.
                reader.execute("BEGIN TRANSACTION")
                try:
                    rows = reader.execute(
                        f'SELECT id, value FROM "{schema}".live ORDER BY id'
                    ).fetchall()
                    marker = reader.execute(
                        f'SELECT marker FROM "{schema}".state'
                    ).fetchone()[0]
                finally:
                    reader.execute("ROLLBACK")
                image = "old" if rows == [(1, "old"), (2, "old")] else (
                    "new" if rows == [(1, "new"), (2, "new"), (3, "new")] else "partial"
                )
                observations.append((image, str(marker)))
            except duckdb.Error:
                # The fixture is created before the observer starts; this only
                # tolerates a transient catalog refresh and is not a success path.
                pass
            stop.wait(0.05)

    try:
        writer.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        writer.execute(f'DROP TABLE IF EXISTS "{schema}".live')
        writer.execute(f'DROP TABLE IF EXISTS "{schema}".shadow')
        writer.execute(f'DROP TABLE IF EXISTS "{schema}".state')
        writer.execute(f'CREATE TABLE "{schema}".live (id INTEGER, value VARCHAR)')
        writer.execute(f'INSERT INTO "{schema}".live VALUES (1, \'old\'), (2, \'old\')')
        writer.execute(f'CREATE TABLE "{schema}".shadow (id INTEGER, value VARCHAR)')
        writer.execute(
            f'INSERT INTO "{schema}".shadow VALUES (1, \'new\'), (2, \'new\'), (3, \'new\')'
        )
        writer.execute(f'CREATE TABLE "{schema}".state (marker VARCHAR)')
        writer.execute(f'INSERT INTO "{schema}".state VALUES (\'old\')')
        writer.execute("CHECKPOINT")
        thread = threading.Thread(target=observe, daemon=True)
        thread.start()
        time.sleep(0.2)
        writer.execute("BEGIN TRANSACTION")
        writer.execute(f'DROP TABLE "{schema}".live')
        time.sleep(0.2)
        writer.execute(f'ALTER TABLE "{schema}".shadow RENAME TO live')
        time.sleep(0.2)
        writer.execute(f'UPDATE "{schema}".state SET marker = \'new\'')
        time.sleep(0.2)
        writer.execute("COMMIT")
        deadline = time.monotonic() + 10
        while ("new", "new") not in observations and time.monotonic() < deadline:
            time.sleep(0.05)
        stop.set()
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert ("old", "old") in observations
        assert ("new", "new") in observations
        assert set(observations) <= {("old", "old"), ("new", "new")}
    finally:
        stop.set()
        if thread is not None:
            thread.join(timeout=10)
        writer.close()
        reader.close()
