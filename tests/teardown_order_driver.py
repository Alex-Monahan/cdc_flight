"""A subprocess that tears a run down in EXACT production order, and must exit.

Rubric 1.9's heartbeat retirement was found twice, at two levels (Codex r5 MAJOR-1, then
r6 MAJOR-1), and both times the in-process test passed while production hung — because
the test stopped at the abstraction boundary it was written for. The round-5 test bounded
`finish()` and production then called `close()`; the round-6 test bounded `close()` and
production then called `con.close()` on the parent handle. A bound on a child resource is
not a bound on the process that closes its parent.

So this driver is not a unit of the teardown. It is the WHOLE of `pipeline.run()`'s
`finally` block, in order, in a real process, against a real serialized DuckDB sink that
never answers:

    phases.finish(ok=...)  ->  phases.close()  ->  release_connection(con)
                           ->  the summary is written  ->  os._exit(code)

and the test's assertion is the one thing neither previous test could make: **the process
exits**, inside the bound, with the code the run earned, having written a summary that
says what happened to both handles.

    python tests/teardown_order_driver.py <duckdb> <summary.json> ok|fail

Exits 0 for `ok`, 1 for `fail`. Anything else — including not exiting at all — is the
defect.
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
import threading
import time
from pathlib import Path

import duckdb

PIPELINE = "teardown_order"
RUNNER = "runner-1"
#: Long enough to outlive the whole bounded teardown on any machine that can run it.
HOG_SQL = "SELECT max(md5(i::VARCHAR)) FROM range(400000000) t(i)"


def main(argv: list[str]) -> int:
    duckdb_path, summary_path, outcome = argv[1:4]
    ok = outcome == "ok"

    from cdc_flight import destination as dest_mod
    from cdc_flight.machines import PHASE_RECONCILING, PHASE_STOPPING
    from cdc_flight.run_state import RunPhaseWriter

    con = duckdb.connect(duckdb_path)
    con.execute("PRAGMA threads=1")
    dest_mod.ensure_control_schema(con)
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    phases.to(PHASE_RECONCILING)

    # Occupy THE SINK — the same cursor the terminal write has to use, and a child of
    # the same connection the teardown then has to release.
    occupied = threading.Event()

    def _hog() -> None:
        occupied.set()
        with contextlib.suppress(Exception):
            phases._sink.execute(HOG_SQL)

    threading.Thread(target=_hog, daemon=True).start()
    occupied.wait(10)
    time.sleep(1.0)

    started = time.monotonic()
    summary: dict = {"outcome": outcome}
    # ---- pipeline.run()'s finally block, in order ------------------------- #
    with contextlib.suppress(Exception):
        phases.ensure(PHASE_STOPPING)
    with contextlib.suppress(Exception):
        phases.finish(ok=ok)
    with contextlib.suppress(Exception):
        phases.close()
    summary.update(phases.summary())
    summary["destination_connection_release"] = dest_mod.release_connection(con)
    summary["teardown_seconds"] = round(time.monotonic() - started, 3)
    # ---- ...and `main()` persists it and exits ---------------------------- #
    Path(summary_path).write_text(json.dumps(summary, default=str, indent=2))
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0 if ok else 1)


if __name__ == "__main__":  # pragma: no cover - it never returns
    sys.exit(main(sys.argv))
