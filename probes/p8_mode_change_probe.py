"""Runtime proof for delete-policy boundaries and crash replay.

The first half drives the shipped ``Applier`` directly so the request is made
while a PostgreSQL transaction is open.  The second half runs the real keyed and
keyless crash matrix in a single worker, covering both hard/soft modes and both
sides of the destination commit.  Output is value-free.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR / "tests"))
sys.path.insert(0, str(PROJECT_DIR / "src"))

from support.applier_lab import Lab, begin, data, end  # noqa: E402

from cdc_flight.delete_modes import DeleteModeResolver  # noqa: E402


def _delete(txn: str, order: int, lsn: int, table: str, ident: int):
    return data(
        txn,
        order,
        lsn,
        table=table,
        op="d",
        key={"id": ident},
        before={"id": ident, "name": f"row-{ident}"},
    )


def _boundary_probe(path: Path) -> dict[str, object]:
    box = Lab(path, delete_mode="hard")
    try:
        box.run(
            [
                data("seed", 1, 10, table="mode_probe", key={"id": 1}, after={"id": 1, "name": "row-1"}),
                data("seed", 2, 11, table="mode_probe", key={"id": 2}, after={"id": 2, "name": "row-2"}),
                data("seed", 3, 12, table="mode_probe", key={"id": 3}, after={"id": 3, "name": "row-3"}),
                end("seed", 3, 20, {"app.mode_probe": 3}),
            ]
        )
        box.feed([begin("hard-delete", 2), _delete("hard-delete", 1, 30, "mode_probe", 1)])
        box.applier.request_delete_policy(DeleteModeResolver(global_mode="soft", epoch=2))
        box.feed([end("hard-delete", 1, 40, {"app.mode_probe": 1})])
        box.feed([begin("soft-delete", 2), _delete("soft-delete", 1, 50, "mode_probe", 2)])
        box.feed([end("soft-delete", 1, 60, {"app.mode_probe": 1})])
        box.commit()
        target = box.target("mode_probe")
        first_state = box.q(
            f'SELECT id, cdcf_deleted FROM "cdc_raw"."{target}" ORDER BY id'
        )
        first_ledger = box.q(
            "SELECT event_id, delete_mode, policy_epoch FROM _cdc_flight.delete_ledger "
            "WHERE target_table = ? ORDER BY event_id",
            [target],
        )
        live_count_before_transition = box.scalar(
            f'SELECT count(*) FROM "cdc_raw"."{target}__live"'
        )
        box.applier.request_delete_policy(DeleteModeResolver(global_mode="hard", epoch=3))
        box.run(
            [
                _delete("hard-again", 1, 70, "mode_probe", 3),
                end("hard-again", 1, 80, {"app.mode_probe": 1}),
            ]
        )
        final_state = box.q(
            f'SELECT id, cdcf_deleted FROM "cdc_raw"."{target}" ORDER BY id'
        )
        final_ledger = box.q(
            "SELECT event_id, delete_mode, policy_epoch FROM _cdc_flight.delete_ledger "
            "WHERE target_table = ? ORDER BY event_id",
            [target],
        )
        return {
            "open_transaction_request_fenced": first_state == [(2, True), (3, False)],
            "soft_live_view_hides_marked_row": live_count_before_transition == 1,
            "hard_transition_does_not_resurrect": final_state == [],
            "durable_mode_epochs": first_ledger == [
                ("30:hard-delete:1", "hard", 1),
                ("50:soft-delete:1", "soft", 2),
            ]
            and final_ledger == [
                ("30:hard-delete:1", "hard", 1),
                ("50:soft-delete:1", "soft", 2),
                ("70:hard-again:1", "hard", 3),
            ],
        }
    finally:
        box.close()


def _crash_probe() -> dict[str, object]:
    env = {**os.environ, "CDC_TEST_PGPORT": "15432", "PGPORT": "15432"}
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-p",
        "no:xdist",
        "tests/rubric/8.1_delete_modes/test_8_1_delete_crash_e2e.py",
        "-m",
        "slow and not motherduck",
    ]
    completed = subprocess.run(
        command,
        cwd=PROJECT_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=1200,
    )
    output = completed.stdout + completed.stderr
    passed_match = re.search(r"(\d+) passed", output)
    passed = int(passed_match.group(1)) if passed_match else 0
    return {
        "worker_count": 1,
        "returncode": completed.returncode,
        "passed": passed,
        "all_four_cases_green": completed.returncode == 0 and passed == 4,
    }


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="p8-mode-change-") as temp:
        boundary = _boundary_probe(Path(temp) / "mode.duckdb")
    crash = _crash_probe()
    findings = {
        "probe": "p8_mode_change",
        "boundary": boundary,
        "crash_matrix": crash,
        "overall": all(boundary.values()) and bool(crash["all_four_cases_green"]),
    }
    print(json.dumps(findings, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
