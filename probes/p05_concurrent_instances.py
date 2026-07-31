"""Probe: two Flight instances running at once.

Rubric item answered: 4.2 (handle or prevent concurrent Flight instances).

Case A: two processes sharing the same slot + offset file + DuckDB file
        (i.e. the same Flight scheduled twice / a stuck previous run).
Case B: two processes with different slots writing the same destination.
"""

from __future__ import annotations

import subprocess
import time

from _common import PROJECT_DIR, Probe, executable, reseed, slot_info


def launch(env, max_seconds: float = 45):
    cmd = [
        executable("cdc-flight"),
        "--destination",
        "duckdb",
        "--max-seconds",
        str(max_seconds),
        "--idle-seconds",
        "20",
        "--min-records",
        "0",
    ]
    return subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, cwd=PROJECT_DIR
    )


def summarise(proc, label):
    out, err = proc.communicate(timeout=300)
    tail = (err or "")[-2500:]
    return {
        "label": label,
        "returncode": proc.returncode,
        "slot_conflict_logged": "replication slot" in (out + err).lower()
        and "active" in (out + err).lower(),
        "duckdb_lock_error": "Conflicting lock" in (out + err) or "lock on file" in (out + err),
        "stderr_tail": tail,
    }


def main() -> None:
    p = Probe("p05_concurrent_instances")
    reseed()
    p.findings["run0_snapshot"] = p.run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)

    # -- Case A: identical configuration, launched simultaneously ----------
    a1 = launch(p.env)
    time.sleep(1.0)
    a2 = launch(p.env)
    p.findings["caseA_first"] = summarise(a1, "same-slot-first")
    p.findings["caseA_second"] = summarise(a2, "same-slot-second")
    p.findings["caseA_slot"] = slot_info(p.slot)

    # -- Case B: different slots, same DuckDB destination file -------------
    envB = {**p.env, "CDC_SLOT_NAME": p.slot + "_b", "CDC_STATE_DIR": p.env["CDC_STATE_DIR"] + "_b"}
    envB["CDC_PIPELINES_DIR"] = envB["CDC_STATE_DIR"] + "/dlt_pipelines"
    b1 = launch(p.env)
    time.sleep(0.5)
    b2 = launch(envB)
    p.findings["caseB_first"] = summarise(b1, "other-slot-first")
    p.findings["caseB_second"] = summarise(b2, "other-slot-second")

    from _common import drop_slot

    drop_slot(p.slot + "_b")
    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
