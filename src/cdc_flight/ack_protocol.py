"""Acknowledgement of throwaway re-snapshot records.

These records have no destination commit.  They still use the same guarded
acknowledgement window as a normal commit, and remain pending until the replacement
snapshot group is durable.
"""

from __future__ import annotations

from . import self_heal
from .run_state import COMMIT_ACK


def acknowledge_discarded_records(applier) -> None:
    pending = list(applier._pending_discarded_records)
    if not pending:
        return
    offset_fingerprint = applier.verifier.before() if applier.verifier else None
    stage = ["discard_ack"]
    marked = 0
    with self_heal.commit_watchdog(
        applier.cfg.commit_timeout,
        applier.last_commit_id,
        stage=lambda: stage[0],
    ):
        COMMIT_ACK.enter()
        try:
            for record in pending:
                if record.raw is None:
                    continue
                applier._committer.markProcessed(record.raw)
                marked += 1
            applier._committer.markBatchFinished()
        finally:
            COMMIT_ACK.leave()
    del applier._pending_discarded_records[: len(pending)]
    if applier.verifier is not None and marked:
        applier._pending_verification = (offset_fingerprint, marked)
