"""Production gates for decisions made by two consistency-state owners.

The individual machines answer local questions.  This module owns the cross-owner
question: may the operation represented by this pair proceed, or must it hold/refuse
until both facts are safe?  The state-matrix suite calls these same gates, and the live
catalog watcher uses the publication-admission gate before handing a discovered relation
to the snapshot coordinator.  There is no test-side disposition table.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from . import machines
from .machines import (
    ADMISSION_ADMITTED,
    ADMISSION_ERROR,
    ADMISSION_EXTERNAL,
    ADMISSION_PENDING,
    ADMISSION_REFUSED,
    CHANGE_APPLIED,
    CHANGE_DEFERRED,
    CHANGE_DUE,
    CHANGE_MARKED,
    CHANGE_PENDING,
    CHANGE_REFUSED,
    CHANGE_SUPERSEDED,
    LIFECYCLE_ABSENT,
    LIFECYCLE_AWAITING,
    LIFECYCLE_COMPLETE,
    LIFECYCLE_IN_PROGRESS,
    LIFECYCLE_NONE,
    OWNERSHIP_ACTIVE,
    OWNERSHIP_AVAILABLE,
    OWNERSHIP_CALLBACK_OWNED,
    REFUSAL_ABSENT,
    REFUSAL_PENDING,
    REFUSAL_QUARANTINED,
    REFUSAL_RESOLVED,
    SCHEMA_EMPTY,
    SCHEMA_ERROR,
    SCHEMA_UNAVAILABLE,
    SCHEMA_VISIBLE,
    SNAPSHOT_CALLBACKS_COMPLETE,
    SNAPSHOT_NOT_REQUIRED,
    SNAPSHOT_STREAMING,
)

OWNER = "state-interaction-gates"

@dataclass(frozen=True)
class InteractionDecision:
    """The result of one production cross-machine gate."""

    kind: str
    reason: str


def discovery_admission_allowed(change, relation) -> bool:
    """Production catalog hook for the new-relation publication gate."""
    if relation is None:
        return False
    return evaluate(
        ("catalog_change", "publication_admission"),
        change.state,
        relation.admission_state,
        left_owner=change,
        right_owner=relation,
    ).kind == "exercised"


_CHANGE_READY = frozenset(
    {
        CHANGE_PENDING,
        CHANGE_MARKED,
        CHANGE_DUE,
        CHANGE_APPLIED,
        CHANGE_SUPERSEDED,
    }
)
_CHANGE_REFUSAL = frozenset({CHANGE_DEFERRED, CHANGE_REFUSED})
_ADMISSION_READY = frozenset({ADMISSION_ADMITTED, ADMISSION_EXTERNAL})
_LIVENESS_BLOCKED = frozenset({SCHEMA_EMPTY, SCHEMA_UNAVAILABLE, SCHEMA_ERROR})
_SNAPSHOT_READY = frozenset(
    {SNAPSHOT_CALLBACKS_COMPLETE, SNAPSHOT_NOT_REQUIRED, SNAPSHOT_STREAMING}
)


def evaluate(
    pair: tuple[str, str],
    left: str,
    right: str,
    *,
    left_owner=None,
    right_owner=None,
) -> InteractionDecision:
    """Run the declared production gate for both state values.

    A non-ready combination is a deliberate, explainable refusal.  It is not silently
    converted into a passing matrix cell: callers must preserve the returned refusal and
    its reason.  The optional owners let a gate invoke the real state-owner admission
    method after the pair policy accepts the values.
    """
    pair = tuple(pair)
    declared = machines.declared_machines()
    if pair not in machines.INTERACTING_MACHINE_PAIRS:
        raise ValueError(f"interaction pair is not production-declared: {pair}")
    declared[pair[0]].parse(left)
    declared[pair[1]].parse(right)
    policy = _POLICIES[pair]
    return policy(left, right, left_owner, right_owner)


def _allow(pair: tuple[str, str], left: str, right: str, reason: str) -> InteractionDecision:
    return InteractionDecision(
        "exercised", f"{pair[0]}={left} and {pair[1]}={right}: {reason}"
    )


def _refuse(pair: tuple[str, str], left: str, right: str, reason: str) -> InteractionDecision:
    return InteractionDecision(
        "refused", f"{pair[0]}={left}, {pair[1]}={right}: {reason}"
    )


def _catalog_publication(change, admission, _change_owner, _admission_owner):
    pair = ("catalog_change", "publication_admission")
    if change in _CHANGE_READY and admission in _ADMISSION_READY:
        return _allow(pair, change, admission, "a queued change has a streamable relation")
    return _refuse(
        pair,
        change,
        admission,
        "catalog work cannot be handed off until the publication admission is ready",
    )


def _catalog_liveness(change, liveness, _change_owner, _liveness_owner):
    pair = ("catalog_change", "catalog_schema_liveness")
    if change in _CHANGE_READY and liveness == SCHEMA_VISIBLE:
        return _allow(pair, change, liveness, "catalog work has positive schema visibility")
    return _refuse(
        pair,
        change,
        liveness,
        "a catalog change cannot act on an empty, unavailable, or errored schema",
    )


def _catalog_refusal(change, refusal, _change_owner, _refusal_owner):
    pair = ("catalog_change", "schema_refusal")
    if change in _CHANGE_READY and refusal in {REFUSAL_ABSENT, REFUSAL_RESOLVED}:
        return _allow(pair, change, refusal, "catalog work has no outstanding refusal")
    if change in _CHANGE_REFUSAL and refusal == REFUSAL_PENDING:
        return _allow(pair, change, refusal, "the refusal is durably awaiting remediation")
    return _refuse(
        pair,
        change,
        refusal,
        "catalog work and schema-remediation state do not describe one safe route",
    )


def _catalog_lifecycle(change, lifecycle, _change_owner, _lifecycle_owner):
    pair = ("catalog_change", "table_lifecycle")
    if change in _CHANGE_READY and lifecycle in {LIFECYCLE_COMPLETE, LIFECYCLE_AWAITING}:
        return _allow(pair, change, lifecycle, "catalog work has a complete or owed image")
    if change in _CHANGE_REFUSAL and lifecycle == LIFECYCLE_AWAITING:
        return _allow(pair, change, lifecycle, "refused catalog work remains owed")
    return _refuse(
        pair,
        change,
        lifecycle,
        "catalog work cannot advance a missing, open, or unowned image",
    )


def _publication_liveness(admission, liveness, _admission_owner, _liveness_owner):
    pair = ("publication_admission", "catalog_schema_liveness")
    if admission in _ADMISSION_READY and liveness == SCHEMA_VISIBLE:
        return _allow(pair, admission, liveness, "an admitted relation has visible catalog state")
    return _refuse(
        pair,
        admission,
        liveness,
        "publication admission cannot be acted on without positive schema visibility",
    )


def _publication_refusal(admission, refusal, _admission_owner, _refusal_owner):
    pair = ("publication_admission", "schema_refusal")
    if admission in _ADMISSION_READY and refusal in {REFUSAL_ABSENT, REFUSAL_RESOLVED}:
        return _allow(pair, admission, refusal, "an admitted relation has no open refusal")
    if (
        admission in {ADMISSION_ERROR, ADMISSION_REFUSED, ADMISSION_PENDING}
        and refusal == REFUSAL_PENDING
    ):
        return _allow(
            pair, admission, refusal, "the blocked admission has durable remediation"
        )
    return _refuse(
        pair,
        admission,
        refusal,
        "publication admission and schema remediation do not form a safe hand-off",
    )


def _publication_lifecycle(admission, lifecycle, _admission_owner, _lifecycle_owner):
    pair = ("publication_admission", "table_lifecycle")
    if admission in _ADMISSION_READY and lifecycle in {
        LIFECYCLE_ABSENT,
        LIFECYCLE_NONE,
        LIFECYCLE_AWAITING,
        LIFECYCLE_COMPLETE,
    }:
        return _allow(pair, admission, lifecycle, "the relation can be observed or queued safely")
    return _refuse(
        pair,
        admission,
        lifecycle,
        "an unadmitted relation or open image cannot enter discovery hand-off",
    )


def _liveness_refusal(liveness, refusal, _liveness_owner, _refusal_owner):
    pair = ("catalog_schema_liveness", "schema_refusal")
    if liveness == SCHEMA_VISIBLE and refusal in {REFUSAL_ABSENT, REFUSAL_RESOLVED}:
        return _allow(pair, liveness, refusal, "visible catalog state has no open refusal")
    if liveness in _LIVENESS_BLOCKED and refusal in {
        REFUSAL_PENDING,
        REFUSAL_QUARANTINED,
    }:
        return _allow(pair, liveness, refusal, "the blocked observation retains remediation")
    return _refuse(
        pair,
        liveness,
        refusal,
        "an observation error or open refusal cannot be treated as a healthy projection",
    )


def _liveness_lifecycle(liveness, lifecycle, _liveness_owner, _lifecycle_owner):
    pair = ("catalog_schema_liveness", "table_lifecycle")
    if liveness == SCHEMA_VISIBLE and lifecycle != LIFECYCLE_IN_PROGRESS:
        return _allow(pair, liveness, lifecycle, "positive catalog visibility may inspect this lifecycle")
    return _refuse(
        pair,
        liveness,
        lifecycle,
        "a schema observation cannot make an open image look settled",
    )


def _refusal_lifecycle(refusal, lifecycle, _refusal_owner, _lifecycle_owner):
    pair = ("schema_refusal", "table_lifecycle")
    if refusal in {REFUSAL_PENDING, REFUSAL_QUARANTINED} and lifecycle in {
        LIFECYCLE_AWAITING,
        LIFECYCLE_IN_PROGRESS,
    }:
        return _allow(pair, refusal, lifecycle, "remediation is durable while the image is owed or open")
    if refusal in {REFUSAL_ABSENT, REFUSAL_RESOLVED} and lifecycle in {
        LIFECYCLE_ABSENT,
        LIFECYCLE_NONE,
        LIFECYCLE_COMPLETE,
    }:
        return _allow(pair, refusal, lifecycle, "lifecycle and refusal are settled together")
    return _refuse(
        pair,
        refusal,
        lifecycle,
        "a refusal cannot be discharged by a non-terminal lifecycle state",
    )


def _snapshot_lifecycle(completion, lifecycle, completion_owner, _lifecycle_owner):
    pair = ("snapshot_completion", "table_lifecycle")
    if lifecycle == LIFECYCLE_ABSENT and completion in {SNAPSHOT_NOT_REQUIRED, SNAPSHOT_STREAMING}:
        return _check_streaming(
            pair, completion, lifecycle, completion_owner,
            "no destination lifecycle row requires a snapshot admission",
        )
    if lifecycle == LIFECYCLE_COMPLETE and completion in _SNAPSHOT_READY:
        return _check_streaming(
            pair, completion, lifecycle, completion_owner,
            "a complete image may admit the stream",
        )
    return _refuse(
        pair,
        completion,
        lifecycle,
        "snapshot callbacks must be terminal and the destination image must be settled",
    )


def _ownership_snapshot(ownership, completion, ownership_owner, completion_owner):
    pair = ("destination_ownership", "snapshot_completion")
    if ownership == OWNERSHIP_CALLBACK_OWNED:
        return _refuse(pair, ownership, completion, "a failed callback owns the destination")
    if ownership == OWNERSHIP_ACTIVE and completion in _SNAPSHOT_READY:
        return _check_streaming(
            pair, ownership, completion, completion_owner,
            "the active destination owns a terminal snapshot phase",
        )
    if ownership == OWNERSHIP_AVAILABLE and completion == SNAPSHOT_NOT_REQUIRED:
        return _allow(pair, ownership, completion, "streaming-only mode needs no callback owner")
    return _refuse(
        pair,
        ownership,
        completion,
        "the destination is not admitted to consume this snapshot phase",
    )


def _backfill_lifecycle(run, lifecycle, _run_owner, _lifecycle_owner):
    pair = ("backfill_run", "table_lifecycle")
    if run in {"requested", "preparing"} and lifecycle in {"none", "awaiting_snapshot", "in_progress"}:
        return _allow(pair, run, lifecycle, "the request can establish or reattach its shadow")
    if run in {"loading", "ready_to_swap", "swapping"} and lifecycle == "in_progress":
        return _allow(pair, run, lifecycle, "active loading owns a nonterminal image")
    if run == "complete" and lifecycle == "complete":
        return _allow(pair, run, lifecycle, "publication and lifecycle completed together")
    if run in {"retry_wait", "blocked"} and lifecycle in {"awaiting_snapshot", "in_progress", "complete"}:
        return _allow(pair, run, lifecycle, "retry/blocked work retains a visible recovery disposition")
    return _refuse(pair, run, lifecycle, "backfill and lifecycle do not describe one safe image")


def _backfill_claim(run, claim, _run_owner, _claim_owner):
    pair = ("backfill_run", "shadow_claim")
    if run in {"requested", "preparing", "retry_wait", "blocked"} and claim in {"free", "backfill"}:
        return _allow(pair, run, claim, "the claim is free or owned by the same backfill route")
    if run in {"loading", "ready_to_swap", "swapping"} and claim == "backfill":
        return _allow(pair, run, claim, "active publication retains the one table claim")
    if run == "complete" and claim == "free":
        return _allow(pair, run, claim, "completed publication released its claim")
    return _refuse(pair, run, claim, "a backfill cannot overwrite another shadow owner")


def _backfill_phase(run, phase, _run_owner, _phase_owner):
    pair = ("backfill_run", "run_phase")
    if phase in {"streaming", "recovering", "reconciling"} and run in {"requested", "preparing", "loading", "ready_to_swap", "retry_wait", "blocked"}:
        return _allow(pair, run, phase, "per-table work is durable while the run remains live")
    if phase == "snapshotting" and run in {"requested", "preparing", "loading", "retry_wait", "blocked"}:
        return _allow(pair, run, phase, "blocking/full fallback uses the existing snapshot hand-off")
    if phase in {"stopping", "stopped", "failed"} and run in {"retry_wait", "blocked", "complete"}:
        return _allow(pair, run, phase, "run teardown leaves retry/terminal disposition durable")
    return _refuse(pair, run, phase, "backfill mode is incompatible with the run phase")


def _backfill_completion(run, completion, _run_owner, _completion_owner):
    pair = ("backfill_run", "snapshot_completion")
    if completion in {SNAPSHOT_CALLBACKS_COMPLETE, SNAPSHOT_NOT_REQUIRED, SNAPSHOT_STREAMING} and run in {"requested", "preparing", "loading", "ready_to_swap", "swapping", "complete"}:
        return _allow(pair, run, completion, "initial completion is terminal before incremental admission")
    if run in {"retry_wait", "blocked"} and completion in {"awaiting_callbacks", "callbacks_started", "completion_notified"}:
        return _allow(pair, run, completion, "a blocked request retains callback evidence for retry")
    return _refuse(pair, run, completion, "initial callback completion does not admit this backfill state")


def _backfill_catalog(run, change, _run_owner, _change_owner):
    pair = ("backfill_run", "catalog_change")
    if change in {"pending", "marked", "due", "applied", "superseded"} and run in {"requested", "preparing", "loading", "ready_to_swap", "complete"}:
        return _allow(pair, run, change, "the catalog epoch is observed by the backfill gate")
    if change in {"deferred", "refused"} and run in {"retry_wait", "blocked"}:
        return _allow(pair, run, change, "catalog refusal queues the table run")
    return _refuse(pair, run, change, "an old or refused catalog epoch cannot be swapped")


def _backfill_ownership(run, ownership, _run_owner, _ownership_owner):
    pair = ("backfill_run", "destination_ownership")
    if ownership == OWNERSHIP_ACTIVE and run in {"preparing", "loading", "ready_to_swap", "swapping"}:
        return _allow(pair, run, ownership, "only the destination owner may signal and swap")
    if ownership == OWNERSHIP_AVAILABLE and run in {"requested", "retry_wait", "blocked"}:
        return _allow(pair, run, ownership, "unadmitted work waits for ownership")
    if ownership in {"stopping", "stopped"} and run in {"retry_wait", "complete"}:
        return _allow(pair, run, ownership, "ownership release leaves a durable retry/terminal state")
    return _refuse(pair, run, ownership, "a non-owner cannot mutate a backfill shadow")


def _claim_catalog(claim, change, _claim_owner, _change_owner):
    pair = ("shadow_claim", "catalog_change")
    if claim == "free" and change in _CHANGE_READY:
        return _allow(pair, claim, change, "catalog work may acquire a free shadow")
    if claim == "backfill" and change in {"pending", "marked", "deferred", "refused"}:
        return _refuse(pair, claim, change, "catalog mutation waits for the backfill claim")
    if claim in {"typed_change", "schema_rebuild"} and change in _CHANGE_READY:
        return _refuse(pair, claim, change, "the typed/schema owner excludes backfill mutation")
    return _allow(pair, claim, change, "the claim state records the owner hand-off")


def _claim_ownership(claim, ownership, _claim_owner, _ownership_owner):
    pair = ("shadow_claim", "destination_ownership")
    if ownership == OWNERSHIP_ACTIVE and claim in {"free", "backfill", "typed_change", "schema_rebuild"}:
        return _allow(pair, claim, ownership, "the active destination owner arbitrates claims")
    if ownership == OWNERSHIP_AVAILABLE and claim == "free":
        return _allow(pair, claim, ownership, "an unowned table has no active shadow mutation")
    return _refuse(pair, claim, ownership, "claim acquisition/release requires destination ownership")


def _check_streaming(pair, left, right, completion_owner, reason):
    if completion_owner is not None:
        try:
            completion_owner.check_streaming_admission()
        except Exception as exc:
            return _refuse(pair, left, right, f"the production snapshot gate refused: {exc}")
    return _allow(pair, left, right, reason)


_POLICIES: dict[tuple[str, str], Callable[..., InteractionDecision]] = {
    ("catalog_change", "publication_admission"): _catalog_publication,
    ("catalog_change", "catalog_schema_liveness"): _catalog_liveness,
    ("catalog_change", "schema_refusal"): _catalog_refusal,
    ("catalog_change", "table_lifecycle"): _catalog_lifecycle,
    ("publication_admission", "catalog_schema_liveness"): _publication_liveness,
    ("publication_admission", "schema_refusal"): _publication_refusal,
    ("publication_admission", "table_lifecycle"): _publication_lifecycle,
    ("catalog_schema_liveness", "schema_refusal"): _liveness_refusal,
    ("catalog_schema_liveness", "table_lifecycle"): _liveness_lifecycle,
    ("schema_refusal", "table_lifecycle"): _refusal_lifecycle,
    ("snapshot_completion", "table_lifecycle"): _snapshot_lifecycle,
    ("destination_ownership", "snapshot_completion"): _ownership_snapshot,
    ("backfill_run", "table_lifecycle"): _backfill_lifecycle,
    ("backfill_run", "shadow_claim"): _backfill_claim,
    ("backfill_run", "run_phase"): _backfill_phase,
    ("backfill_run", "snapshot_completion"): _backfill_completion,
    ("backfill_run", "catalog_change"): _backfill_catalog,
    ("backfill_run", "destination_ownership"): _backfill_ownership,
    ("shadow_claim", "catalog_change"): _claim_catalog,
    ("shadow_claim", "destination_ownership"): _claim_ownership,
}
