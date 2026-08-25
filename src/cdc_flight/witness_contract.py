"""The enumerable contract for the single-process delivery witness.

There are two consumers of the witness:

* :class:`SourceHealth` decides whether the source evidence is good enough to
  describe the admitted Flight as connected; and
* :class:`ServiceContext` decides whether that evidence is good enough to renew
  the destination lease.

Keeping the inputs in one registry matters more than the individual predicates.
The registry is the production fold and the mutation-test collection at the same
time.  Every entry must provide a production guard and a negative test case.  A
new guard therefore cannot be added to one side of the contract without failing
module import/collection on the other side.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Literal

STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME = "Debezium Streaming"


class WitnessInput(StrEnum):
    """Every independent fact allowed to make the witness healthy."""

    SAMPLE_PRESENT = "sample_present"
    SLOT_EXISTS = "slot_exists"
    SLOT_ACTIVE = "slot_active"
    WALSENDER_IDENTITY = "walsender_identity"
    SAMPLER_OBSERVATION_FRESHNESS = "sampler_observation_freshness"
    ENGINE_THREAD_ALIVE = "engine_thread_alive"
    STREAM_RECOVERY = "stream_recovery"
    DURABLE_LSN = "durable_lsn"
    OWN_PROGRESS_PRESENT = "own_progress_present"
    OWN_ACK_TIMESTAMP_PRESENT = "own_ack_timestamp_present"
    OWN_ACK_LSN_PRESENT = "own_ack_lsn_present"
    CONFIRMED_NOT_AHEAD_DURABLE = "confirmed_not_ahead_durable"
    OWN_ACK_NOT_AHEAD_DURABLE = "own_ack_not_ahead_durable"
    OWN_PROGRESS_FRESHNESS = "own_progress_freshness"
    OWN_ACK_FRESHNESS = "own_ack_freshness"
    RECEIVED_HIGH_WATER = "received_high_water"
    RENEWAL_STATUS = "renewal_status"
    RENEWAL_ENGINE_THREAD_ALIVE = "renewal_engine_thread_alive"
    RENEWAL_OWN_PROGRESS_PRESENT = "renewal_own_progress_present"
    RENEWAL_OWN_ACK_TIMESTAMP_PRESENT = "renewal_own_ack_timestamp_present"
    RENEWAL_OWN_ACK_LSN_PRESENT = "renewal_own_ack_lsn_present"
    RENEWAL_SOURCE_OBSERVATION_FRESHNESS = "renewal_source_observation_freshness"
    RENEWAL_OWN_PROGRESS_FRESHNESS = "renewal_own_progress_freshness"
    RENEWAL_OWN_ACK_FRESHNESS = "renewal_own_ack_freshness"


@dataclass(frozen=True)
class ServiceWitnessEvidence:
    """The pure inputs to the source-side service witness fold."""

    now: float
    sample_present: bool
    sample_error: bool
    sample_age: float
    sample_stale_after: float
    slot_exists: bool
    slot_active: bool
    walsender_identity: bool
    engine_thread_alive: bool
    stream_recovery_pending: bool
    retained_lag_bytes: int | None
    own_progress_at: float | None
    own_ack_at: float | None
    own_ack_lsn: int | None
    durable_lsn: int | None
    confirmed_pos: int | None
    received_high_water: int | None
    progress_stale_after: float


@dataclass(frozen=True)
class RenewalWitnessEvidence:
    """The pure inputs to the destination-lease renewal fold."""

    now: float
    source_status: str | None
    source_observed_at: float | None
    engine_thread_alive: bool | None
    own_progress_at: float | None
    own_ack_at: float | None
    own_ack_lsn: int | None
    stale_after: float


ServiceGuard = Callable[[ServiceWitnessEvidence], bool]
RenewalGuard = Callable[[RenewalWitnessEvidence], bool]
FailureStatus = Callable[[ServiceWitnessEvidence], str]
NegativeCase = Callable[[object], object]


@dataclass(frozen=True)
class WitnessInputSpec:
    """One production guard plus its mandatory mutation negative cell.

    ``negative_case`` is deliberately required rather than being an optional test
    annotation.  The test collection iterates this exact tuple, while the
    production folds iterate the same tuple for their guards.  Adding a new enum
    member without a registry entry raises at import time; adding a registry entry
    without a negative case is a constructor error.
    """

    key: WitnessInput
    layer: Literal["service", "renewal"]
    negative_case: NegativeCase
    expected: str | bool
    service_guard: ServiceGuard | None = None
    service_failure: FailureStatus | None = None
    renewal_guard: RenewalGuard | None = None
    #: Most negative cells falsify the named boolean guard directly.  A derived
    #: output input (currently the received high-water calculation) instead
    #: keeps its guard true and proves the resulting classification.
    negative_guard_must_fail: bool = True


def _stalled_or_unproven(evidence: ServiceWitnessEvidence) -> str:
    return "stalled" if (evidence.retained_lag_bytes or 0) > 0 else "unproven"


def _service_guard_sample_present(e: ServiceWitnessEvidence) -> bool:
    return e.sample_present


def _service_guard_slot_exists(e: ServiceWitnessEvidence) -> bool:
    return e.slot_exists


def _service_guard_slot_active(e: ServiceWitnessEvidence) -> bool:
    return e.slot_active


def _service_guard_identity(e: ServiceWitnessEvidence) -> bool:
    return e.walsender_identity


def _service_guard_sample_fresh(e: ServiceWitnessEvidence) -> bool:
    return not e.sample_error and e.sample_age <= max(e.sample_stale_after, 0.0)


def _service_guard_engine_alive(e: ServiceWitnessEvidence) -> bool:
    return e.engine_thread_alive


def _service_guard_recovery(e: ServiceWitnessEvidence) -> bool:
    return not e.stream_recovery_pending or (e.retained_lag_bytes or 0) <= 0


def _service_guard_durable(e: ServiceWitnessEvidence) -> bool:
    return e.durable_lsn is not None


def _service_guard_progress_present(e: ServiceWitnessEvidence) -> bool:
    return e.own_progress_at is not None


def _service_guard_ack_timestamp(e: ServiceWitnessEvidence) -> bool:
    return e.own_ack_at is not None


def _service_guard_ack_lsn(e: ServiceWitnessEvidence) -> bool:
    return e.own_ack_lsn is not None


def _service_guard_confirmed_not_ahead(e: ServiceWitnessEvidence) -> bool:
    return (
        e.confirmed_pos is None
        or e.durable_lsn is None
        or e.confirmed_pos <= e.durable_lsn
    )


def _service_guard_ack_not_ahead(e: ServiceWitnessEvidence) -> bool:
    return (
        e.own_ack_lsn is None
        or e.durable_lsn is None
        or e.own_ack_lsn <= e.durable_lsn
    )


def _service_guard_progress_fresh(e: ServiceWitnessEvidence) -> bool:
    return (
        e.own_progress_at is not None
        and e.now - e.own_progress_at <= max(e.progress_stale_after, 0.0)
    )


def _service_guard_ack_fresh(e: ServiceWitnessEvidence) -> bool:
    return (
        e.own_ack_at is not None
        and e.now - e.own_ack_at <= max(e.progress_stale_after, 0.0)
    )


def _service_guard_received_high_water(e: ServiceWitnessEvidence) -> bool:
    return e.received_high_water is not None and e.confirmed_pos is not None


def _renewal_guard_status(e: RenewalWitnessEvidence) -> bool:
    return e.source_status in {"connected_quiet", "connected_busy"}


def _renewal_guard_engine_alive(e: RenewalWitnessEvidence) -> bool:
    return e.engine_thread_alive is True


def _renewal_guard_progress_present(e: RenewalWitnessEvidence) -> bool:
    return e.own_progress_at is not None


def _renewal_guard_ack_timestamp(e: RenewalWitnessEvidence) -> bool:
    return e.own_ack_at is not None


def _renewal_guard_ack_lsn(e: RenewalWitnessEvidence) -> bool:
    return e.own_ack_lsn is not None


def _renewal_guard_source_observation_fresh(e: RenewalWitnessEvidence) -> bool:
    return (
        e.source_observed_at is not None
        and e.now - e.source_observed_at <= max(e.stale_after, 0.0)
    )


def _renewal_guard_progress_fresh(e: RenewalWitnessEvidence) -> bool:
    return (
        e.own_progress_at is not None
        and e.now - e.own_progress_at <= max(e.stale_after, 0.0)
    )


def _renewal_guard_ack_fresh(e: RenewalWitnessEvidence) -> bool:
    return (
        e.own_ack_at is not None
        and e.now - e.own_ack_at <= max(e.stale_after, 0.0)
    )


def _service_negative(mutator: Callable[[ServiceWitnessEvidence], ServiceWitnessEvidence]):
    return mutator


def _renewal_negative(mutator: Callable[[RenewalWitnessEvidence], RenewalWitnessEvidence]):
    return mutator


_SERVICE_CASE = "service"
_RENEWAL_CASE = "renewal"


WITNESS_INPUTS: tuple[WitnessInputSpec, ...] = (
    WitnessInputSpec(
        WitnessInput.SAMPLE_PRESENT,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, sample_present=False)),
        "unobserved",
        service_guard=_service_guard_sample_present,
        service_failure=lambda _e: "unobserved",
    ),
    WitnessInputSpec(
        WitnessInput.SLOT_EXISTS,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, slot_exists=False)),
        "disconnected",
        service_guard=_service_guard_slot_exists,
        service_failure=lambda _e: "disconnected",
    ),
    WitnessInputSpec(
        WitnessInput.SLOT_ACTIVE,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, slot_active=False)),
        "disconnected",
        service_guard=_service_guard_slot_active,
        service_failure=lambda _e: "disconnected",
    ),
    WitnessInputSpec(
        WitnessInput.SAMPLER_OBSERVATION_FRESHNESS,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, sample_age=2.0)),
        "stale",
        service_guard=_service_guard_sample_fresh,
        service_failure=lambda e: "unknown" if e.sample_error else "stale",
    ),
    WitnessInputSpec(
        WitnessInput.ENGINE_THREAD_ALIVE,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, engine_thread_alive=False)),
        "engine_thread_dead",
        service_guard=_service_guard_engine_alive,
        service_failure=lambda _e: "engine_thread_dead",
    ),
    WitnessInputSpec(
        WitnessInput.STREAM_RECOVERY,
        _SERVICE_CASE,
        _service_negative(
            lambda e: replace(e, stream_recovery_pending=True, retained_lag_bytes=1)
        ),
        "stalled",
        service_guard=_service_guard_recovery,
        service_failure=_stalled_or_unproven,
    ),
    WitnessInputSpec(
        WitnessInput.DURABLE_LSN,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, durable_lsn=None)),
        "unproven",
        service_guard=_service_guard_durable,
        service_failure=_stalled_or_unproven,
    ),
    WitnessInputSpec(
        WitnessInput.OWN_PROGRESS_PRESENT,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, own_progress_at=None)),
        "unproven",
        service_guard=_service_guard_progress_present,
        service_failure=_stalled_or_unproven,
    ),
    WitnessInputSpec(
        WitnessInput.OWN_ACK_TIMESTAMP_PRESENT,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, own_ack_at=None)),
        "unproven",
        service_guard=_service_guard_ack_timestamp,
        service_failure=_stalled_or_unproven,
    ),
    WitnessInputSpec(
        WitnessInput.OWN_ACK_LSN_PRESENT,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, own_ack_lsn=None)),
        "unproven",
        service_guard=_service_guard_ack_lsn,
        service_failure=_stalled_or_unproven,
    ),
    WitnessInputSpec(
        WitnessInput.CONFIRMED_NOT_AHEAD_DURABLE,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, confirmed_pos=101)),
        "stalled",
        service_guard=_service_guard_confirmed_not_ahead,
        service_failure=lambda _e: "stalled",
    ),
    WitnessInputSpec(
        WitnessInput.OWN_ACK_NOT_AHEAD_DURABLE,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, own_ack_lsn=101)),
        "stalled",
        service_guard=_service_guard_ack_not_ahead,
        service_failure=lambda _e: "stalled",
    ),
    WitnessInputSpec(
        WitnessInput.OWN_PROGRESS_FRESHNESS,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, own_progress_at=e.now - 30.0)),
        "unproven",
        service_guard=_service_guard_progress_fresh,
        service_failure=_stalled_or_unproven,
    ),
    WitnessInputSpec(
        WitnessInput.OWN_ACK_FRESHNESS,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, own_ack_at=e.now - 30.0)),
        "unproven",
        service_guard=_service_guard_ack_fresh,
        service_failure=_stalled_or_unproven,
    ),
    WitnessInputSpec(
        WitnessInput.WALSENDER_IDENTITY,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, walsender_identity=False)),
        "foreign_walsender",
        service_guard=_service_guard_identity,
        service_failure=lambda _e: "foreign_walsender",
    ),
    WitnessInputSpec(
        WitnessInput.RECEIVED_HIGH_WATER,
        _SERVICE_CASE,
        _service_negative(lambda e: replace(e, received_high_water=200)),
        "connected_busy",
        service_guard=_service_guard_received_high_water,
        service_failure=lambda _e: "unproven",
        negative_guard_must_fail=False,
    ),
    WitnessInputSpec(
        WitnessInput.RENEWAL_STATUS,
        _RENEWAL_CASE,
        _renewal_negative(lambda e: replace(e, source_status="stalled")),
        False,
        renewal_guard=_renewal_guard_status,
    ),
    WitnessInputSpec(
        WitnessInput.RENEWAL_ENGINE_THREAD_ALIVE,
        _RENEWAL_CASE,
        _renewal_negative(lambda e: replace(e, engine_thread_alive=False)),
        False,
        renewal_guard=_renewal_guard_engine_alive,
    ),
    WitnessInputSpec(
        WitnessInput.RENEWAL_OWN_PROGRESS_PRESENT,
        _RENEWAL_CASE,
        _renewal_negative(lambda e: replace(e, own_progress_at=None)),
        False,
        renewal_guard=_renewal_guard_progress_present,
    ),
    WitnessInputSpec(
        WitnessInput.RENEWAL_OWN_ACK_TIMESTAMP_PRESENT,
        _RENEWAL_CASE,
        _renewal_negative(lambda e: replace(e, own_ack_at=None)),
        False,
        renewal_guard=_renewal_guard_ack_timestamp,
    ),
    WitnessInputSpec(
        WitnessInput.RENEWAL_OWN_ACK_LSN_PRESENT,
        _RENEWAL_CASE,
        _renewal_negative(lambda e: replace(e, own_ack_lsn=None)),
        False,
        renewal_guard=_renewal_guard_ack_lsn,
    ),
    WitnessInputSpec(
        WitnessInput.RENEWAL_SOURCE_OBSERVATION_FRESHNESS,
        _RENEWAL_CASE,
        _renewal_negative(lambda e: replace(e, source_observed_at=e.now - 30.0)),
        False,
        renewal_guard=_renewal_guard_source_observation_fresh,
    ),
    WitnessInputSpec(
        WitnessInput.RENEWAL_OWN_PROGRESS_FRESHNESS,
        _RENEWAL_CASE,
        _renewal_negative(lambda e: replace(e, own_progress_at=e.now - 30.0)),
        False,
        renewal_guard=_renewal_guard_progress_fresh,
    ),
    WitnessInputSpec(
        WitnessInput.RENEWAL_OWN_ACK_FRESHNESS,
        _RENEWAL_CASE,
        _renewal_negative(lambda e: replace(e, own_ack_at=e.now - 30.0)),
        False,
        renewal_guard=_renewal_guard_ack_fresh,
    ),
)


def _validate_registry() -> None:
    keys = [spec.key for spec in WITNESS_INPUTS]
    if len(keys) != len(set(keys)):
        raise RuntimeError("the delivery witness registry contains duplicate inputs")
    missing = set(WitnessInput) - set(keys)
    extra = set(keys) - set(WitnessInput)
    if missing or extra:
        raise RuntimeError(
            "the delivery witness registry and enum disagree: "
            f"missing={sorted(item.value for item in missing)} "
            f"extra={sorted(item.value for item in extra)}"
        )
    for spec in WITNESS_INPUTS:
        if spec.layer == "service":
            if spec.service_guard is None or spec.service_failure is None:
                raise RuntimeError(f"service witness input lacks a production guard: {spec.key}")
        elif spec.renewal_guard is None:
            raise RuntimeError(f"renewal witness input lacks a production guard: {spec.key}")
        if not callable(spec.negative_case):
            raise RuntimeError(f"witness input lacks a negative cell: {spec.key}")


_validate_registry()


def evaluate_service_witness(evidence: ServiceWitnessEvidence) -> str:
    """Fold service evidence through every registered service input."""
    for spec in WITNESS_INPUTS:
        if spec.layer != "service":
            continue
        assert spec.service_guard is not None
        if not spec.service_guard(evidence):
            assert spec.service_failure is not None
            return spec.service_failure(evidence)
    outstanding = max(
        0,
        int(evidence.received_high_water) - int(evidence.confirmed_pos),
    )
    return "connected_quiet" if outstanding == 0 else "connected_busy"


def renewal_witness_allows(evidence: RenewalWitnessEvidence) -> bool:
    """Fold renewal evidence through every registered renewal input."""
    for spec in WITNESS_INPUTS:
        if spec.layer != "renewal":
            continue
        assert spec.renewal_guard is not None
        if not spec.renewal_guard(evidence):
            return False
    return True


def canonical_service_evidence(*, now: float = 1_000.0) -> ServiceWitnessEvidence:
    """Stable healthy fixture used by the structural mutation collection."""
    return ServiceWitnessEvidence(
        now=now,
        sample_present=True,
        sample_error=False,
        sample_age=0.0,
        sample_stale_after=1.0,
        slot_exists=True,
        slot_active=True,
        walsender_identity=True,
        engine_thread_alive=True,
        stream_recovery_pending=False,
        retained_lag_bytes=0,
        own_progress_at=now,
        own_ack_at=now,
        own_ack_lsn=100,
        durable_lsn=100,
        confirmed_pos=100,
        received_high_water=100,
        progress_stale_after=15.0,
    )


def canonical_renewal_evidence(*, now: float = 1_000.0) -> RenewalWitnessEvidence:
    """Stable healthy renewal fixture used by the structural mutation collection."""
    return RenewalWitnessEvidence(
        now=now,
        source_status="connected_quiet",
        source_observed_at=now,
        engine_thread_alive=True,
        own_progress_at=now,
        own_ack_at=now,
        own_ack_lsn=100,
        stale_after=15.0,
    )
