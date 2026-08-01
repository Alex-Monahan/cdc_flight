"""Compositional ownership of the one destination connection runtime.

An applier is attached before anything can construct or start its consumer.  It becomes
active only at the callback boundary.  Teardown consults this token rather than a local
``applier`` variable, so a nested blocking re-snapshot cannot disappear from the outer
pipeline's ownership decision.
"""

from __future__ import annotations

from .machines import (
    DESTINATION_OWNERSHIP,
    OWNERSHIP_ACTIVE,
    OWNERSHIP_ATTACHED,
    OWNERSHIP_AVAILABLE,
    OWNERSHIP_CALLBACK_OWNED,
)


class DestinationOwnership:
    """Track the applier which may own the destination and its child cursors."""

    def __init__(self) -> None:
        self._applier = None
        self._state = OWNERSHIP_AVAILABLE

    def attach(self, applier) -> None:
        """Attach a constructed applier before consumer construction can fail."""
        if self._applier is not None or self._state != OWNERSHIP_AVAILABLE:
            raise RuntimeError("a destination applier is already attached")
        DESTINATION_OWNERSHIP.check(self._state, OWNERSHIP_ATTACHED)
        self._applier = applier
        self._state = OWNERSHIP_ATTACHED

    def activate(self, applier) -> None:
        """Publish that engine callbacks may now enter this applier."""
        self._assert_owner(applier)
        DESTINATION_OWNERSHIP.check(self._state, OWNERSHIP_ACTIVE)
        self._state = OWNERSHIP_ACTIVE

    def transfer_to_callback(self, applier) -> None:
        """Record the supervisor's failed-quiescence verdict as a terminal handoff.

        This transition is deliberately based on the supervisor's completed bounded
        proof, not on another read of ``callback_quiesced``. Once transferred, no
        enclosing finalizer may retire the runtime even if the callback leaves later.
        """
        self._assert_owner(applier)
        if self._state == OWNERSHIP_CALLBACK_OWNED:
            return
        DESTINATION_OWNERSHIP.check(self._state, OWNERSHIP_CALLBACK_OWNED)
        self._state = OWNERSHIP_CALLBACK_OWNED

    def owns(self, applier) -> bool:
        return self._applier is applier

    @property
    def state(self) -> str:
        return self._state

    @property
    def callback_owned(self) -> bool:
        return self._state == OWNERSHIP_CALLBACK_OWNED

    @property
    def destination_quiescent(self) -> bool:
        """Whether teardown may touch the parent connection and all its children.

        An inactive attached applier deliberately returns false until
        :meth:`retire_if_quiescent` seals it.  This prevents an unsealed construction
        failure from being mistaken for proof that no callback can enter.
        """
        if self._state == OWNERSHIP_AVAILABLE:
            return True
        if self._state == OWNERSHIP_CALLBACK_OWNED:
            return False
        return self._state == OWNERSHIP_ACTIVE and bool(
            self._applier.callback_quiesced
        )

    def retire_if_quiescent(self, *, reason: str) -> bool:
        """Seal an idle applier or retire a proved-quiescent active one.

        Returns false without touching any child handle when an active callback still
        owns the runtime.  Alert cursor retirement is part of the same ownership
        decision as parent retirement; it is never a caller-local ``finally`` action.
        """
        applier = self._applier
        if self._state == OWNERSHIP_AVAILABLE:
            return True
        if self._state == OWNERSHIP_CALLBACK_OWNED:
            return False
        if self._state == OWNERSHIP_ATTACHED:
            applier.shutdown(reason=reason)
        if not bool(applier.callback_quiesced):
            return False
        DESTINATION_OWNERSHIP.check(self._state, OWNERSHIP_AVAILABLE)
        applier.alerts.close()
        self._applier = None
        self._state = OWNERSHIP_AVAILABLE
        return True

    def _assert_owner(self, applier) -> None:
        if self._applier is not applier:
            raise RuntimeError("the applier does not own this destination runtime")
