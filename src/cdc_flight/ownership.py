"""Compositional ownership of the one destination connection runtime.

An applier is attached before anything can construct or start its consumer.  It becomes
active only at the callback boundary.  Teardown consults this token rather than a local
``applier`` variable, so a nested blocking re-snapshot cannot disappear from the outer
pipeline's ownership decision.
"""

from __future__ import annotations


class DestinationOwnership:
    """Track the applier which may own the destination and its child cursors."""

    def __init__(self) -> None:
        self._applier = None
        self._active = False

    def attach(self, applier) -> None:
        """Attach a constructed applier before consumer construction can fail."""
        if self._applier is not None:
            raise RuntimeError("a destination applier is already attached")
        self._applier = applier
        self._active = False

    def activate(self, applier) -> None:
        """Publish that engine callbacks may now enter this applier."""
        self._assert_owner(applier)
        self._active = True

    def owns(self, applier) -> bool:
        return self._applier is applier

    @property
    def destination_quiescent(self) -> bool:
        """Whether teardown may touch the parent connection and all its children.

        An inactive attached applier deliberately returns false until
        :meth:`retire_if_quiescent` seals it.  This prevents an unsealed construction
        failure from being mistaken for proof that no callback can enter.
        """
        if self._applier is None:
            return True
        return self._active and bool(self._applier.callback_quiesced)

    @property
    def live_callback_owner(self) -> bool:
        return (
            self._applier is not None
            and self._active
            and not bool(self._applier.callback_quiesced)
        )

    def retire_if_quiescent(self, *, reason: str) -> bool:
        """Seal an idle applier or retire a proved-quiescent active one.

        Returns false without touching any child handle when an active callback still
        owns the runtime.  Alert cursor retirement is part of the same ownership
        decision as parent retirement; it is never a caller-local ``finally`` action.
        """
        applier = self._applier
        if applier is None:
            return True
        if not self._active:
            applier.shutdown(reason=reason)
        if not bool(applier.callback_quiesced):
            return False
        applier.alerts.close()
        self._applier = None
        self._active = False
        return True

    def _assert_owner(self, applier) -> None:
        if self._applier is not applier:
            raise RuntimeError("the applier does not own this destination runtime")
