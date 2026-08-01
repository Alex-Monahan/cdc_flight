"""One bounded protocol for retiring destination handles at process teardown."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from .machines import CONNECTION_RETIREMENT

log = logging.getLogger("cdc_flight.retirement")


@dataclass(frozen=True)
class RetirementResult:
    """The declared retirement state and a close failure, when one occurred."""

    state: str
    error: str | None = None


def retire_handle(
    handle,
    *,
    timeout: float,
    thread_name: str,
    description: str,
) -> RetirementResult:
    """Run ``handle.close()`` on a daemon and wait no longer than ``timeout``.

    Completion is not synonymous with success: a close that raises is ``failed``, a
    close still running at the bound is ``abandoned``, and only a successful return is
    ``closed``. The daemon owns the handle after the caller stops waiting, so it cannot
    keep process exit open.
    """
    if handle is None:
        return RetirementResult(CONNECTION_RETIREMENT.parse("never_opened"))

    done = threading.Event()
    failures: list[str] = []

    def _close() -> None:
        try:
            handle.close()
        except Exception as exc:  # a broken handle is an outcome, not a clean close
            failures.append(f"{type(exc).__name__}: {exc}")
            log.debug("closing %s failed", description, exc_info=True)
        finally:
            done.set()

    threading.Thread(target=_close, name=thread_name, daemon=True).start()
    if not done.wait(timeout):
        log.error(
            "%s did not close within %.1fs; RELEASING it and finishing teardown",
            description,
            timeout,
        )
        return RetirementResult(CONNECTION_RETIREMENT.parse("abandoned"))
    if failures:
        return RetirementResult(
            CONNECTION_RETIREMENT.parse("failed"), error=failures[0]
        )
    return RetirementResult(CONNECTION_RETIREMENT.parse("closed"))
