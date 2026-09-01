"""The single source-endpoint routing policy used by the Flight.

There are deliberately three routes.  A hot standby is the read/decoding endpoint,
but it is not the source of truth for writes, and a logical slot physically created
on that standby must not be administered through the primary write route.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - imported only for static checkers
    from .config import SourceConfig


@dataclass(frozen=True)
class SourceRoutePolicy:
    """Explicit read, source-write, and logical-slot-owner endpoints."""

    role: str
    read_replication_dsn: str
    source_write_dsn: str
    slot_owner_dsn: str

    @property
    def read_dsn(self) -> str:
        """The endpoint used for catalog, snapshot, decoding, and slot health reads."""
        return self.read_replication_dsn

    @property
    def slot_owner_scope(self) -> str:
        """The server that physically owns the local logical slot."""
        return "standby" if self.role == "standby" else "primary"

    @classmethod
    def from_source(cls, source: SourceConfig) -> SourceRoutePolicy:
        """Resolve all three routes before any pipeline side effect is allowed."""
        role = source.role
        read_dsn = str(source.dsn).strip()
        if not read_dsn:
            raise ValueError("the source read/replication DSN may not be empty")

        if role == "standby":
            # SourceConfig.primary_dsn is intentionally fail-closed.  Calling it
            # here makes missing CDC_PRIMARY_DSN an admission error, rather than a
            # late failure after destination/state mutation has started.
            source_write_dsn = str(source.primary_dsn).strip()
            if not source_write_dsn:
                raise ValueError(
                    "CDC_PRIMARY_DSN must be non-empty when CDC_SOURCE_ROLE=standby"
                )
            # This is the central distinction: source writes go to the primary, but
            # a local logical slot operation stays on the standby that owns it.
            slot_owner_dsn = read_dsn
        else:
            source_write_dsn = read_dsn
            slot_owner_dsn = read_dsn

        return cls(
            role=role,
            read_replication_dsn=read_dsn,
            source_write_dsn=source_write_dsn,
            slot_owner_dsn=slot_owner_dsn,
        )

    def as_dict(self) -> dict[str, str]:
        """Return durable, human-auditable route facts without credentials changes."""
        result = asdict(self)
        result["read_dsn"] = self.read_dsn
        result["slot_owner_scope"] = self.slot_owner_scope
        return result


__all__ = ["SourceRoutePolicy"]
