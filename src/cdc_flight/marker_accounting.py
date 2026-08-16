"""Count Flight-owned logical-message transaction records received by a run."""

from __future__ import annotations

from .envelope import KIND_MESSAGE, KIND_TXN_BEGIN, KIND_TXN_END, PendingRecord


class SourceMarkerReceiptCounter:
    """Return newly received raw records belonging to a Flight marker.

    Source-marker writes are not delivery evidence: the source can accept the
    shutdown marker after callback admission has been sealed. The logical message
    identifies a marker transaction; its BEGIN and END may arrive in another
    callback, so their counts are retained until the identifying message appears.
    """

    def __init__(self, prefixes: tuple[str, ...] | None = None) -> None:
        self._prefixes = tuple(dict.fromkeys(prefixes or ("cdcf",)))
        self._txn_records: dict[str, int] = {}
        self._marker_txns: set[str] = set()

    def observe(self, rec: PendingRecord) -> int:
        """Return how many records in ``rec``'s transaction are marker records."""
        if rec.kind not in (KIND_TXN_BEGIN, KIND_TXN_END, KIND_MESSAGE):
            return 0
        txn_id = rec.txn_id
        if txn_id is None:
            return int(rec.kind == KIND_MESSAGE and self._is_marker(rec.message_prefix))

        if rec.kind == KIND_TXN_BEGIN:
            self._txn_records[txn_id] = 1
            return 0

        count = self._txn_records.get(txn_id, 0) + 1
        self._txn_records[txn_id] = count
        received = 0
        if rec.kind == KIND_MESSAGE and self._is_marker(rec.message_prefix):
            self._marker_txns.add(txn_id)
            received = count
        elif txn_id in self._marker_txns:
            received = 1

        if rec.kind == KIND_TXN_END:
            self._txn_records.pop(txn_id, None)
            self._marker_txns.discard(txn_id)
        return received

    def _is_marker(self, prefix: str | None) -> bool:
        value = str(prefix or "")
        return any(value.startswith(f"{marker}_") for marker in self._prefixes)
