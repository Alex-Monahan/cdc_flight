"""Start-up reconciliation, unit level (ADR 0001 §4.5, §4.7).

The subprocess suite in `tests/rubric/1.1_exactly_once_pk/test_1_1_reconciliation.py`
covers the end-to-end decisions. These tests cover the two cells the reviews
found wrong, which need a *constructed* offset map rather than a real run:

* the file and the destination row agree on a scalar `lsn` and disagree on
  `lsn_proc` / transaction progress — several events share one commit LSN, so the
  file can be genuinely ahead while a scalar guard says "resume" (Codex 3);
* the slot exists and has advanced but the destination has no durable row —
  `check_invariant_o` returned `ok=True` for that, which is ADR §4.5's
  "absent/absent but slot exists" cell reported as healthy (Codex 3).

They need a JVM (the offsets codec) but no Postgres.
"""

from __future__ import annotations

import duckdb
import pytest

from cdc_flight import destination as dest_mod
from cdc_flight import offsets, reconcile
from cdc_flight.destination import ResumePoint
from cdc_flight.errors import NoDurableDestinationRow

PIPELINE = "recon"
NAMESPACE = "cdc-flight-engine"
PARTITION = {"server": "cdcflight"}


@pytest.fixture
def con(tmp_path):
    connection = duckdb.connect(str(tmp_path / "recon.duckdb"))
    dest_mod.ensure_control_schema(connection)
    yield connection
    connection.close()


def _write_row(con, offset: dict, last_lsn: int) -> None:
    dest_mod.write_resume_point(
        con,
        pipeline=PIPELINE,
        namespace=NAMESPACE,
        point=ResumePoint(partition=dict(PARTITION), offset=dict(offset), last_lsn=last_lsn),
        commit_id=1,
        offset_blob=None,
        offset_key_blob=offsets.encode_key(NAMESPACE, PARTITION),
    )


def _write_file(path, offset: dict, *, namespace: str = NAMESPACE, partition=None) -> None:
    offsets.write(
        path,
        {
            offsets.encode_key(namespace, partition or PARTITION): offsets.encode_value(
                offset
            )
        },
    )


def _reconcile(con, path):
    return reconcile.reconcile(
        con, pipeline=PIPELINE, namespace=NAMESPACE, offset_path=path, repair=True
    )


# --------------------------------------------------------------------------- #
# the full typed offset map is canonical, not a scalar LSN
# --------------------------------------------------------------------------- #
def test_an_identical_offset_map_resumes(con, tmp_path):
    path = tmp_path / "offsets.dat"
    offset = {"lsn": 100, "lsn_proc": 100, "txId": 7, "transaction_id": "7:100"}
    _write_row(con, offset, 100)
    _write_file(path, offset)
    assert _reconcile(con, path).decision == "resume"


def test_a_file_ahead_within_one_shared_lsn_is_rebuilt(con, tmp_path):
    """Codex 3's exact probe: `{lsn:100, lsn_proc:999}` versus a durable
    `{lsn:100, lsn_proc:1}` used to return `decision="resume"`."""
    path = tmp_path / "offsets.dat"
    _write_row(con, {"lsn": 100, "lsn_proc": 1}, 100)
    _write_file(path, {"lsn": 100, "lsn_proc": 999})

    outcome = _reconcile(con, path)
    assert outcome.decision == "file_offset_mismatch_rebuilt", outcome
    assert outcome.repaired is True
    (_partition, offset), = offsets.parse_offsets(offsets.read(path))
    assert offset == {"lsn": 100, "lsn_proc": 1}, "the file was not rewritten from the destination"


def test_a_file_whose_partition_is_not_the_expected_one_is_rebuilt(con, tmp_path):
    path = tmp_path / "offsets.dat"
    offset = {"lsn": 100, "lsn_proc": 100}
    _write_row(con, offset, 100)
    _write_file(path, offset, partition={"server": "someone-elses-prefix"})
    assert _reconcile(con, path).decision == "file_offset_mismatch_rebuilt"


def test_a_file_with_extra_entries_is_rebuilt(con, tmp_path):
    """Only `parsed[0]` was ever considered, so a second entry was invisible."""
    path = tmp_path / "offsets.dat"
    offset = {"lsn": 100, "lsn_proc": 100}
    _write_row(con, offset, 100)
    offsets.write(
        path,
        {
            offsets.encode_key(NAMESPACE, PARTITION): offsets.encode_value(offset),
            offsets.encode_key("another-engine", PARTITION): offsets.encode_value(offset),
        },
    )
    assert _reconcile(con, path).decision == "file_offset_mismatch_rebuilt"


def test_a_file_ahead_on_the_scalar_lsn_is_still_reported_as_ahead(con, tmp_path):
    """The alert-worthy case keeps its own decision string and its own alert."""
    path = tmp_path / "offsets.dat"
    _write_row(con, {"lsn": 100}, 100)
    _write_file(path, {"lsn": 200})
    outcome = _reconcile(con, path)
    assert outcome.decision == "file_ahead_rebuilt", outcome
    codes = [r[0] for r in con.execute("SELECT code FROM _cdc_flight.alerts").fetchall()]
    assert "offset_file_ahead" in codes


def test_repair_callback_runs_before_the_canonical_write(con, tmp_path):
    path = tmp_path / "offsets.dat"
    point = {"lsn": 100, "lsn_proc": 1}
    _write_row(con, point, 100)
    observations = []

    outcome = reconcile.reconcile(
        con,
        pipeline=PIPELINE,
        namespace=NAMESPACE,
        offset_path=path,
        repair=True,
        before_repair=lambda row, decision: observations.append(
            (decision, path.exists(), row.commit_id)
        ),
    )

    assert outcome.decision == "file_missing_rebuilt"
    assert observations == [("file_missing_rebuilt", False, 1)]


def test_a_decision_string_never_claims_a_rebuild_that_did_not_happen(con, tmp_path):
    """MINOR-3: with no offset map on the durable row there is nothing to rebuild
    *from*, and an absent file then silently means "re-snapshot"."""
    path = tmp_path / "offsets.dat"
    _write_row(con, {}, 0)
    outcome = _reconcile(con, path)
    assert outcome.repaired is False
    assert outcome.decision == "file_missing_no_durable_offset", outcome


# --------------------------------------------------------------------------- #
# the Invariant-O guard, and the cell it never had
# --------------------------------------------------------------------------- #
def test_a_slot_that_exists_with_no_durable_row_is_not_healthy(con, monkeypatch):
    """ADR §4.5's "absent/absent but slot exists" decision, which
    `check_invariant_o` reported as `ok=True` because `durable is None`.

    With a snapshot mode that does not backfill, the connector would start
    streaming from the slot's confirmed position and every change before it is
    silently gone.
    """
    monkeypatch.setattr(reconcile, "slot_position", lambda dsn, slot_name: 9_000)
    with pytest.raises(NoDurableDestinationRow):
        reconcile.check_invariant_o(
            con,
            pipeline=PIPELINE,
            namespace=NAMESPACE,
            dsn="postgresql://ignored",
            slot_name="slot",
            snapshot_mode="never",
        )
    assert reconcile.check_invariant_o(
        con,
        pipeline=PIPELINE,
        namespace=NAMESPACE,
        dsn="postgresql://ignored",
        slot_name="slot",
        snapshot_mode="never",
        raise_on_violation=False,
    )["ok"] is False
    codes = [r[0] for r in con.execute("SELECT code FROM _cdc_flight.alerts").fetchall()]
    assert "no_durable_destination_row" in codes
    assert codes.count("no_durable_destination_row") == 1


def test_a_slot_that_exists_with_no_durable_row_is_allowed_when_a_backfill_follows(con, monkeypatch):
    """`snapshot.mode=initial` re-snapshots every captured table, so the history
    the destination is missing is about to be re-read in full. It is still not
    "Invariant O healthy" — it gets its own decision, so an operator sees it."""
    monkeypatch.setattr(reconcile, "slot_position", lambda dsn, slot_name: 9_000)
    result = reconcile.check_invariant_o(
        con,
        pipeline=PIPELINE,
        namespace=NAMESPACE,
        dsn="postgresql://ignored",
        slot_name="slot",
        snapshot_mode="initial",
    )
    assert result["ok"] is True
    assert result["decision"] == "no_durable_row_full_snapshot"


def test_a_fresh_start_with_no_slot_is_healthy(con, monkeypatch):
    monkeypatch.setattr(reconcile, "slot_position", lambda dsn, slot_name: None)
    result = reconcile.check_invariant_o(
        con,
        pipeline=PIPELINE,
        namespace=NAMESPACE,
        dsn="postgresql://ignored",
        slot_name="slot",
        snapshot_mode="never",
    )
    assert result["ok"] is True


def test_a_slot_ahead_of_the_destination_raises(con, monkeypatch):
    """The one row of the decision table that had no dedicated test; it is what
    fires on a re-created slot and it routes to rubric 1.8."""
    from cdc_flight.errors import SlotAheadOfDestination

    _write_row(con, {"lsn": 100}, 100)
    monkeypatch.setattr(reconcile, "slot_position", lambda dsn, slot_name: 500)
    with pytest.raises(SlotAheadOfDestination):
        reconcile.check_invariant_o(
            con,
            pipeline=PIPELINE,
            namespace=NAMESPACE,
            dsn="postgresql://ignored",
            slot_name="slot",
            snapshot_mode="initial",
        )
