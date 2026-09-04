"""Pure contracts for retained-slot recovery summary evidence."""

from __future__ import annotations

from cdc_flight import resnapshot_batches


def test_verified_empty_summary_requires_positive_resnapshot_evidence():
    """A non-empty rebuild never receives an emptiness certificate by default."""
    no_empty = resnapshot_batches.summarize_passes(
        [
            {
                "resnapshot_swapped": ["app.documents"],
                "resnapshot_emptied": [],
                "resnapshot_empty_check_lsn": None,
            }
        ]
    )
    assert "verified_empty_after_snapshot" not in no_empty
    assert "verified_empty_fence_lsn" not in no_empty
    assert resnapshot_batches.verified_empty_summary(
        no_empty["resnapshot_passes"]
    ) == {}
    assert resnapshot_batches.verified_empty_summary(
        [
            {
                "resnapshot_emptied": ["app.documents"],
                "resnapshot_empty_check_lsn": 123,
            }
        ]
    ) == {
        "verified_empty_after_snapshot": ["app.documents"],
        "verified_empty_fence_lsn": 123,
    }
