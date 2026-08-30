"""Policy exclusion preserves TOAST disposition without retaining source values."""

from __future__ import annotations

from support.applier_lab import data

from cdc_flight.policy import PIIPolicy, PolicyGate
from cdc_flight.toast import STRUCTURAL_MARKER
from cdc_flight.typed_types import SourceTypeDescriptor


def test_excluded_toast_field_is_not_reintroduced_by_sanitization(tmp_path):
    salt = tmp_path / "toast-policy-salt"
    salt.write_bytes(b"toast-policy-salt")
    salt.chmod(0o600)
    policy = PIIPolicy.from_manifest(
        [
            {"column_regex": r"^app\.toast_policy\.id$", "action": "replicate"},
            {"column_regex": r"^app\.toast_policy\.body$", "action": "exclude"},
        ],
        unmatched="exclude",
        salt_file=None,
    )
    event = data(
        "toast-policy", 1, 100, table="toast_policy", key={"id": 1},
        after={"id": 1, "body": STRUCTURAL_MARKER},
    )
    int4 = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
    text = SourceTypeDescriptor(25, "pg_catalog.text", "text")
    event.key_descriptors = {"id": int4}
    event.after_descriptors = {"id": int4, "body": text}
    PolicyGate(policy).sanitize(event, event.after_descriptors)
    assert event.after == {"id": 1}
    assert "body" not in event.after_descriptors
    assert "body" not in (event.typed_after.to_dict() if event.typed_after else {})
