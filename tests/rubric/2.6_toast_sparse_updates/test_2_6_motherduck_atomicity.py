"""MotherDuck atomicity guard for sparse writes."""

from __future__ import annotations

import pytest

from cdc_flight.row_patch import RowPatch
from cdc_flight.toast import STRUCTURAL_MARKER, field_value
from cdc_flight.typed_types import SourceTypeDescriptor

pytestmark = pytest.mark.motherduck


def test_patch_digest_is_stable_for_memory_and_spill():
    text = SourceTypeDescriptor(25, "pg_catalog.text", "text")
    memory = RowPatch({"body": field_value(STRUCTURAL_MARKER, text)})
    spill = RowPatch.from_dict(memory.to_dict())
    assert spill.digest == memory.digest
    assert spill.field("body").state.value == "unchanged_toast"
