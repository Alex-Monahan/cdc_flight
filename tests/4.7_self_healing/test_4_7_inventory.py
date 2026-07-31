"""Rubric 4.7 — the failure-mode inventory's headline counts are PARSED, not recalled.

The 4.7 score is a *count* of manual-intervention cases, so the count is load-bearing
evidence and it was wrong: the ADR claimed `24 AUTO / 9 MANUAL / 6 UNDEFINED`, which
totals 39 against a 40-row table and matched no reading of the table's own class column
(Codex M5 / Opus MAJOR-3). Both reviewers had to count the rows by hand to find that out.

This test re-parses A51 and fails if the stated headline stops matching the rows, and if
any row carries two terminal classes in one cell — which is how the old table hid a
mode that was AUTO on one reading and UNDEFINED on another.

It is a documentation test on purpose. A number a rubric score rests on is code.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pytest

ADR = Path(__file__).resolve().parents[2] / "docs" / "adr" / "0001-transactional-applier.md"
CLASSES = ("AUTO", "MANUAL", "UNDEFINED")


def _section() -> tuple[str, str]:
    text = ADR.read_text()
    start = text.index("### A51 —")
    split = text.index("**The counts, parsed from")
    return text[start:split], text[split:]


def _rows() -> list[tuple[str, str]]:
    table, _ = _section()
    rows = []
    for line in table.splitlines():
        if not line.startswith("| ") or line.startswith("| # |") or set(line) <= set("|- "):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        assert len(cells) == 6, f"A51 row is not 6 columns: {line}"
        rows.append((cells[0], cells[-1]))
    return rows


def _classify(value: str) -> str:
    named = [c for c in CLASSES if c in value]
    assert named, f"A51 row class {value!r} names none of {CLASSES}"
    assert len(named) == 1, (
        f"A51 row class {value!r} names {named}: one row, one failure, one terminal "
        "class. A cell that is AUTO on one reading and UNDEFINED on another is how the "
        "old inventory made its headline unreconstructable"
    )
    return named[0]


def _stated() -> dict[str, int]:
    _table, counts = _section()
    stated = {}
    for cls in CLASSES:
        match = re.search(rf"\| `{cls}` \| \*\*(\d+)\*\* \|", counts)
        assert match, f"A51's count table does not state a number for {cls}"
        stated[cls] = int(match.group(1))
    return stated


def test_every_row_carries_exactly_one_terminal_class():
    for number, value in _rows():
        assert _classify(value), number


def test_row_numbers_are_unique():
    numbers = [n for n, _ in _rows()]
    duplicates = sorted({n for n in numbers if numbers.count(n) > 1})
    assert not duplicates, duplicates


@pytest.mark.parametrize("cls", CLASSES)
def test_the_headline_count_matches_the_rows(cls):
    actual = defaultdict(int)
    for _number, value in _rows():
        actual[_classify(value)] += 1
    assert actual[cls] == _stated()[cls], (
        f"A51 says {_stated()[cls]} {cls} rows and the table contains {actual[cls]}. "
        "The score for rubric 4.7 is this number; it may not be approximate."
    )


def test_the_manual_count_is_what_the_rubric_band_is_read_from():
    """4.7's 1-band is literally "more than 2 cases that cause manual intervention"."""
    manual = _stated()["MANUAL"]
    assert manual > 2, (
        "if this ever drops to 2 or fewer, rubric 4.7 moves off its 1-band and "
        "RUBRIC_STATUS must be rescored deliberately rather than by this assertion "
        "quietly failing"
    )
