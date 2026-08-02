"""Rubric 4.7 — the failure-mode inventory is checked against the code, not recalled.

The 4.7 score is a *count* of manual-intervention cases, so the count is load-bearing
evidence, and it was wrong: the ADR claimed `24 AUTO / 9 MANUAL / 6 UNDEFINED`, which
totals 39 against a 40-row table and matched no reading of the table's own class column
(Codex M5 / Opus MAJOR-3). Both reviewers had to count the rows by hand to find that out.
Rev 8 made the headline a parsed aggregation, which closed that.

**Rev 9 (rubric 1.9) closes the failure mode that outranks it: a row that names a
transition the code does not have, and a transition the code has that no row accounts
for.** Every inventory row now carries a `machine · edge` cell, and this test:

* regenerates §A51.1's transition tables from `machine.table()` and fails on any
  difference — the ADR's tables are *generated*, so MAJOR-3's drift cannot recur one
  level up;
* resolves every row's edge against the declared machines and domains;
* prints, for every machine, the `states x states` cells with no declared edge — which
  is what makes UNDEFINED mechanically findable rather than found by reading code for a
  day.

It is a documentation test on purpose. A number a rubric score rests on is code.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pytest

import cdc_flight.pipeline  # noqa: F401 - imports every module that declares a machine
from cdc_flight import machines as machines_mod
from cdc_flight.states import Domain, Machine

ADR = Path(__file__).resolve().parents[2] / "docs" / "adr" / "0001-transactional-applier.md"
RUBRIC_STATUS = Path(__file__).resolve().parents[2] / "RUBRIC_STATUS.md"
CLASSES = ("AUTO", "MANUAL", "UNDEFINED")
COLUMNS = 7


def _text() -> str:
    return ADR.read_text()


def _between(start: str, end: str) -> str:
    text = _text()
    return text[text.index(start) : text.index(end)]


def _inventory() -> str:
    return _between("#### A51.2 —", "**The counts, parsed from")


def _rows() -> list[tuple[str, str, str]]:
    """`(number, machine-edge cell, class cell)` for every inventory row."""
    rows = []
    for line in _inventory().splitlines():
        if not line.startswith("| ") or line.startswith("| # |") or set(line) <= set("|- "):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        assert len(cells) == COLUMNS, f"A51 row is not {COLUMNS} columns: {line}"
        rows.append((cells[0], cells[1], cells[-1]))
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
    counts = _between("**The counts, parsed from", "#### A51.3")
    stated = {}
    for cls in CLASSES:
        match = re.search(rf"\| `{cls}` \| \*\*(\d+)\*\* \|", counts)
        assert match, f"A51's count table does not state a number for {cls}"
        stated[cls] = int(match.group(1))
    return stated


def _declared_machines() -> dict[str, Machine]:
    return {v.name: v for v in vars(machines_mod).values() if isinstance(v, Machine)}


def _declared_domains() -> dict[str, Domain]:
    return {v.name: v for v in vars(machines_mod).values() if isinstance(v, Domain)}


# --------------------------------------------------------------------------- #
# the headline still matches the rows
# --------------------------------------------------------------------------- #
def test_every_row_carries_exactly_one_terminal_class():
    for number, _edge, value in _rows():
        assert _classify(value), number


def test_row_numbers_are_unique():
    numbers = [n for n, _e, _c in _rows()]
    duplicates = sorted({n for n in numbers if numbers.count(n) > 1})
    assert not duplicates, duplicates


@pytest.mark.parametrize("cls", CLASSES)
def test_the_headline_count_matches_the_rows(cls):
    actual = defaultdict(int)
    for _number, _edge, value in _rows():
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


def test_rubric_status_uses_the_counts_parsed_from_a51():
    """Round 8 MINOR-2: the score narrative is another consumer of this data."""
    actual = defaultdict(int)
    rows = _rows()
    for _number, _edge, value in rows:
        actual[_classify(value)] += 1
    generated = (
        f"{len(rows)} rows, {actual['AUTO']} AUTO / {actual['MANUAL']} MANUAL / "
        f"{actual['UNDEFINED']} UNDEFINED"
    )
    assert generated in RUBRIC_STATUS.read_text(), (
        "RUBRIC_STATUS's current A51 count is not the value parsed from the ADR rows"
    )


# --------------------------------------------------------------------------- #
# rev 9: every row is anchored to something the code actually has
# --------------------------------------------------------------------------- #
def test_the_adr_transition_tables_are_the_ones_the_code_declares():
    """Generated, not transcribed.

    MAJOR-3 was a hand-authored number drifting from the rows it summarised. A
    hand-authored transition table is the same failure one level up, and it is the one
    the whole inventory now rests on.
    """
    section = _between("#### A51.1 —", "#### A51.2 —")
    for name, machine in _declared_machines().items():
        assert f"**`{name}`**" in section, f"§A51.1 does not document the {name} machine"
        for row in machine.table():
            line = f"| `{row['from']}` | `{row['to']}` | {'yes' if row['terminal'] else 'no'} |"
            assert line in section, (
                f"§A51.1's table for `{name}` is missing the declared edge "
                f"{row['from']!r} -> {row['to']!r}. The tables are generated from "
                "`machine.table()`; regenerate them rather than editing by hand."
            )
        # And no edge in the doc that the code does not have.
        block = section[section.index(f"**`{name}`**"):]
        for other in _declared_machines():
            marker = f"**`{other}`**"
            if other != name and marker in block:
                block = block[: block.index(marker)]
        documented = set(re.findall(r"^\| `([a-z_]+)` \| `([a-z_]+)` \|", block, re.M))
        assert documented == set(machine.edges), (
            f"§A51.1's table for `{name}` documents edges the code does not declare: "
            f"{sorted(documented - set(machine.edges))}"
        )


def test_every_inventory_row_names_a_real_edge_or_domain_or_says_it_is_not_one():
    """The row that used to be impossible to get wrong because it said nothing.

    Grammar of the `machine · edge` cell:
      * `—`                       — not a transition (a pre-condition, an internal
                                    invariant, or the commit group, which is memory-only
                                    by design)
      * `<machine>: <from> -> <to>` — must be a DECLARED edge
      * `<domain> (domain)`         — must be a declared decision domain
    """
    machines = _declared_machines()
    domains = _declared_domains()
    problems: list[str] = []
    for number, edge, _cls in _rows():
        if edge == "—":
            continue
        if edge.endswith("(domain)"):
            name = edge[: -len("(domain)")].strip()
            if name not in domains:
                problems.append(f"row {number}: {name!r} is not a declared domain")
            continue
        match = re.fullmatch(r"([a-z_]+): ([a-z_]+) -> ([a-z_]+)", edge)
        if not match:
            problems.append(f"row {number}: {edge!r} is not `machine: from -> to`")
            continue
        name, frm, to = match.groups()
        machine = machines.get(name)
        if machine is None:
            problems.append(f"row {number}: {name!r} is not a declared machine")
        elif (frm, to) not in machine.edges:
            problems.append(
                f"row {number}: `{name}` has no edge {frm!r} -> {to!r}; the inventory "
                "describes a transition the code does not have"
            )
    assert not problems, "\n".join(problems)


def test_every_durable_machine_is_accounted_for_by_at_least_one_row():
    """A machine with no inventory row is a durable state whose failure modes nobody
    enumerated — which is precisely the hole the restructure exists to close."""
    referenced = {
        edge.split(":")[0]
        for _n, edge, _c in _rows()
        if edge != "—" and not edge.endswith("(domain)")
    }
    durable = {name for name, mm in _declared_machines().items() if mm.durable}
    missing = sorted(durable - referenced)
    assert not missing, (
        f"these machines persist state and no A51 row names any of their edges: {missing}"
    )


def test_the_cells_with_no_declared_edge_are_printable(capsys):
    """UNDEFINED, made mechanical.

    The *count* is not the point — most cells are nonsense (`complete -> complete` is
    declared, `absent -> absent` is not, and neither is interesting). The point is that
    the set is computable, so a genuinely missing edge cannot hide in prose. Printed on
    every run of this test, which is where a reviewer looks.
    """
    lines = []
    for name, machine in sorted(_declared_machines().items()):
        cells = machine.unreachable_cells()
        lines.append(f"{name}: {len(cells)} of {len(machine.states) ** 2 - len(machine.states)} "
                     f"cells have no declared edge")
        for frm, to in cells:
            lines.append(f"    {frm} -> {to}")
    print("\n".join(lines))
    assert lines
