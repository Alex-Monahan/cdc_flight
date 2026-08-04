"""Named states and declared transitions — rubric 1.9's mechanism (ADR §20/A55).

Every named regression this project has recorded is one shape: **a state that exists
in the design, is represented as a derived expression over two or more variables, and
is therefore mutated by a path the design did not enumerate.** `_reset_group()` on the
success path only; `stop_reason='source_dark'` overwritten by `'hung'` in a `finally`;
"the re-snapshot completed" inferred from `swaps > 0 and not active` across three
modules; "a recovery is in progress" as four unjournalled mutations and a local
variable. None of those is a hard problem once the state has a *name* and the legal
edges are written down: the unenumerated path becomes a run-time error instead of a
review finding.

This module is the whole mechanism, and it is deliberately about 200 lines with no
dependencies:

* **states are plain strings.** They already live in `VARCHAR` columns
  (`table_state.snapshot_state`, `recovery_state.phase`), in `last_run.json`, and in
  test literals. An `enum.Enum` would need a migration and would break every existing
  SQL comparison for no gain; the value of a state machine here is the *declared edge
  set*, not the Python type.
* **`Machine.check(from, to)` raises** on an edge nobody declared. It is called inside
  the one function allowed to write that state, so there is no second path.
* **`Machine.table()` emits the transition table as data**, which is what makes the
  rubric 4.7 inventory (ADR §19/A51) mechanical: every row of the inventory names a
  machine and an edge, and the test checks those exist.

Two shapes are supported:

* `Machine` — a state domain plus its legal edges (`TableLifecycle`, `RunPhase`,
  `AcquisitionRecovery`, `CatalogChangeState`, `CatalogBaseline`).
* `Domain` — a frozen set of values with **no** transition structure, for decision
  tables whose outputs are classifications rather than states an object moves through
  (`SlotVerdict.decision`, the reconciliation decisions). Freezing them costs nothing
  and gives the inventory a shared vocabulary; pretending they are state machines
  would be ceremony.

`RANKED` is the third shape and it is a `Machine` built from a **precedence**: the
only legal edges are escalations. That is the whole of A49's cause-before-symptom
rule, which was previously two copies of a literal tuple at `supervisor.py:180` and
`:186` — add a tenth outcome and you had to remember both sites.
"""

from __future__ import annotations

import logging
from typing import ClassVar

log = logging.getLogger("cdc_flight.states")

__all__ = [
    "Domain",
    "IllegalTransition",
    "Machine",
    "UnknownState",
    "machine",
    "machines",
    "transition_rows",
]


class UnknownState(ValueError):
    """A durable value that is not in the declared domain.

    Loud on purpose. A control row written by a newer (or corrupted) version names a
    state we have no rules for, and the honest answer is to refuse rather than to fall
    through every branch and log success — which is exactly what `recovery.resume()`
    did before `PHASES` was enforced.
    """


class IllegalTransition(RuntimeError):
    """A transition nobody declared. The bug class rubric 1.9 exists to close."""


#: Every machine, by name, in declaration order. Populated by `Machine.__init__`, so
#: importing this module (and the modules that declare machines) is the only
#: registration step. The 4.7 inventory test enumerates it.
_REGISTRY: dict[str, Machine] = {}


class Machine:
    """A named state domain plus its legal edges. Frozen at construction."""

    __slots__ = (
        "durable",
        "edges",
        "initial",
        "initial_states",
        "name",
        "purpose",
        "states",
        "terminal",
    )

    def __init__(
        self,
        name: str,
        *,
        states: tuple[str, ...],
        edges: tuple[tuple[str, str], ...],
        terminal: tuple[str, ...] = (),
        initial: str | None = None,
        initial_states: tuple[str, ...] | None = None,
        purpose: str = "",
        durable: str | None = None,
    ) -> None:
        """`durable` names where the state is persisted, or None for memory-only.

        It is not decoration: the distinguishing test for "does this need a machine at
        all" is *does a crash leave durable state in an intermediate configuration*,
        and recording the answer next to the machine is what stops a memory-only
        machine quietly acquiring a durable writer.
        """
        if len(set(states)) != len(states):
            raise ValueError(f"{name}: duplicate state")
        unknown = {s for edge in edges for s in edge} | set(terminal)
        unknown -= set(states)
        if unknown:
            raise ValueError(f"{name}: edges/terminal name undeclared states {sorted(unknown)}")
        declared_initials = (
            tuple(initial_states)
            if initial_states is not None
            else (() if initial is None else (initial,))
        )
        if len(set(declared_initials)) != len(declared_initials):
            raise ValueError(f"{name}: duplicate initial state")
        unknown_initials = set(declared_initials) - set(states)
        if unknown_initials:
            raise ValueError(
                f"{name}: initial states undeclared {sorted(unknown_initials)}"
            )
        if initial is not None and initial not in declared_initials:
            raise ValueError(
                f"{name}: initial {initial!r} is not in initial_states"
            )
        self.name = name
        self.states = tuple(states)
        self.edges = frozenset(edges)
        self.terminal = frozenset(terminal)
        self.initial_states = declared_initials
        self.initial = initial if initial is not None else (
            declared_initials[0] if declared_initials else None
        )
        self.purpose = purpose
        self.durable = durable
        if name in _REGISTRY:
            raise ValueError(f"a machine named {name!r} is already declared")
        _REGISTRY[name] = self

    # -- reading a state ---------------------------------------------------- #
    def parse(self, value) -> str:
        """A durable string -> a state. Unknown values raise, loudly."""
        text = str(value)
        if text not in self.states:
            raise UnknownState(
                f"{self.name}: {text!r} is not one of {list(self.states)}. A state "
                "outside the declared domain belongs to no queue and no recovery path, "
                "so it is refused rather than skipped (ADR 0001 §20/A55)."
            )
        return text

    def is_terminal(self, state: str) -> bool:
        return self.parse(state) in self.terminal

    # -- moving between states ---------------------------------------------- #
    def allows(self, frm: str, to: str) -> bool:
        """True if `(frm, to)` is declared. Does not raise on unknown states."""
        return (str(frm), str(to)) in self.edges

    def check(self, frm: str, to: str) -> None:
        """Raise `IllegalTransition` unless `(frm, to)` is declared.

        Both ends are parsed first, so a corrupted `from` is an `UnknownState` rather
        than a confusing "no such edge".
        """
        frm = self.parse(frm)
        to = self.parse(to)
        if (frm, to) not in self.edges:
            legal = sorted(self.successors(frm)) or ["nothing - it is terminal"]
            raise IllegalTransition(
                f"{self.name}: {frm!r} -> {to!r} is not a declared transition "
                f"(legal from {frm!r}: {legal}). This is refused rather than performed: "
                "an undeclared edge is how every regression in this project's history "
                "was written."
            )

    def successors(self, frm: str) -> set[str]:
        return {b for a, b in self.edges if a == frm}

    def reachable_states(self) -> frozenset[str]:
        """States reachable from every declared initial state.

        A machine may have more than one legitimate starting condition.  The
        snapshot-completion machine uses that to model a streaming-only run, whose
        initial state is ``not_required`` and has no edge from the callback protocol.
        Matrix coverage must enumerate those starts instead of silently dropping one.
        """
        reachable = set(self.initial_states)
        frontier = list(reachable)
        while frontier:
            state = frontier.pop()
            for successor in self.successors(state):
                if successor not in reachable:
                    reachable.add(successor)
                    frontier.append(successor)
        return frozenset(reachable)

    # -- the transition table, as data -------------------------------------- #
    def table(self) -> list[dict]:
        """Every declared edge, sorted. Consumed by the 4.7 inventory test and the ADR."""
        return [
            {
                "machine": self.name,
                "from": frm,
                "to": to,
                "terminal": to in self.terminal,
                "durable": self.durable,
            }
            for frm, to in sorted(self.edges)
        ]

    def unreachable_cells(self) -> list[tuple[str, str]]:
        """`(from, to)` pairs in `states x states` with no declared edge.

        The mechanical definition of "a cell nobody has thought about". Most are
        nonsense (`live -> live`) and the inventory says so once, per machine, rather
        than enumerating the product; what matters is that the product is *computable*
        so a genuinely missing edge cannot hide in prose.
        """
        return [
            (a, b)
            for a in self.states
            for b in self.states
            if a != b and (a, b) not in self.edges
        ]

    def __repr__(self) -> str:  # pragma: no cover - diagnostics
        return f"<Machine {self.name}: {len(self.states)} states, {len(self.edges)} edges>"


def ranked(
    name: str,
    *,
    order: tuple[str, ...],
    purpose: str = "",
    durable: str | None = None,
) -> Machine:
    """A machine whose only legal edges are **escalations** along `order`.

    `order` is least-severe first. `RunOutcome` is the case this exists for: A49's
    defect was a `finally` block overwriting the *cause* (`source_dark`) with the
    *symptom* (`hung`), and the fix was a hand-written `if stop_reason not in
    ("source_dark", "engine_error")` repeated at two call sites. Expressed as a
    precedence, "the symptom overwrites the diagnosis" is not a rule anyone has to
    remember: it is an edge that does not exist.
    """
    edges = tuple(
        (order[i], order[j]) for i in range(len(order)) for j in range(i + 1, len(order))
    )
    return Machine(
        name,
        states=order,
        edges=edges,
        terminal=(),
        initial=order[0],
        purpose=purpose,
        durable=durable,
    )


class Domain:
    """A frozen set of values with no transition structure.

    For decision tables (`check_slot`, `offset_reconcile.reconcile`) whose output
    classifies an *external* configuration rather than naming a state something moves
    through. `RESNAPSHOT_DECISIONS` and `RECONCILE_DECISIONS` were both declared and
    consumed only by a test; freezing them here makes them the shared vocabulary the
    4.7 inventory and the run summary both use.
    """

    __slots__ = ("name", "purpose", "values")

    _REGISTRY: ClassVar[dict[str, Domain]] = {}

    def __init__(self, name: str, *, values: tuple[str, ...], purpose: str = "") -> None:
        if len(set(values)) != len(values):
            raise ValueError(f"{name}: duplicate value")
        self.name = name
        self.values = tuple(values)
        self.purpose = purpose
        if name in Domain._REGISTRY:
            raise ValueError(f"a domain named {name!r} is already declared")
        Domain._REGISTRY[name] = self

    def parse(self, value) -> str:
        text = str(value)
        if text not in self.values:
            raise UnknownState(
                f"{self.name}: {text!r} is not one of {list(self.values)}"
            )
        return text

    def __contains__(self, value) -> bool:
        return str(value) in self.values

    def __iter__(self):
        return iter(self.values)

    def __len__(self) -> int:
        return len(self.values)

    def __repr__(self) -> str:  # pragma: no cover - diagnostics
        return f"<Domain {self.name}: {len(self.values)} values>"


# --------------------------------------------------------------------------- #
# registry access
# --------------------------------------------------------------------------- #
def machine(name: str) -> Machine:
    return _REGISTRY[name]


def machines() -> dict[str, Machine]:
    """Every declared machine. Import `cdc_flight.machines` first — that module is
    where the declarations live, and importing it is what registers them."""
    return dict(_REGISTRY)


def domains() -> dict[str, Domain]:
    return dict(Domain._REGISTRY)


def transition_rows() -> list[dict]:
    """Every edge of every machine, as flat data. The 4.7 inventory's key space."""
    rows: list[dict] = []
    for m in _REGISTRY.values():
        rows.extend(m.table())
    return rows
