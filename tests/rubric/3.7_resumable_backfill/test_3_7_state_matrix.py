"""§3 state-combination coverage is generated from the production declarations."""

from __future__ import annotations

from support.backfill_lab import require_backfill


def test_backfill_machines_and_every_declared_interaction_pair_have_matrix_cells():
    """A new state or pair without an executed/refused cell must fail collection."""
    backfill = require_backfill()
    from cdc_flight import machines

    declared = machines.declared_machines()
    assert "backfill_run" in declared
    assert "shadow_claim" in declared
    matrix = backfill.build_state_matrix(declared, machines.INTERACTING_MACHINE_PAIRS)
    assert matrix.unaccounted == ()
    assert matrix.cell_count > 0
    assert matrix.has_pair("backfill_run", "shadow_claim")
    assert matrix.has_pair("backfill_run", "table_lifecycle")
    assert matrix.has_pair("backfill_run", "destination_ownership")


def test_every_backfill_run_edge_and_shadow_claim_collision_is_refused_or_executed():
    """Normal, terminal, invalid-regression, and owner-collision edges are not prose."""
    backfill = require_backfill()
    report = backfill.run_state_matrix()
    assert report.uncovered_edges == ()
    assert report.refused_edges
    assert report.collision_cells
