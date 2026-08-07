"""Structural guards for the test-tree refactor.

The lane tests subtract the guard module itself before comparing against the
baseline. That keeps the expected values tied to the pre-refactor suite while
allowing later module-surface guards in this file to be counted explicitly.
"""

from __future__ import annotations

import pytest

_BASELINE_SELECTED = {
    "not motherduck and not slow": 1203,
    "slow and not motherduck": 119,
    "motherduck": 24,
}


def _assert_lane_baseline(request, expression: str) -> None:
    if request.config.getoption("markexpr") != expression:
        return
    guard_path = "tests/unit/test_structure_guards.py::"
    guard_count = sum(item.nodeid.startswith(guard_path) for item in request.session.items)
    selected_without_guards = len(request.session.items) - guard_count
    assert selected_without_guards == _BASELINE_SELECTED[expression]


def test_default_lane_composition(request):
    _assert_lane_baseline(request, "not motherduck and not slow")


@pytest.mark.slow
def test_slow_lane_composition(request):
    _assert_lane_baseline(request, "slow and not motherduck")


@pytest.mark.motherduck
def test_motherduck_lane_composition(request):
    _assert_lane_baseline(request, "motherduck")
