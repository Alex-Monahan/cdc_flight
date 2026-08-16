"""Load the shared pytest fixtures from :mod:`tests.support.fixtures`.

Pytest applies this file to every suite below ``tests/``.  The implementation
lives beside the other reusable test support, while this small boundary module
keeps the common fixture scope and compatibility for tests that inspect the
support module directly.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from support import fixtures as _support_fixtures

_MOTHERDUCK_STATUSES: dict[str, str] = {}
_MOTHERDUCK_LANE_REQUESTED = False

# Preserve the old ``import conftest`` surface for direct infrastructure tests
# without duplicating the fixtures or pytest hooks in this loader.
for _name, _value in vars(_support_fixtures).items():
    if not _name.startswith("__") and _name not in {
        "pytest_configure",
        "pytest_sessionstart",
        "pytest_unconfigure",
    }:
        globals()[_name] = _value


def pytest_configure(config):
    global _MOTHERDUCK_LANE_REQUESTED
    _MOTHERDUCK_STATUSES.clear()
    _MOTHERDUCK_LANE_REQUESTED = False
    _support_fixtures.pytest_configure(config)


def pytest_sessionstart(session):
    _support_fixtures.pytest_sessionstart(session)


def _motherduck_lane_requested(config) -> bool:
    return (config.getoption("markexpr") or "").strip() == "motherduck"


def pytest_collection_finish(session):
    """Make an explicitly requested MotherDuck lane fail closed."""
    global _MOTHERDUCK_LANE_REQUESTED
    if not _motherduck_lane_requested(session.config):
        return
    _MOTHERDUCK_LANE_REQUESTED = True
    if not (os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")):
        raise pytest.UsageError(
            "the MotherDuck capability lane selected tests but no "
            "motherduck_token/MOTHERDUCK_TOKEN is set"
        )


def pytest_runtest_logreport(report):
    """Count real MotherDuck test bodies separately from fixture skips."""
    if not _MOTHERDUCK_LANE_REQUESTED:
        return
    if report.when == "setup" and report.outcome == "skipped":
        _MOTHERDUCK_STATUSES[report.nodeid] = "skipped"
    elif report.when == "call":
        _MOTHERDUCK_STATUSES[report.nodeid] = (
            "skipped" if report.outcome == "skipped" else "executed"
        )


def pytest_sessionfinish(session, exitstatus):
    if not _MOTHERDUCK_LANE_REQUESTED:
        return
    if session.config.getoption("collectonly"):
        return
    selected = len(session.items)
    executed = sum(status == "executed" for status in _MOTHERDUCK_STATUSES.values())
    skipped = sum(status == "skipped" for status in _MOTHERDUCK_STATUSES.values())
    terminal = session.config.pluginmanager.get_plugin("terminalreporter")
    if terminal is not None:
        terminal.write_line(
            "MotherDuck capability lane accounting: "
            f"selected={selected}, executed={executed}, skipped={skipped}"
        )
    if selected != executed or skipped:
        session.exitstatus = 1


def pytest_unconfigure(config):
    _support_fixtures.pytest_unconfigure(config)
