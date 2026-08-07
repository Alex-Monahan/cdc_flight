"""Load the shared pytest fixtures from :mod:`tests.support.fixtures`.

Pytest applies this file to every suite below ``tests/``.  The implementation
lives beside the other reusable test support, while this small boundary module
keeps the common fixture scope and compatibility for tests that inspect the
support module directly.
"""

from __future__ import annotations

import sys
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from support import fixtures as _support_fixtures

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
    _support_fixtures.pytest_configure(config)


def pytest_sessionstart(session):
    _support_fixtures.pytest_sessionstart(session)


def pytest_unconfigure(config):
    _support_fixtures.pytest_unconfigure(config)
