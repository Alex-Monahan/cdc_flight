"""Regression coverage for MotherDuck scratch-database cleanup."""

from __future__ import annotations

import pytest
from support.motherduck_probe import _drop_database

pytestmark = pytest.mark.motherduck


def test_failing_drop_is_surfaced_instead_of_swallowed():
    class FailingConnection:
        def execute(self, sql):
            if str(sql).startswith("DROP DATABASE"):
                raise RuntimeError("drop denied")
            raise AssertionError("database verification must not follow a failed DROP")

        def close(self):
            return None

    with pytest.raises(RuntimeError, match="could not prove") as raised:
        _drop_database(
            "token",
            "scratch_db",
            connect_factory=lambda _dsn: FailingConnection(),
            attempts=1,
            delay=0,
        )
    assert isinstance(raised.value.__cause__, RuntimeError)
    assert str(raised.value.__cause__) == "drop denied"
