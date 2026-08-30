"""§8.1 final-state and ledger proof against the actual MotherDuck destination."""

from __future__ import annotations

import contextlib
from pathlib import Path

import pytest
from support.applier_lab import Lab, data, end
from support.motherduck_probe import connect

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]


@pytest.mark.parametrize("mode", ["hard", "soft"])
def test_hard_and_soft_delete_contract_on_motherduck(motherduck_case, mode):
    token = motherduck_case["token"]
    database = motherduck_case["database"]
    control = motherduck_case["control_schema"]
    dataset = motherduck_case["dataset"]
    con = connect(token, database)
    box = Lab(
        Path("motherduck-delete-lab.duckdb"),
        connection=con,
        dataset=dataset,
        control_schema=control,
        pipeline=f"p8_md_delete_{mode}",
        namespace=f"p8-md-delete-{mode}",
        delete_mode=mode,
    )
    try:
        box.run(
            [
                data("seed", 1, 10, table="md_delete", key={"id": 1}, after={"id": 1, "name": "md-row"}),
                end("seed", 1, 20, {"app.md_delete": 1}),
            ]
        )
        box.run(
            [
                data("delete", 1, 30, table="md_delete", op="d", key={"id": 1}, before={"id": 1, "name": "md-row"}),
                end("delete", 1, 40, {"app.md_delete": 1}),
            ]
        )
        target = box.target("md_delete")
        qualified = f'"{database}"."{dataset}"."{target}"'
        assert con.execute(
            f"SELECT count(*) FROM {qualified}"
        ).fetchone() == ((0,) if mode == "hard" else (1,))
        if mode == "soft":
            assert con.execute(
                f"SELECT cdcf_deleted FROM {qualified}"
            ).fetchall() == [(True,)]
        assert con.execute(
            f'SELECT count(*) FROM "{database}"."{dataset}"."{target}__live"'
        ).fetchone() == (0,)
        assert con.execute(
            f'SELECT delete_mode, effect_state FROM "{database}"."{control}".delete_ledger'
        ).fetchall() == [(mode, "applied")]
    finally:
        with contextlib.suppress(Exception):
            con.execute(f'DROP SCHEMA IF EXISTS "{dataset}" CASCADE')
        box.close()
