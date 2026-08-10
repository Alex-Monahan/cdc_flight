"""FIX ROUND 9 corpus proof against the real MotherDuck destination."""

from __future__ import annotations

import os

import pytest
from support.fix9_opaque import (
    EXACT_CORPUS,
    UNDELIVERABLE_TEXT_TYPES,
    capture_environment,
    create_corpus,
    drop_corpus,
    populate_corpus,
    source_connector_text,
)
from support.fixtures import Sandbox
from support.motherduck_probe import connect

pytestmark = [pytest.mark.motherduck, pytest.mark.slow, pytest.mark.e2e]


def _md_rows(con, database: str, dataset: str, name: str) -> list[tuple]:
    return con.execute(
        f'SELECT "id", "value" FROM "{database}"."{dataset}".'
        f'"cdcflight_app_p2b_r9_{name}" ORDER BY "id"'
    ).fetchall()


def test_postgresql_generated_opaque_corpus_is_lossless_or_refused_on_motherduck(
    tmp_path, postgres_cluster, motherduck_case
):
    sandbox = Sandbox("fix9_md_generated", tmp_path / "sandbox", postgres_cluster)
    database = motherduck_case["database"]
    dataset = motherduck_case["dataset"]
    token = motherduck_case["token"]
    env = {
        **capture_environment([]),
        "CDC_DATASET": dataset,
        "CDC_MD_DATABASE": database,
        "CDC_CONTROL_SCHEMA": motherduck_case["control_schema"],
        "MOTHERDUCK_TOKEN": token,
        "motherduck_token": token,
    }
    con = None
    try:
        assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
        sandbox.reseed()
        tables = create_corpus(sandbox, EXACT_CORPUS)
        env.update(capture_environment(tables))
        try:
            baseline = sandbox.run(
                destination="motherduck",
                reset_state=True,
                extra_env=env,
                max_seconds=240,
                timeout=420,
            )
            assert baseline["ok"] is True, baseline
            populate_corpus(sandbox)
            for _attempt in range(7):
                result = sandbox.run(
                    destination="motherduck",
                    extra_env=env,
                    max_seconds=240,
                    timeout=420,
                    expect_success=False,
                )
                assert result["ok"] is False, result

            con = connect(token, database)
            control = motherduck_case["control_schema"]
            for name in (*UNDELIVERABLE_TEXT_TYPES, "int2vector"):
                assert con.execute(
                    f'SELECT state FROM "{database}"."{control}"."schema_refusals" '
                    "WHERE source_schema='app' AND source_table=?",
                    [f"p2b_r9_{name}"],
                ).fetchall() == [("quarantined",)]
                assert con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema='{dataset}' AND table_name=?",
                    [f"cdcflight_app_p2b_r9_{name}"],
                ).fetchall() == []
            for name in EXACT_CORPUS:
                if name == "int2vector" or name in UNDELIVERABLE_TEXT_TYPES:
                    continue
                source = source_connector_text(sandbox, name)
                destination = _md_rows(con, database, dataset, name)
                assert destination == source, name
        finally:
            drop_corpus(sandbox, EXACT_CORPUS)
    finally:
        if con is not None:
            con.execute(f'DROP SCHEMA IF EXISTS "{dataset}" CASCADE')
            con.close()
        sandbox.cleanup()
