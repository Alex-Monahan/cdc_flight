"""Round 1: durable, identity-bearing per-table history-mode admission."""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest

from cdc_flight import destination as dest_mod
from cdc_flight import history_policy, table_lifecycle
from cdc_flight.catalog_state import SourceRelation
from cdc_flight.planner import GroupPlan

PIPELINE = "history-policy-test"
TARGET = "cdcflight_app_customers"
_UNSET = object()


@pytest.fixture
def destination(tmp_path):
    con = duckdb.connect(str(tmp_path / "destination.duckdb"))
    dest_mod.ensure_control_schema(con)
    try:
        yield con
    finally:
        con.close()


def _relation(
    table: str = "customers",
    *,
    schema: str = "app",
    keys: tuple[str, ...] = ("id",),
    partition: bool = False,
) -> SourceRelation:
    return SourceRelation(
        schema=schema,
        table=table,
        oid=101,
        published=True,
        replica_identity="d",
        primary_key_columns=keys,
        is_partition=partition,
    )


def _admit(con, relation=_UNSET, *, mode: str = "scd2", target: str = TARGET):
    return history_policy.admit_history_mode(
        con,
        pipeline=PIPELINE,
        source_relation=_relation() if relation is _UNSET else relation,
        qualified_table="app.customers",
        mode=mode,
        target_table=target,
        dataset="cdc_raw",
    )


def test_qualified_parser_preserves_quoted_identity_and_folds_unquoted_spelling():
    assert history_policy.parse_qualified_table(" APP.Customers ") == (
        "app",
        "customers",
    )
    assert history_policy.parse_qualified_table('"App"."Customer Name"') == (
        "App",
        "Customer Name",
    )
    assert history_policy.parse_qualified_table('"app"."customer""s"') == (
        "app",
        'customer"s',
    )


def test_unqualified_or_incomplete_identity_is_refused():
    for value in ("customers", "", "app.", ".customers", '"app"'):
        with pytest.raises(history_policy.HistoryPolicyRefused):
            history_policy.parse_qualified_table(value)


def test_extra_or_injected_qualification_is_refused():
    for value in (
        "app.customers.extra",
        "app.customers; DROP TABLE app.customers",
        "app..customers",
        '"app.customers"',
        'app."customers trailing" extra',
    ):
        with pytest.raises(history_policy.HistoryPolicyRefused):
            history_policy.parse_qualified_table(value)


def test_only_none_and_scd2_are_admissible_history_modes():
    assert history_policy.parse_history_mode(" NONE ") == "none"
    assert history_policy.parse_history_mode("ScD2") == "scd2"
    for value in ("current", "history", "", None, 2):
        with pytest.raises(history_policy.HistoryPolicyRefused):
            history_policy.parse_history_mode(value)


def test_unknown_ambiguous_partition_and_keyless_source_identities_are_refused(destination):
    identities = (
        None,
        _relation(keys=()),
        _relation(partition=True),
        SimpleNamespace(schema="other", table="customers", primary_key_columns=("id",)),
    )
    for relation in identities:
        with pytest.raises(history_policy.HistoryPolicyRefused):
            _admit(destination, relation, target="cdcflight_app_rejected")


def test_first_admission_commits_one_scoped_durable_policy_and_defaults_peer_to_none(
    destination, tmp_path
):
    result = _admit(destination)
    assert result.as_dict() == {
        "qualified_table": "app.customers",
        "history_mode": "scd2",
        "previous_history_mode": "none",
        "snapshot_state": "none",
        "target_table": TARGET,
        "changed": True,
        "policy_authority": "table_state.history_mode",
        "transaction_scope": "one_destination_transaction",
    }
    destination.close()
    fresh = duckdb.connect(str(tmp_path / "destination.duckdb"), read_only=True)
    try:
        assert fresh.execute(
            "SELECT history_mode FROM _cdc_flight.table_state "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [PIPELINE, "app", "customers"],
        ).fetchone() == ("scd2",)
        assert history_policy.read_history_mode(
            fresh,
            pipeline=PIPELINE,
            source_schema="app",
            source_table="orders",
        ) == "none"
        assert fresh.execute(
            "SELECT count(*) FROM _cdc_flight.table_state WHERE pipeline = ?", [PIPELINE]
        ).fetchone() == (1,)
    finally:
        fresh.close()


def test_admission_is_idempotent_and_downgrade_is_refused(destination):
    first = _admit(destination)
    second = _admit(destination)
    assert first.changed is True
    assert second.changed is False
    assert second.previous_mode == "scd2"
    with pytest.raises(history_policy.HistoryPolicyRefused, match="downgrade"):
        _admit(destination, mode="none")
    assert history_policy.read_history_mode(
        destination,
        pipeline=PIPELINE,
        source_schema="app",
        source_table="customers",
    ) == "scd2"


def test_existing_current_only_target_is_a_safe_refusal(destination):
    dest_mod.ensure_dataset(destination, "cdc_raw")
    destination.execute(f'CREATE TABLE "cdc_raw"."{TARGET}" (id INTEGER)')
    with pytest.raises(history_policy.HistoryPolicyRefused, match="current-only"):
        _admit(destination)
    assert table_lifecycle.read(
        destination,
        pipeline=PIPELINE,
        source_schema="app",
        source_table="customers",
    ) == table_lifecycle.ABSENT
    assert history_policy.read_history_mode(
        destination,
        pipeline=PIPELINE,
        source_schema="app",
        source_table="customers",
    ) == "none"


def test_replace_lifecycle_preserves_the_selected_history_policy(destination):
    _admit(destination)
    table_lifecycle.transition(
        destination,
        pipeline=PIPELINE,
        source_schema="app",
        source_table="customers",
        to=table_lifecycle.IN_PROGRESS,
        reason="begin snapshot",
        target_table="cdcflight_app_customers_new",
        replace=True,
    )
    assert destination.execute(
        "SELECT target_table, snapshot_state, history_mode, refresh_mode, delete_mode "
        "FROM _cdc_flight.table_state WHERE pipeline = ? AND source_table = ?",
        [PIPELINE, "customers"],
    ).fetchone() == (
        "cdcflight_app_customers_new",
        "in_progress",
        "scd2",
        "cdc",
        "hard",
    )


def test_planner_reads_durable_policy_without_a_manual_history_override(destination):
    _admit(destination)
    plan = GroupPlan(
        destination,
        commit_id=1,
        registry_of=lambda: None,
        snapshots=None,
        spill=None,
        truncate_mode="replicate",
        created_in_txn=set(),
        pipeline=PIPELINE,
    )
    assert plan._history_mode_for("app.customers") == "scd2"
    assert plan._history_mode_for("app.orders") == "none"
    assert not hasattr(plan, "history_modes")


def test_policy_operation_creates_no_bundle_or_dispatch_side_effect(destination):
    _admit(destination)
    assert destination.execute("SELECT count(*) FROM _cdc_flight.scd2_bundles").fetchone() == (0,)
    assert destination.execute("SELECT count(*) FROM _cdc_flight.run_logs").fetchone() == (0,)
    assert history_policy.HISTORY_POLICY_AUTHORITY == "table_state.history_mode"
