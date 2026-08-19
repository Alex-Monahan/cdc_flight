from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest


def _once_call_violations(tree: ast.AST, source_name: str) -> list[str]:
    aliases = {"raise_alert_once"}
    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for imported in node.names:
                    if imported.name == "raise_alert_once":
                        alias = imported.asname or imported.name
                        if alias not in aliases:
                            aliases.add(alias)
                            changed = True
            elif isinstance(node, ast.Assign):
                value = node.value
                value_is_once = (
                    isinstance(value, ast.Name) and value.id in aliases
                ) or (
                    isinstance(value, ast.Attribute) and value.attr == "raise_alert_once"
                )
                if value_is_once:
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id not in aliases:
                            aliases.add(target.id)
                            changed = True
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                forwards_once = any(
                    isinstance(child, ast.Call)
                    and (
                        (isinstance(child.func, ast.Name) and child.func.id in aliases)
                        or (
                            isinstance(child.func, ast.Attribute)
                            and child.func.attr == "raise_alert_once"
                        )
                    )
                    and child.args
                    and all(isinstance(arg, ast.Starred) for arg in child.args)
                    and all(keyword.arg is None for keyword in child.keywords)
                    for child in ast.walk(node)
                )
                if forwards_once and node.name not in aliases:
                    aliases.add(node.name)
                    changed = True

    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        is_once = (
            isinstance(target, ast.Name) and target.id in aliases
        ) or (isinstance(target, ast.Attribute) and target.attr == "raise_alert_once")
        if not is_once:
            continue
        keywords = {keyword.arg for keyword in node.keywords if keyword.arg is not None}
        missing = {"condition_key", "occurrence_key"} - keywords
        if "marker_value" in keywords:
            missing.add("marker_value is forbidden")
        for keyword in node.keywords:
            if (
                keyword.arg == "occurrence_key"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value is None
            ):
                missing.add("occurrence_key cannot be None")
        if missing:
            violations.append(
                f"{source_name}:{node.lineno}: missing {', '.join(sorted(missing))}"
            )
    return violations


def test_every_raise_alert_once_call_has_condition_and_occurrence_components():
    source_root = Path(__file__).parents[2] / "src"
    violations = []
    for path in sorted(source_root.rglob("*.py")):
        violations.extend(
            _once_call_violations(
                ast.parse(path.read_text(encoding="utf-8")), str(path)
            )
        )
    assert violations == []


def test_occurrence_guard_rejects_a_condition_only_call_site():
    bad_source = """
sink.raise_alert_once(
    severity='critical', code='run_failure', message='failed',
    condition_key='run_failure:fingerprint',
)
"""
    violations = _once_call_violations(ast.parse(bad_source), "synthetic.py")
    assert violations == ["synthetic.py:2: missing occurrence_key"]


def test_raise_alert_once_requires_occurrence_at_runtime():
    # `destination_alerts` is a state-owner split that expects the stable public
    # destination module to have completed its re-export cycle first.
    from cdc_flight import destination as _destination  # noqa: F401
    from cdc_flight.destination_alerts import raise_alert_once

    signature = inspect.signature(raise_alert_once)
    assert signature.parameters["condition_key"].default is inspect.Parameter.empty
    assert signature.parameters["occurrence_key"].default is inspect.Parameter.empty
    with pytest.raises(TypeError, match="occurrence_key"):
        raise_alert_once(
            None,
            pipeline="p",
            severity="critical",
            code="run_failure",
            message="failed",
            condition_key="run_failure:fingerprint",
        )


def test_occurrence_key_has_no_text_constructor_or_string_factory():
    from cdc_flight.destination import (
        EpisodeState,
        OccurrenceKey,
        RecoveryGeneration,
        RunState,
        SlotState,
    )

    with pytest.raises(TypeError, match="opaque"):
        OccurrenceKey("constant")

    raw = "exception-derived-text"
    factories = (
        lambda: OccurrenceKey.from_episode(raw),
        lambda: OccurrenceKey.from_recovery_generation(raw),
        lambda: OccurrenceKey.from_run(raw),
        lambda: OccurrenceKey.from_slot_state(raw),
    )
    for factory in factories:
        with pytest.raises(TypeError):
            factory()

    # The accepted inputs are state owners, not strings.  These objects are the
    # same shapes used by the production paths below.
    assert str(OccurrenceKey.from_episode(EpisodeState("p", 1, "dark"))) == "episode:1"
    assert str(
        OccurrenceKey.from_recovery_generation(
            RecoveryGeneration("p", "main", "generation-1", "slot_missing")
        )
    ) == "recovery:generation-1"
    assert str(OccurrenceKey.from_run(RunState.new("p"))).startswith("run:")
    assert str(
        OccurrenceKey.from_slot_state(SlotState("slot_missing", "slot"))
    ).startswith("slot-state:")


def test_runtime_type_gate_closes_every_alias_and_raw_value_defeat():
    import hashlib

    from cdc_flight import destination as alerts
    from cdc_flight.destination import raise_alert_once as aliased_once

    def forwarding_wrapper(*args, **kwargs):
        return aliased_once(*args, **kwargs)

    aliased_wrapper = forwarding_wrapper

    def invoke(function, occurrence):
        return function(
            None,
            pipeline="p",
            severity="critical",
            code="run_failure",
            message="identical failure",
            condition_key="run_failure:fingerprint",
            occurrence_key=occurrence,
        )

    raw_values = [hashlib.sha256(b"identical failure").hexdigest(), "constant", None]
    for function in (alerts.raise_alert_once, aliased_once, aliased_wrapper):
        for raw in raw_values:
            with pytest.raises(TypeError, match="OccurrenceKey"):
                invoke(function, raw)

    with pytest.raises(TypeError):
        alerts.raise_alert_once(None, "p", "critical")
    with pytest.raises(TypeError):
        alerts.raise_alert_once(
            None,
            pipeline="p",
            severity="critical",
            code="run_failure",
            message="failed",
            condition_key="run_failure:fingerprint",
        )


def test_ast_backstop_resolves_import_and_wrapper_aliases():
    source = """
from cdc_flight.destination import raise_alert_once as aliased_once

def forwarding_wrapper(*args, **kwargs):
    return aliased_once(*args, **kwargs)

aliased_wrapper = forwarding_wrapper
aliased_wrapper(severity='critical', code='run_failure', message='failed')
"""
    violations = _once_call_violations(ast.parse(source), "aliases.py")
    assert len(violations) == 2
    assert all("condition_key" in item and "occurrence_key" in item for item in violations)


def test_fingerprint_conditions_are_distinct_per_occurrence_and_idempotent(tmp_path):
    import duckdb

    from cdc_flight.config import DestinationConfig
    from cdc_flight.control_schema import ensure_control_schema
    from cdc_flight.destination import OffsetRowState, RunState
    from cdc_flight.errors import OffsetUnusable
    from cdc_flight.pipeline import _record_run_failure_alert

    dest = DestinationConfig(
        kind="duckdb",
        pipeline_name="fingerprint-occurrences",
        duckdb_path=tmp_path / "dest.duckdb",
    )
    cases = [
        (
            "offset_unusable",
            {"stop_reason": "error"},
            lambda row: OffsetUnusable("identical malformed row", offset_row=row),
        ),
        (
            "run_failure",
            {"stop_reason": "error"},
            lambda _key: RuntimeError("identical runner failure"),
        ),
        (
            "run_incomplete",
            {"stop_reason": "max_seconds"},
            lambda _key: RuntimeError("identical incomplete run"),
        ),
    ]
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        for code, summary, make_exc in cases:
            first_state = OffsetRowState(
                pipeline=dest.pipeline_name,
                namespace="main",
                resume_json="not-json",
                commit_id=1,
                snapshot_epoch=1,
                last_lsn=11,
                updated_at=None,
            )
            second_state = OffsetRowState(
                pipeline=dest.pipeline_name,
                namespace="main",
                resume_json="not-json",
                commit_id=2,
                snapshot_epoch=1,
                last_lsn=12,
                updated_at=None,
            )
            first_run = RunState.new(dest.pipeline_name)
            second_run = RunState.new(dest.pipeline_name)
            _record_run_failure_alert(
                con,
                dest=dest,
                run_state=first_run,
                exc=make_exc(first_state),
                summary=dict(summary),
            )
            # A repeated observation of the same occurrence is idempotent, even
            # when the boundary is visited twice by the same run.
            _record_run_failure_alert(
                con,
                dest=dest,
                run_state=first_run,
                exc=make_exc(first_state),
                summary=dict(summary),
            )
            # A new durable run/generation after recovery must be visible even when
            # its exception text and condition fingerprint are identical.
            _record_run_failure_alert(
                con,
                dest=dest,
                run_state=second_run,
                exc=make_exc(second_state),
                summary=dict(summary),
            )

            contexts = [
                __import__("json").loads(row[0])
                for row in con.execute(
                    'SELECT context FROM "_cdc_flight".alerts '
                    "WHERE pipeline = ? AND code = ? ORDER BY raised_at",
                    [dest.pipeline_name, code],
                ).fetchall()
            ]
            assert len(contexts) == 2, (code, contexts)
            assert contexts[0]["condition_key"] == contexts[1]["condition_key"]
            assert contexts[0]["occurrence_key"] != contexts[1]["occurrence_key"]
            assert contexts[0]["alert_identity"] != contexts[1]["alert_identity"]
    finally:
        con.close()
