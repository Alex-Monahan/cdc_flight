from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest


def _once_call_violations(tree: ast.AST, source_name: str) -> list[str]:
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        is_once = (
            isinstance(target, ast.Name) and target.id == "raise_alert_once"
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


def test_fingerprint_conditions_are_distinct_per_occurrence_and_idempotent(tmp_path):
    import duckdb

    from cdc_flight.config import DestinationConfig
    from cdc_flight.control_schema import ensure_control_schema
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
            lambda key: OffsetUnusable("identical malformed row", occurrence_key=key),
            "offset-row:generation-",
        ),
        (
            "run_failure",
            {"stop_reason": "error"},
            lambda _key: RuntimeError("identical runner failure"),
            "run:",
        ),
        (
            "run_incomplete",
            {"stop_reason": "max_seconds"},
            lambda _key: RuntimeError("identical incomplete run"),
            "run:",
        ),
    ]
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        for code, summary, make_exc, occurrence_prefix in cases:
            first_key = f"{occurrence_prefix}1"
            second_key = f"{occurrence_prefix}2"
            _record_run_failure_alert(
                con,
                dest=dest,
                runner_id="runner-a",
                exc=make_exc(first_key),
                summary=dict(summary),
            )
            # A repeated observation of the same occurrence is idempotent, even
            # when the boundary is visited twice by the same run.
            _record_run_failure_alert(
                con,
                dest=dest,
                runner_id="runner-a",
                exc=make_exc(first_key),
                summary=dict(summary),
            )
            # A new durable run/generation after recovery must be visible even when
            # its exception text and condition fingerprint are identical.
            _record_run_failure_alert(
                con,
                dest=dest,
                runner_id="runner-b",
                exc=make_exc(second_key),
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
