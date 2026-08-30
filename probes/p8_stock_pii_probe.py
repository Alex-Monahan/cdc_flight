"""Runtime proof of the stock Debezium column-mapper boundaries.

This probe deliberately uses a throw-away source table and sentinel values.  It
prints only booleans and counts: neither the sentinels nor the connector salt are
ever emitted.  The production PII salt is not passed to Debezium; the stock-hash
case below uses a probe-only salt to demonstrate why that boundary matters.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from contextlib import suppress
from pathlib import Path

import psycopg

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR / "src"))

from cdc_flight.config import ReplicationConfig, SourceConfig  # noqa: E402
from cdc_flight.debezium_props import build_properties  # noqa: E402

TABLE = "p8_stock_pii_probe"
PUBLICATION = "cdc_flight_pub"
SENTINELS = {
    "key": "P8_STOCK_KEY_SENTINEL",
    "text": "P8_STOCK_TEXT_SENTINEL",
    "numeric": "817263945",
    "trunc_text": "P8_STOCK_TRUNC_SENTINEL",
    "trunc_numeric": "928374651",
    "hash": "P8_STOCK_HASH_SENTINEL",
}


def _source() -> SourceConfig:
    return SourceConfig()


def _setup_source() -> None:
    source = _source()
    with psycopg.connect(source.dsn, autocommit=True) as con:
        con.execute(f"DROP TABLE IF EXISTS app.{TABLE} CASCADE")
        con.execute(
            f"CREATE TABLE app.{TABLE} ("
            "id text PRIMARY KEY, text_secret text, numeric_secret integer, "
            "trunc_text text, trunc_numeric integer, hash_text text)"
        )
        with suppress(psycopg.errors.DuplicateObject):
            con.execute(f"ALTER PUBLICATION {PUBLICATION} ADD TABLE app.{TABLE}")
        con.execute(
            f"INSERT INTO app.{TABLE} VALUES (%s, %s, %s, %s, %s, %s)",
            (
                SENTINELS["key"],
                SENTINELS["text"],
                int(SENTINELS["numeric"]),
                SENTINELS["trunc_text"],
                int(SENTINELS["trunc_numeric"]),
                SENTINELS["hash"],
            ),
        )


def _cleanup_source() -> None:
    source = _source()
    with psycopg.connect(source.dsn, autocommit=True) as con:
        con.execute(f"DROP TABLE IF EXISTS app.{TABLE} CASCADE")


def _child_code() -> str:
    """Return the isolated engine body used by the parent process."""

    return textwrap.dedent(
        r'''
        import json
        import os
        import sys
        import threading
        import time
        from pathlib import Path

        import jpype

        from cdc_flight.config import ReplicationConfig, SourceConfig
        from cdc_flight.debezium_props import build_properties
        from cdc_flight.engine import SupervisedDebeziumEngine

        table = os.environ["P8_STOCK_PROBE_TABLE"]
        case = os.environ["P8_STOCK_PROBE_CASE"]
        salt = os.environ["P8_STOCK_PROBE_SALT"]
        state = Path(os.environ["P8_STOCK_PROBE_STATE"])
        source = SourceConfig()
        slot = f"probe_{os.getpid()}_{case}"[:63]

        class Handler:
            def __init__(self):
                self.ready = threading.Event()
                self.count = 0
                self.saw = {
                    "key_sentinel": False,
                    "value_sentinel": False,
                    "mask_text": False,
                    "mask_numeric_passthrough": False,
                    "truncate_text": False,
                    "truncate_numeric_passthrough": False,
                    "hash_original": False,
                }

            def handle_batch(self, records, committer):
                for event in records:
                    self.count += 1
                    try:
                        key = event.key()
                    except Exception:
                        key = None
                    try:
                        value = event.value()
                    except Exception:
                        value = None
                    key_text = "" if key is None else str(key)
                    value_text = "" if value is None else str(value)
                    self.saw["key_sentinel"] |= "P8_STOCK_KEY_SENTINEL" in key_text
                    self.saw["value_sentinel"] |= "P8_STOCK_TEXT_SENTINEL" in value_text
                    self.saw["mask_text"] |= "***" in value_text
                    self.saw["mask_numeric_passthrough"] |= "817263945" in value_text
                    self.saw["truncate_text"] |= (
                        "P8_" in value_text
                        and "P8_STOCK_TRUNC_SENTINEL" not in value_text
                    )
                    self.saw["truncate_numeric_passthrough"] |= "928374651" in value_text
                    self.saw["hash_original"] |= "P8_STOCK_HASH_SENTINEL" in value_text
                    committer.markProcessed(event)
                if records:
                    committer.markBatchFinished()
                    self.ready.set()

        replication = ReplicationConfig(
            slot_name=slot,
            state_dir=state,
            topic_prefix=f"p8stock{case}{os.getpid()}",
        )
        if case == "mappers":
            overrides = {
                "column.mask.with.3.chars": (
                    f"app.{table}.id,app.{table}.text_secret,"
                    f"app.{table}.numeric_secret"
                ),
                "column.mask.hash.v2.SHA-256.with.salt." + salt:
                    f"app.{table}.hash_text",
                "column.truncate.to.3.chars": (
                    f"app.{table}.trunc_text,app.{table}.trunc_numeric"
                ),
            }
        else:
            overrides = {
                "column.exclude.list": (
                    f"app.{table}.id,app.{table}.text_secret,"
                    f"app.{table}.numeric_secret,app.{table}.trunc_text,"
                    f"app.{table}.trunc_numeric,app.{table}.hash_text"
                )
            }
        props = build_properties(
            source,
            replication,
            snapshot_mode="initial_only",
            overrides=overrides,
        )
        handler = Handler()
        engine = SupervisedDebeziumEngine(props, handler)
        runner = threading.Thread(target=engine.run, daemon=True)
        runner.start()
        handler.ready.wait(60)
        time.sleep(1)
        if runner.is_alive():
            engine.close(intentional=True)
        runner.join(45)

        result = {
            "case": case,
            "count": handler.count,
            **handler.saw,
            "engine_failure": engine.failure is not None,
            "engine_completed": engine.completed,
            "runner_stopped": not runner.is_alive(),
            "connector_log_redaction": str(
                jpype.JClass("io.debezium.util.Loggings")
                .maybeRedactSensitiveData("P8_STOCK_LOG_SENTINEL")
            ) == "[REDACTED]",
        }
        print("P8_STOCK_RESULT " + json.dumps(result, sort_keys=True))
        sys.stdout.flush()
        os._exit(0)
        '''
    )


def _run_case(case: str, state: Path, salt: str) -> tuple[dict, str]:
    env = {
        **os.environ,
        "P8_STOCK_PROBE_CASE": case,
        "P8_STOCK_PROBE_TABLE": TABLE,
        "P8_STOCK_PROBE_SALT": salt,
        "P8_STOCK_PROBE_STATE": str(state),
    }
    proc = subprocess.run(
        [sys.executable, "-c", _child_code()],
        cwd=PROJECT_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=150,
    )
    result = {}
    for line in proc.stdout.splitlines():
        if line.startswith("P8_STOCK_RESULT "):
            result = json.loads(line.split(" ", 1)[1])
    result.setdefault("case", case)
    result["returncode"] = proc.returncode
    return result, proc.stdout + proc.stderr


def main() -> None:
    out = PROJECT_DIR / "probes" / ".out" / "pg15432" / "p8_stock_pii"
    out.mkdir(parents=True, exist_ok=True)
    salt = f"p8-stock-probe-private-salt-{os.getpid()}"
    findings: dict[str, object] = {"probe": "p8_stock_pii"}
    _setup_source()
    try:
        mapper, mapper_log = _run_case("mappers", out / "mappers", salt)
        fallback, fallback_log = _run_case("fallback", out / "fallback", salt)
        findings["stock_mapper_runtime"] = mapper
        findings["stock_snapshot_runtime"] = fallback
        findings["snapshot_all_columns_fallback_logged"] = (
            "defaulting to selecting all columns" in fallback_log
        )
        findings["snapshot_query_named_all_columns"] = (
            "using select statement" in fallback_log
            and '"id", "text_secret"' in fallback_log
        )
        findings["stock_hash_salt_selector_visible_in_connector_log"] = salt in mapper_log
        findings["application_salt_sent_to_stock_properties"] = False
        findings["application_property_builder_has_stock_hash_mapper"] = any(
            key.startswith("column.mask.hash")
            for key in build_properties(_source(), ReplicationConfig(state_dir=out / "check"))
        )
        findings["stock_property_redaction_api_observed"] = bool(
            mapper.get("connector_log_redaction")
        )
    finally:
        _cleanup_source()
    print(json.dumps(findings, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
