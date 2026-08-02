"""Regression guards for the native test infrastructure itself."""

from __future__ import annotations

import fcntl
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import conftest
import pytest


def test_run_lock_takes_over_stale_metadata_after_kernel_release(tmp_path: Path):
    lock_path = tmp_path / "instance.lock"
    lock_path.write_text('{"pid": 999999, "run_uid": "crashed"}\n')

    handle = conftest._acquire_test_run_lock(
        lock_path, run_uid="replacement", wait_seconds=0
    )
    try:
        metadata = json.loads(lock_path.read_text())
        assert metadata["run_uid"] == "replacement"
        assert metadata["instance_id"] == conftest.TEST_INSTANCE_ID
    finally:
        fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()


def test_run_lock_never_takes_over_a_live_kernel_owner(tmp_path: Path):
    lock_path = tmp_path / "instance.lock"
    owner = lock_path.open("a+")
    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(TimeoutError, match="timed out waiting"):
            conftest._acquire_test_run_lock(
                lock_path, run_uid="intruder", wait_seconds=0
            )
    finally:
        fcntl.flock(owner, fcntl.LOCK_UN)
        owner.close()


def test_run_and_setup_locks_are_distinct_and_instance_scoped():
    assert conftest.TEST_LOCK_PATH != conftest.TEST_SETUP_LOCK_PATH
    assert conftest.TEST_INSTANCE_ID in conftest.TEST_LOCK_PATH.name
    assert conftest.TEST_INSTANCE_ID in conftest.TEST_SETUP_LOCK_PATH.name


def test_test_source_rejects_a_remote_pghost(monkeypatch):
    monkeypatch.setenv("PGHOST", "db.example.invalid")

    with pytest.raises(pytest.fail.Exception, match="refused non-local"):
        conftest._isolated_source("postgres")


def test_provisioner_refuses_a_non_derived_data_directory(tmp_path: Path):
    env = {
        **os.environ,
        "CDC_TEST_PGPORT": str(conftest.TEST_PGPORT),
        "CDC_TEST_PGDATA": str(tmp_path / "arbitrary-pgdata"),
    }

    proc = subprocess.run(
        [str(conftest.PG_SH), "init"],
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
    )

    assert proc.returncode == 2
    assert "refusing non-derived CDC_TEST_PGDATA" in proc.stderr


def test_destructive_guard_requires_the_provisioner_sentinel(monkeypatch, tmp_path):
    monkeypatch.setattr(conftest, "TEST_PGDATA", tmp_path)
    monkeypatch.setattr(
        conftest,
        "TEST_CLUSTER_SENTINEL",
        tmp_path / ".cdc_flight_disposable_test_cluster",
    )
    monkeypatch.setattr(conftest, "_VERIFIED_CLUSTER_IDENTITY", None)
    source = conftest.SourceConfig(host="127.0.0.1", port=conftest.TEST_PGPORT)

    with pytest.raises(RuntimeError, match="missing"):
        conftest._require_disposable_cluster(source)


def test_replication_budget_covers_base_resnapshot_and_headroom():
    assert conftest._required_replication_capacity(12) == 28


def test_xdist_worker_restarts_are_disabled():
    config = SimpleNamespace(
        option=SimpleNamespace(numprocesses=12, maxworkerrestart=None)
    )

    conftest._enforce_no_worker_restarts(config)

    assert config.option.maxworkerrestart == 0


def test_xdist_worker_restart_override_is_refused():
    config = SimpleNamespace(
        option=SimpleNamespace(numprocesses=12, maxworkerrestart="1")
    )

    with pytest.raises(pytest.UsageError, match="requires --max-worker-restart=0"):
        conftest._enforce_no_worker_restarts(config)


@pytest.mark.parametrize(
    "name",
    [
        f"{conftest.WORKER_DATABASE_PREFIX}gw99",
        f"{conftest.TEMPLATE_DATABASE_PREFIX}gw99",
    ],
)
def test_stale_database_contract_covers_replacement_worker_artifacts(name):
    assert conftest._owned_database_name(name)


def test_namespaced_base_and_resnapshot_slots_share_the_sweep_prefix():
    base = f"{conftest.TEST_SLOT_PREFIX}t_crashed_999"
    assert base.startswith(conftest.TEST_SLOT_PREFIX)
    assert f"{base}_rs".startswith(conftest.TEST_SLOT_PREFIX)


def test_probe_output_roots_are_disjoint_per_instance(monkeypatch):
    monkeypatch.syspath_prepend(str(conftest.PROJECT_DIR))
    from probes._common import probe_output_root

    assert probe_output_root("pg15432") != probe_output_root("pg15436")


@pytest.mark.parametrize("requested", [6, 10])
def test_root_runner_preserves_explicit_proof_windows(monkeypatch, requested):
    observed: list[float] = []

    def fake_invoke(_env, **kwargs):
        observed.append(kwargs["idle_seconds"])
        return {"late_failure_observed": kwargs["idle_seconds"] >= requested}

    monkeypatch.setattr(conftest, "_invoke_pipeline", fake_invoke)
    runner = conftest.run_pipeline.__wrapped__({})

    summary = runner(idle_seconds=requested)

    assert observed == [requested]
    assert summary["late_failure_observed"], (
        "the fixture stopped before the caller's historical proof window"
    )


def test_sandbox_keeps_the_historical_six_second_default(monkeypatch, tmp_path: Path):
    observed: list[float] = []

    def fake_invoke(_env, **kwargs):
        observed.append(kwargs["idle_seconds"])
        return {}

    monkeypatch.setattr(conftest, "_invoke_pipeline", fake_invoke)
    sandbox = object.__new__(conftest.Sandbox)
    sandbox.env = {"CDC_STATE_DIR": str(tmp_path)}

    sandbox.run()

    assert observed == [6]
