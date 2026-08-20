from __future__ import annotations

import os
import subprocess
import sys

import pytest

_LOCAL_RESOLVE = r'''
from cdc_flight.config import DestinationConfig
from cdc_flight.destination import connect, ensure_control_schema, ensure_dataset
import sys

dest = DestinationConfig(
    kind="duckdb",
    pipeline_name="physical-identity-proof",
    duckdb_path=sys.argv[1],
    dataset_name="cdc_raw",
    control_schema="_cdc_flight",
)
con = connect(dest)
try:
    ensure_control_schema(con, dest.control_schema)
    ensure_dataset(con, dest.dataset_name)
    print(dest.resolve_physical_lease_key(con), flush=True)
finally:
    con.close()
'''

_MOTHERDUCK_LEASE = r'''
import sys
import time
from cdc_flight.config import DestinationConfig
from cdc_flight.destination import connect, ensure_control_schema, ensure_dataset
from cdc_flight.destination_lease import Lease
from cdc_flight.errors import LeaseLost

role, database, dataset, control = sys.argv[1:5]
dest = DestinationConfig(
    kind="motherduck",
    pipeline_name="physical-identity-proof",
    motherduck_database=database,
    dataset_name=dataset,
    control_schema=control,
)
con = connect(dest)
try:
    ensure_control_schema(con, dest.control_schema)
    ensure_dataset(con, dest.dataset_name)
    key = dest.resolve_physical_lease_key(con)
    try:
        Lease(key, owner_id=role, ttl_seconds=30, control_schema=dest.control_schema).acquire(con)
    except LeaseLost:
        print("LEASE_LOST " + key, flush=True)
    else:
        print("ACQUIRED " + key, flush=True)
        if role == "holder":
            time.sleep(6)
finally:
    con.close()
'''


def _child_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    return env


def test_local_path_aliases_resolve_to_one_lease_key_in_two_processes(tmp_path):
    """Real processes prove inode identity across relative/absolute/symlink paths."""
    real = tmp_path / "destination.duckdb"
    alias = tmp_path / "Alias.duckdb"
    (tmp_path / "nested").mkdir()
    alias.symlink_to(real)
    first = subprocess.run(
        [sys.executable, "-c", _LOCAL_RESOLVE, str(real)],
        capture_output=True,
        text=True,
        env=_child_env(),
        check=True,
    )
    second = subprocess.run(
        [
            sys.executable,
            "-c",
            _LOCAL_RESOLVE,
            str(tmp_path / "nested" / ".." / alias.name),
        ],
        capture_output=True,
        text=True,
        env=_child_env(),
        check=True,
    )
    assert first.stdout.strip() == second.stdout.strip(), (first, second)
    assert '"file":"inode:' in first.stdout


@pytest.mark.motherduck
def test_motherduck_aliases_share_one_lease_and_duplicate_is_refused():
    from cdc_flight.config import motherduck_token
    from cdc_flight.naming import quote

    token = motherduck_token()
    if not token:
        pytest.skip("motherduck_token is not set")
    import uuid

    import duckdb

    database = f"cdc_alias_identity_{uuid.uuid4().hex[:10]}"
    env = _child_env()
    env["motherduck_token"] = token
    holder = None
    try:
        holder = subprocess.Popen(
            [
                sys.executable,
                "-c",
                _MOTHERDUCK_LEASE,
                "holder",
                f'  "{database.upper()}"  ',
                '  "MiXeD_Data"  ',
                '  "MiXeD_Control"  ',
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        assert holder.stdout is not None
        holder_line = holder.stdout.readline().strip()
        if not holder_line:
            remaining = holder.stdout.read()
            pytest.fail(
                f"holder exited before publishing its lease: rc={holder.poll()} "
                f"output={remaining!r}"
            )
        assert holder_line.startswith("ACQUIRED destination:"), holder_line

        contender = subprocess.run(
            [
                sys.executable,
                "-c",
                _MOTHERDUCK_LEASE,
                "contender",
                database.lower(),
                "mixed_data",
                "mixed_control",
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )
        assert contender.returncode == 0, contender.stderr
        assert contender.stdout.startswith("LEASE_LOST destination:"), contender.stdout
        assert contender.stdout.strip().split(" ", 1)[1] == holder_line.split(" ", 1)[1]
    finally:
        if holder is not None and holder.poll() is None:
            holder.terminate()
            holder.wait(timeout=20)
        cleanup = duckdb.connect(f"md:?motherduck_token={token}")
        try:
            cleanup.execute(f"DROP DATABASE IF EXISTS {quote(database)}")
        finally:
            cleanup.close()
        verify = duckdb.connect(f"md:?motherduck_token={token}")
        try:
            names = {str(row[0]) for row in verify.execute("SHOW DATABASES").fetchall()}
        finally:
            verify.close()
        assert database not in names, f"MotherDuck scratch database left behind: {database}"
