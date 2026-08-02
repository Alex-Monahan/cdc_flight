#!/usr/bin/env bash
# Manage a *project-local, disposable* PostgreSQL cluster for CDC development.
#
# Deliberately does NOT touch any Homebrew-managed cluster the developer already
# runs (e.g. `brew services start postgresql@18` on :5432). Everything lives in
# the instance data/socket paths and listens on CDC_TEST_PGPORT only.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --- configuration (override via env) ---------------------------------------
PG_VERSION="${PG_VERSION:-18}"
PGBIN="${PGBIN:-/opt/homebrew/opt/postgresql@${PG_VERSION}/bin}"
DEFAULT_PGPORT="15432"
export PGPORT="${CDC_TEST_PGPORT:-${PGPORT:-${DEFAULT_PGPORT}}}"
if ! [[ "${PGPORT}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: CDC_TEST_PGPORT/PGPORT must be numeric, got '${PGPORT}'" >&2
  exit 2
fi
if [[ "${PGPORT}" == "${DEFAULT_PGPORT}" ]]; then
  DEFAULT_PGDATA="${PROJECT_DIR}/.pgdata"
else
  DEFAULT_PGDATA="${PROJECT_DIR}/.pgdata_${PGPORT}"
fi

canonical_path() {
  local candidate="$1"
  local parent
  parent="$(dirname "${candidate}")"
  if [[ ! -d "${parent}" ]]; then
    echo "ERROR: parent directory does not exist: ${parent}" >&2
    return 2
  fi
  printf '%s/%s\n' "$(cd "${parent}" && pwd -P)" "$(basename "${candidate}")"
}

EXPECTED_PGDATA="$(canonical_path "${DEFAULT_PGDATA}")"
CONFIGURED_PGDATA="$(canonical_path "${CDC_TEST_PGDATA:-${DEFAULT_PGDATA}}")"
if [[ "${CONFIGURED_PGDATA}" != "${EXPECTED_PGDATA}" ]]; then
  echo "ERROR: refusing non-derived CDC_TEST_PGDATA '${CONFIGURED_PGDATA}'" >&2
  echo "       selected port ${PGPORT} owns only '${EXPECTED_PGDATA}'" >&2
  exit 2
fi
export PGDATA="${EXPECTED_PGDATA}"
export CDC_TEST_PGDATA="${PGDATA}"
PGSOCKET="${CDC_TEST_PGSOCKET:-${PGSOCKET:-${PGDATA}}}"
export CDC_TEST_PGSOCKET="${PGSOCKET}"
export PGHOST="${PGHOST:-127.0.0.1}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
PGDATABASE_NAME="${CDC_TEST_PGDATABASE:-${PGDATABASE:-cdc_source}}"
export PGDATABASE="${PGDATABASE_NAME}"
LOGFILE="${CDC_TEST_PGLOG:-${PGDATA}/server.log}"
export CDC_TEST_PGLOG="${LOGFILE}"
TEST_CLUSTER_SENTINEL="${PGDATA}/.cdc_flight_disposable_test_cluster"
TEST_ONLY_CONFIG="${PGDATA}/cdc_flight_test_only.conf"

if [[ ! -x "${PGBIN}/initdb" ]]; then
  echo "ERROR: PostgreSQL ${PG_VERSION} binaries not found at ${PGBIN}" >&2
  echo "       Install with: brew install postgresql@${PG_VERSION}" >&2
  echo "       Or point PGBIN at another install." >&2
  exit 1
fi

# --- commands ----------------------------------------------------------------

cmd_init() {
  if [[ -f "${PGDATA}/PG_VERSION" ]]; then
    if [[ ! -f "${TEST_CLUSTER_SENTINEL}" ]]; then
      echo "ERROR: refusing unmarked cluster at ${PGDATA}" >&2
      echo "       recreate it with this disposable test provisioner" >&2
      exit 2
    fi
    echo "Cluster already initialised at ${PGDATA} (use 'reset' to recreate)."
    return 0
  fi
  mkdir -p "${PGDATA}"
  chmod 700 "${PGDATA}"
  mkdir -p "${PGSOCKET}"
  chmod 700 "${PGSOCKET}"
  local pwfile
  pwfile="$(mktemp)"
  printf '%s' "${PGPASSWORD}" > "${pwfile}"
  echo "Initialising cluster in ${PGDATA} (postgresql@${PG_VERSION})..."
  "${PGBIN}/initdb" \
    --pgdata="${PGDATA}" \
    --username="${PGUSER}" \
    --pwfile="${pwfile}" \
    --auth-local=trust \
    --auth-host=scram-sha-256 \
    --encoding=UTF8 \
    --locale=C >/dev/null
  rm -f "${pwfile}"
  printf 'cdc_flight disposable test cluster\nport=%s\n' "${PGPORT}" \
    > "${TEST_CLUSTER_SENTINEL}"

  # Logical decoding + isolation from the developer's default cluster.
  cat >> "${PGDATA}/postgresql.conf" <<EOF

# ---- cdc_flight overrides -------------------------------------------------
listen_addresses = 'localhost'
port = ${PGPORT}
unix_socket_directories = '${PGSOCKET}'

# Logical replication (required by Debezium / pgoutput)
wal_level = logical
# Twelve xdist workers may each retain a base slot while a throwaway re-snapshot
# slot is active; provide that 24-slot worst case plus four slots of headroom.
max_replication_slots = 28
max_wal_senders = 28
max_logical_replication_workers = 4
include = 'cdc_flight_test_only.conf'

# Keep the dev cluster small & chatty enough to debug
shared_buffers = 256MB
logging_collector = off
log_line_prefix = '%m [%p] %q%u@%d '
log_min_duration_statement = 2000

# Surface TOAST + logical-decoding behaviour quickly in tests
wal_sender_timeout = 60s
EOF
  if [[ ! -f "${TEST_CLUSTER_SENTINEL}" ]]; then
    echo "ERROR: test-only settings require ${TEST_CLUSTER_SENTINEL}" >&2
    exit 2
  fi
  cat > "${TEST_ONLY_CONFIG}" <<EOF
# Generated only for the sentinel-marked disposable cluster at ${PGDATA}.
fsync = off
synchronous_commit = off
full_page_writes = off
EOF
  echo "Initialised. Data dir: ${PGDATA}"
}

cmd_start() {
  cmd_init
  mkdir -p "${PGSOCKET}"
  chmod 700 "${PGSOCKET}"
  if cmd_status >/dev/null 2>&1; then
    echo "Cluster already running on :${PGPORT}."
  else
    echo "Starting cluster on :${PGPORT}..."
    "${PGBIN}/pg_ctl" -D "${PGDATA}" -l "${LOGFILE}" -w -t 60 start
  fi
  # Create the application database if missing.
  if ! "${PGBIN}/psql" -h "${PGSOCKET}" -p "${PGPORT}" -U "${PGUSER}" -d postgres \
        -tAc "SELECT 1 FROM pg_database WHERE datname='${PGDATABASE_NAME}'" | grep -q 1; then
    echo "Creating database ${PGDATABASE_NAME}..."
    "${PGBIN}/createdb" -h "${PGSOCKET}" -p "${PGPORT}" -U "${PGUSER}" "${PGDATABASE_NAME}"
  fi
  echo "Ready: postgresql://${PGUSER}:***@${PGHOST}:${PGPORT}/${PGDATABASE_NAME}"
}

cmd_stop() {
  if [[ -f "${PGDATA}/postmaster.pid" ]]; then
    echo "Stopping cluster..."
    "${PGBIN}/pg_ctl" -D "${PGDATA}" -m fast -w -t 60 stop || true
  else
    echo "Cluster not running."
  fi
}

cmd_status() {
  "${PGBIN}/pg_ctl" -D "${PGDATA}" status
}

cmd_reset() {
  if [[ -e "${PGDATA}" && ! -f "${TEST_CLUSTER_SENTINEL}" \
        && "${CDC_TEST_RECREATE_UNMARKED_DISPOSABLE:-0}" != "1" ]]; then
    echo "ERROR: refusing to remove unmarked directory ${PGDATA}" >&2
    echo "       set CDC_TEST_RECREATE_UNMARKED_DISPOSABLE=1 only to migrate" >&2
    echo "       this canonical derived test path to the sentinel boundary" >&2
    exit 2
  fi
  cmd_stop
  echo "Removing ${PGDATA}..."
  rm -rf "${PGDATA}"
  cmd_start
}

cmd_psql() {
  exec "${PGBIN}/psql" -h "${PGSOCKET}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDATABASE_NAME}" "$@"
}

cmd_seed() {
  echo "Applying schema + seed data to ${PGDATABASE_NAME}..."
  echo "  -> 01_schema.sql + 02_seed.sql (one transaction)"
  "${PGBIN}/psql" -h "${PGSOCKET}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDATABASE_NAME}" \
    -v ON_ERROR_STOP=1 -q --single-transaction \
    -f "${PROJECT_DIR}/sql/01_schema.sql" -f "${PROJECT_DIR}/sql/02_seed.sql"
  echo "Seed complete."
}

cmd_dsn() {
  echo "postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${PGDATABASE_NAME}"
}

case "${1:-}" in
  init)   cmd_init ;;
  start)  cmd_start ;;
  stop)   cmd_stop ;;
  status) cmd_status ;;
  reset)  cmd_reset ;;
  seed)   cmd_seed ;;
  psql)   shift; cmd_psql "$@" ;;
  dsn)    cmd_dsn ;;
  *)
    echo "usage: $0 {init|start|stop|status|reset|seed|psql|dsn}" >&2
    exit 2
    ;;
esac
