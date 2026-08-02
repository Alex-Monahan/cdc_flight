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
export PGDATA="${CDC_TEST_PGDATA:-${PGDATA:-${DEFAULT_PGDATA}}}"
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

if [[ ! -x "${PGBIN}/initdb" ]]; then
  echo "ERROR: PostgreSQL ${PG_VERSION} binaries not found at ${PGBIN}" >&2
  echo "       Install with: brew install postgresql@${PG_VERSION}" >&2
  echo "       Or point PGBIN at another install." >&2
  exit 1
fi

# --- commands ----------------------------------------------------------------

cmd_init() {
  if [[ -f "${PGDATA}/PG_VERSION" ]]; then
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

  # Logical decoding + isolation from the developer's default cluster.
  cat >> "${PGDATA}/postgresql.conf" <<EOF

# ---- cdc_flight overrides -------------------------------------------------
listen_addresses = 'localhost'
port = ${PGPORT}
unix_socket_directories = '${PGSOCKET}'

# Logical replication (required by Debezium / pgoutput)
wal_level = logical
# Twelve xdist workers need at most one active slot/sender each; leave four
# spare slots/senders for throwaway resnapshot/recovery connections.
max_replication_slots = 16
max_wal_senders = 16
max_logical_replication_workers = 4

# This cluster is disposable test infrastructure. Keep logical decoding enabled,
# but avoid durability work whose only consumer is a throwaway test database.
fsync = off
synchronous_commit = off
full_page_writes = off

# Keep the dev cluster small & chatty enough to debug
shared_buffers = 256MB
logging_collector = off
log_line_prefix = '%m [%p] %q%u@%d '
log_min_duration_statement = 2000

# Surface TOAST + logical-decoding behaviour quickly in tests
wal_sender_timeout = 60s
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
