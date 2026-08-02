#!/usr/bin/env bash
# Prepare or remove one sentinel-marked, project-local runtime-state directory.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
RUNTIME_PARENT="${PROJECT_DIR}/.cdc_instances"
SENTINEL_NAME=".cdc_flight_disposable_runtime"

refuse() {
  echo "ERROR: refusing runtime-state ${1}" >&2
  exit 2
}

if [[ -n "${CDC_INSTANCE_RUNTIME_ROOT+x}" ]]; then
  refuse "cleanup from CDC_INSTANCE_RUNTIME_ROOT; select CDC_TEST_INSTANCE_ID instead"
fi

instance_id="${CDC_TEST_INSTANCE_ID:-pg${CDC_TEST_PGPORT:-15432}}"
if ! [[ "${instance_id}" =~ ^[a-z0-9][a-z0-9_]*$ ]]; then
  refuse "path for invalid CDC_TEST_INSTANCE_ID '${instance_id}'"
fi

mkdir -p "${RUNTIME_PARENT}"
runtime_parent="$(cd "${RUNTIME_PARENT}" && pwd -P)"
runtime_target="${runtime_parent}/${instance_id}"

resolve_target() {
  if [[ -d "${runtime_target}" ]]; then
    (cd "${runtime_target}" && pwd -P)
  elif [[ -e "${runtime_target}" || -L "${runtime_target}" ]]; then
    refuse "path because it is not a directory: ${runtime_target}"
  else
    printf '%s\n' "${runtime_target}"
  fi
}

require_exact_child() {
  local resolved_target="$1"
  if [[ "$(dirname "${resolved_target}")" != "${runtime_parent}" \
        || "$(basename "${resolved_target}")" != "${instance_id}" ]]; then
    refuse "path outside ${runtime_parent}: ${resolved_target}"
  fi
}

case "${1:-}" in
  prepare)
    resolved_target="$(resolve_target)"
    require_exact_child "${resolved_target}"
    mkdir -p -- "${runtime_target}"
    resolved_target="$(resolve_target)"
    require_exact_child "${resolved_target}"
    printf 'cdc_flight disposable runtime state\ninstance=%s\n' "${instance_id}" \
      > "${resolved_target}/${SENTINEL_NAME}"
    ;;
  clean)
    if [[ ! -e "${runtime_target}" && ! -L "${runtime_target}" ]]; then
      exit 0
    fi
    resolved_target="$(resolve_target)"
    require_exact_child "${resolved_target}"
    sentinel="${resolved_target}/${SENTINEL_NAME}"
    if [[ ! -f "${sentinel}" ]]; then
      refuse "deletion of unmarked directory ${resolved_target}; missing ${sentinel}"
    fi
    rm -rf -- "${resolved_target}"
    ;;
  *)
    echo "usage: $0 {prepare|clean}" >&2
    exit 2
    ;;
esac
