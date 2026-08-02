#!/usr/bin/env bash
# Prepare or remove one sentinel-marked, project-local runtime-state directory.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"

exec python3 "${SCRIPT_DIR}/runtime_state.py" --project-dir "${PROJECT_DIR}" "$@"
