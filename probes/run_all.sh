#!/usr/bin/env bash
# Run every probe serially. They all reseed the source schema, so they must not
# overlap. Each probe's JSON lands in probes/.out/<name>.json.
set -u
cd "$(dirname "$0")"
mkdir -p .out
for f in "$@"; do
  name="${f%.py}"
  echo "=== $name $(date +%T) ==="
  ../.venv/bin/python "$f" > ".out/${name}.json" 2> ".out/${name}.log"
  echo "    exit=$? $(date +%T)"
done
