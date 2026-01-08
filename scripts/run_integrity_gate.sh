#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$ROOT_DIR/.venv_fund_etl/bin/python"

if [[ ! -x "$VENV_PY" ]]; then
  echo "Missing venv: $VENV_PY"
  exit 1
fi

mkdir -p "$ROOT_DIR/logs"

export PYTHONPATH="$ROOT_DIR"
export ALLOW_PARTIAL_NAV="${ALLOW_PARTIAL_NAV:-1}"

"$VENV_PY" "$ROOT_DIR/src/05_db_synchronization/integrity_gate_pipeline.py"
