#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$ROOT_DIR/.venv_fund_etl/bin/python"

log() {
  printf "[setup_vm] %s\n" "$*"
}

fail() {
  printf "[setup_vm] ERROR: %s\n" "$*" >&2
  exit 1
}

if [[ ! -f "$ROOT_DIR/requirements.txt" ]]; then
  fail "requirements.txt not found at $ROOT_DIR"
fi

OS_NAME="$(uname -s || true)"
if [[ "$OS_NAME" != "Linux" ]]; then
  log "Warning: expected Linux VM, detected $OS_NAME."
fi

if ! command -v apt-get >/dev/null 2>&1; then
  fail "apt-get not found. This script targets Debian/Ubuntu."
fi

SUDO=""
if [[ "$EUID" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  else
    fail "sudo not found. Run as root or install sudo."
  fi
fi

log "Installing OS packages..."
$SUDO apt-get update -y
$SUDO apt-get install -y \
  python3 \
  python3-venv \
  python3-pip \
  git \
  curl \
  ca-certificates \
  cron

log "Ensuring .env exists..."
if [[ ! -f "$ROOT_DIR/.env" ]]; then
  if [[ -f "$ROOT_DIR/.env.example" ]]; then
    cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
    log "Copied .env.example to .env (please update DB credentials)."
  else
    log "No .env or .env.example found; create .env with DB credentials."
  fi
fi

log "Creating virtual environment..."
if [[ ! -x "$VENV_PY" ]]; then
  python3 -m venv "$ROOT_DIR/.venv_fund_etl"
fi

log "Installing Python dependencies..."
"$VENV_PY" -m pip install --upgrade pip
"$VENV_PY" -m pip install -r "$ROOT_DIR/requirements.txt"

log "Installing Playwright browsers..."
if [[ "$OS_NAME" == "Linux" ]]; then
  "$VENV_PY" -m playwright install --with-deps
else
  "$VENV_PY" -m playwright install
fi

log "Ensuring logs directory exists..."
mkdir -p "$ROOT_DIR/logs"

if command -v crontab >/dev/null 2>&1; then
  CRON_CMD="0 6 * * 1-5 cd $ROOT_DIR && /bin/bash scripts/run_integrity_gate.sh >> $ROOT_DIR/logs/cron_integrity_gate.log 2>&1"
  EXISTING_CRON="$(crontab -l 2>/dev/null || true)"
  if echo "$EXISTING_CRON" | grep -F "$CRON_CMD" >/dev/null 2>&1; then
    log "Cron entry already exists."
  else
    printf "%s\n%s\n" "$EXISTING_CRON" "$CRON_CMD" | crontab -
    log "Cron entry installed for 06:00 weekdays."
  fi
else
  log "crontab not found; skipping cron setup."
fi

log "Setup complete."
