#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

dump_file="database/fund_analytics_staging_db.dump"
message="Update ETL and latest DB dump"
yes=false
no_push=false

usage() {
  cat <<'USAGE'
Usage: scripts/publish_with_db_dump.sh [options]
  -m, --message  Commit message (default: "Update ETL and latest DB dump")
  --dump         Dump file path (default: database/fund_analytics_staging_db.dump)
  -y, --yes      Skip confirmation prompt
  --no-push      Skip git push
  -h, --help     Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--message)
      message="${2:-}"
      shift 2
      ;;
    --dump)
      dump_file="${2:-}"
      shift 2
      ;;
    -y|--yes)
      yes=true
      shift
      ;;
    --no-push)
      no_push=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 2
      ;;
  esac
done

if ! command -v pg_dump >/dev/null 2>&1; then
  echo "pg_dump not found. Install PostgreSQL client tools first."
  exit 1
fi

if [[ -f ".env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi

: "${DB_USER:?DB_USER is required}"
: "${DB_PASSWORD:?DB_PASSWORD is required}"
: "${DB_NAME:?DB_NAME is required}"
: "${DB_HOST:=localhost}"
: "${DB_PORT:=5432}"

mkdir -p "$(dirname "$dump_file")"

export PGPASSWORD="$DB_PASSWORD"
pg_dump --no-owner --format=custom \
  --file "$dump_file" \
  "postgresql://${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
unset PGPASSWORD

if command -v python3 >/dev/null 2>&1; then
  py=python3
else
  py=python
fi

size_bytes="$("$py" - <<'PY' "$dump_file"
import os, sys
print(os.path.getsize(sys.argv[1]))
PY
)"

if [[ "$size_bytes" -gt 104857600 ]]; then
  if git lfs version >/dev/null 2>&1; then
    git lfs track "database/*.dump" >/dev/null
    git add .gitattributes
  else
    echo "Warning: dump file > 100MB but git-lfs is not available."
  fi
fi

git status -sb

if [[ "$yes" != "true" ]]; then
  read -r -p "Commit and push all changes? [y/N] " reply
  case "$reply" in
    y|Y|yes|YES) ;;
    *) echo "Aborted."; exit 1 ;;
  esac
fi

git add -A

if git diff --cached --quiet; then
  echo "No changes to commit."
  exit 0
fi

git commit -m "$message"

if [[ "$no_push" != "true" ]]; then
  branch="$(git rev-parse --abbrev-ref HEAD)"
  git push origin "$branch"
fi
