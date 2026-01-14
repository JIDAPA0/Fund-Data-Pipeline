#!/usr/bin/env sh
set -e

cd /app
export PYTHONPATH=/app

mkdir -p /app/logs /app/data /app/validation_output /app/tmp

if [ -f /app/cron_schedule ]; then
  crontab /app/cron_schedule
fi

echo "Starting cron..."
exec cron -f
