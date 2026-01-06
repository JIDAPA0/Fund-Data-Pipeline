#!/usr/bin/env sh
set -e

cd /app
export PYTHONPATH=/app

echo "Starting Fund ETL main pipeline..."
python src/05_db_synchronization/main_pipeline.py
