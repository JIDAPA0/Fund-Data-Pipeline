import subprocess
import sys
import time
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.logger import setup_logger, log_execution_summary

logger = setup_logger("holdings_sync_orchestrator")

PIPELINE = [
    {"name": "00 Holdings Cleaner", "path": "src/05_db_synchronization/04_holdings_sync/00_holdings_data_cleaner.py"},
    {"name": "01 Holdings Integrity", "path": "src/05_db_synchronization/04_holdings_sync/01_holdings_integrity_checker.py"},
    {"name": "02 Holdings Hasher", "path": "src/05_db_synchronization/04_holdings_sync/02_holdings_hasher.py"},
    {"name": "03 Holdings Loader", "path": "src/05_db_synchronization/04_holdings_sync/03_holdings_loader.py"},
    {"name": "04 Allocations Loader", "path": "src/05_db_synchronization/04_holdings_sync/04_allocations_loader.py"},
    {"name": "05 Holdings Archiver", "path": "src/05_db_synchronization/04_holdings_sync/05_holdings_archiver.py"},
]


def get_env():
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR)
    return env


def run_step(step):
    full_path = BASE_DIR / step["path"]
    if not full_path.exists():
        logger.error(f"❌ Missing script: {full_path}")
        return False

    logger.info(f"▶️  Running: {step['name']}")
    start = time.time()
    try:
        subprocess.run([sys.executable, str(full_path)], check=True, env=get_env())
        logger.info(f"✅ Finished {step['name']} ({time.time() - start:.2f}s)")
        return True
    except subprocess.CalledProcessError:
        logger.error(f"❌ Failed: {step['name']}")
        return False


def main():
    pipeline_start = time.time()
    logger.info("🚀 HOLDINGS SYNC ORCHESTRATOR STARTED")

    results = []
    for step in PIPELINE:
        ok = run_step(step)
        results.append((step["name"], ok))
        if not ok:
            logger.critical("🛑 Aborting holdings sync due to failure.")
            break

    log_execution_summary(
        logger,
        start_time=pipeline_start,
        total_items=0,
        status="Success" if all(r[1] for r in results) else "Failed",
        extra_info={name: ("Success" if ok else "Failed") for name, ok in results},
    )


if __name__ == "__main__":
    main()
