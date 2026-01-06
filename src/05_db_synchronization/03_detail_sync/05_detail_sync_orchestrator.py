import subprocess
import sys
import time
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.logger import setup_logger, log_execution_summary

logger = setup_logger("detail_sync_orchestrator")

PIPELINE = [
    {"name": "00 Static Cleaner", "path": "src/05_db_synchronization/03_detail_sync/00_static_data_cleaner.py"},
    {"name": "01 Detail Validator", "path": "src/05_db_synchronization/03_detail_sync/01_detail_validator.py"},
    {"name": "02 Static Hasher", "path": "src/05_db_synchronization/03_detail_sync/02_static_hasher.py"},
    {"name": "03 Fund Detail Loader", "path": "src/05_db_synchronization/03_detail_sync/03_fund_detail_loader.py"},
    {"name": "04 Detail Archiver", "path": "src/05_db_synchronization/03_detail_sync/04_detail_archiver.py"},
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
    logger.info("🚀 DETAIL SYNC ORCHESTRATOR STARTED")

    results = []
    for step in PIPELINE:
        ok = run_step(step)
        results.append((step["name"], ok))
        if not ok:
            logger.critical("🛑 Aborting detail sync due to failure.")
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
