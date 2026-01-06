import subprocess
import sys
import time
import os
from pathlib import Path
import logging

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.logger import setup_logger, log_execution_summary

# Use INFO level; category resolved by name prefix
logger = setup_logger("05_sync_GLOBAL_PIPELINE", logging.INFO)

GLOBAL_PIPELINE = [
    {
        "name": "MASTER LIST SYNC (Module 01 + 05.01)",
        "path": "src/05_db_synchronization/01_master_sync/07_master_sync_orchestrator.py"
    },
    {
        "name": "PERFORMANCE SYNC (Module 02 + 05.02)",
        "path": "src/05_db_synchronization/02_performance_sync/05_performance_sync_orchestrator.py"
    },
    {
        "name": "DETAIL SYNC (Module 03 + 05.03)",
        "path": "src/05_db_synchronization/03_detail_sync/05_detail_sync_orchestrator.py"
    },
    {
        "name": "HOLDINGS SYNC (Module 04 + 05.04)",
        "path": "src/05_db_synchronization/04_holdings_sync/06_holdings_sync_orchestrator.py"
    },
]

def run_orchestrator(module):
    name = module["name"]
    full_path = BASE_DIR / module["path"]
    
    if not full_path.exists():
        logger.error(f"❌ Orchestrator Not Found: {full_path}")
        return False

    logger.info(f"🌐 [GLOBAL] Starting Module: {name}")
    start = time.time()
    
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR)

    try:
        
        subprocess.run([sys.executable, str(full_path)], check=True, env=env)
        
        duration = time.time() - start
        logger.info(f"✅ [GLOBAL] Module {name} Finished ({round(duration, 2)}s)")
        return True
    except subprocess.CalledProcessError:
        logger.error(f"❌ [GLOBAL] Module {name} Failed during execution.")
        return False

def main():
    pipeline_start = time.time()
    logger.info(f"{'='*60}")
    logger.info("🏁 STARTING GLOBAL DATA PIPELINE (END-TO-END)")
    logger.info(f"{'='*60}")
    
    results = []
    for module in GLOBAL_PIPELINE:
        success = run_orchestrator(module)
        results.append((module["name"], success))
        
        
        if not success:
            logger.critical("🛑 Critical Module failed. Stopping Global Pipeline to prevent data corruption.")
            break

    
    log_execution_summary(
        logger,
        start_time=pipeline_start,
        total_items=0,
        status="All Completed" if all(r[1] for r in results) else "Partial Completion",
        extra_info={f"Module {r[0]}": "Success" if r[1] else "Failed" for r in results}
    )

if __name__ == "__main__":
    main()
