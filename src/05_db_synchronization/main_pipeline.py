import subprocess
import sys
import time
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.logger import setup_logger, log_execution_summary

logger = setup_logger("GLOBAL_PIPELINE", "05_sync")

GLOBAL_PIPELINE = [
    {
        "name": "MASTER LIST SYNC (Module 01 + 05.01)",
        "path": "src/05_db_synchronization/01_master_sync/07_master_sync_orchestrator.py"
    },
]

def run_orchestrator(module):
    """ฟังก์ชันสั่งรัน Orchestrator รายโมดูล"""
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
        # รัน Orchestrator ของโมดูลนั้นๆ
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
        
        # ถ้า Master List พัง ไม่ควรไปทำโมดูลราคาต่อ (เพราะจะไม่มี Ticker ให้ดึง)
        if not success:
            logger.critical("🛑 Critical Module failed. Stopping Global Pipeline to prevent data corruption.")
            break

    # สรุปผลการรันทุกโมดูล
    log_execution_summary(
        logger,
        start_time=pipeline_start,
        total_items=0,
        status="All Completed" if all(r[1] for r in results) else "Partial Completion",
        extra_info={f"Module {r[0]}": "Success" if r[1] else "Failed" for r in results}
    )

if __name__ == "__main__":
    main()