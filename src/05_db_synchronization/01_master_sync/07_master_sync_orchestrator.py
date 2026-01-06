import subprocess
import sys
import time
import os
from pathlib import Path

# ==========================================
# 1. SETUP ROOT PATH
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.logger import setup_logger, log_execution_summary

# ✅ Logger Name
logger = setup_logger("05_sync_Orchestrator")

# ==========================================
# 2. DEFINE PIPELINES
# ==========================================


SCRAPER_GROUP = [
    {"name": "FT List Scraper",      "path": "src/01_master_list_acquisition/01_ft_list_scraper.py"},
    {"name": "YF List Scraper",      "path": "src/01_master_list_acquisition/02_yf_list_scraper.py"},
    {"name": "SA List Scraper",      "path": "src/01_master_list_acquisition/03_sa_list_scraper.py"},
]


ETL_PIPELINE = [
    {"name": "00 Data Cleaner",         "path": "src/05_db_synchronization/01_master_sync/00_master_list_cleaner.py"},
    {"name": "01 Source Consolidator",  "path": "src/05_db_synchronization/01_master_sync/01_source_consolidator.py"},
    {"name": "02 Data Validator",       "path": "src/05_db_synchronization/01_master_sync/02_master_list_validator.py"},
    {"name": "03 Data Remediator",      "path": "src/05_db_synchronization/01_master_sync/03_master_list_remediator.py"},
    {"name": "04 Database Loader",      "path": "src/05_db_synchronization/01_master_sync/04_master_list_loader.py"},
    {"name": "05 Status Manager",       "path": "src/05_db_synchronization/01_master_sync/05_status_manager.py"},
    {"name": "06 Data Archiver",        "path": "src/05_db_synchronization/01_master_sync/06_master_data_archiver.py"}
]

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================

def get_env():
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR)
    return env

def run_scrapers_in_parallel():
    logger.info(f"⚡ STARTING PHASE 1: Scrapers (Parallel Mode - {len(SCRAPER_GROUP)} tasks)")
    
    processes = []
    
    
    for script in SCRAPER_GROUP:
        full_path = BASE_DIR / script["path"]
        if not full_path.exists():
            logger.error(f"❌ Script Not Found: {full_path}")
            continue
            
        logger.info(f"   ▶️  Launching: {script['name']}...")
        
        
        try:
            p = subprocess.Popen(
                [sys.executable, str(full_path)],
                env=get_env(),
                
                # stderr=subprocess.PIPE
            )
            processes.append({"name": script["name"], "process": p})
        except Exception as e:
            logger.error(f"❌ Failed to launch {script['name']}: {e}")

    
    logger.info("⏳ Waiting for all scrapers to finish...")
    success_count = 0
    
    for item in processes:
        p = item["process"]
        name = item["name"]
        
        
        return_code = p.wait()
        
        if return_code == 0:
            logger.info(f"   ✅ Finished: {name}")
            success_count += 1
        else:
            logger.warning(f"   ⚠️ Failed: {name} (Return Code: {return_code})")
            
    return success_count

def run_etl_sequentially():
    logger.info(f"🔄 STARTING PHASE 2: ETL Pipeline (Sequential Mode)")
    
    for script in ETL_PIPELINE:
        name = script["name"]
        full_path = BASE_DIR / script["path"]
        
        if not full_path.exists():
            logger.error(f"❌ Script Not Found: {full_path}")
            return False

        logger.info(f"   ▶️  Executing: {name}...")
        start = time.time()
        
        try:
            
            subprocess.run([sys.executable, str(full_path)], check=True, env=get_env())
            logger.info(f"   ✅ Success: {name} ({round(time.time() - start, 2)}s)")
        except subprocess.CalledProcessError:
            logger.critical(f"🛑 CRITICAL ERROR: {name} failed. Aborting Pipeline.")
            return False
            
    return True

# ==========================================
# 4. MAIN ORCHESTRATOR
# ==========================================
def main():
    total_start = time.time()
    logger.info("🚀 MASTER SYNC ORCHESTRATOR STARTED")
    
    # --- PHASE 1: ACQUISITION ---
    scrapers_success = run_scrapers_in_parallel()
    
    
    if scrapers_success == 0:
        logger.warning("⚠️ All scrapers failed or none ran. Proceeding to ETL with existing data (if any).")
    
    logger.info("-" * 50)
    
    # --- PHASE 2: SYNCHRONIZATION ---
    etl_success = run_etl_sequentially()
    
    status = "Success" if etl_success else "Failed"
    
    log_execution_summary(
        logger, 
        total_start, 
        total_items=0, 
        status=status,
        extra_info={
            "Scrapers OK": f"{scrapers_success}/{len(SCRAPER_GROUP)}",
            "ETL Status": "Completed" if etl_success else "Aborted"
        }
    )

if __name__ == "__main__":
    main()