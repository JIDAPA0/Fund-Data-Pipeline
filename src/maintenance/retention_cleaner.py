import sys
import os
import time
from pathlib import Path
from datetime import datetime, timedelta

# Setup Root Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.logger import setup_logger, log_execution_summary
from src.utils.path_manager import DATA_STORE_DIR

logger = setup_logger("Retention_Cleaner", "99_sys")

RETENTION_DAYS = 60  

def run_retention_policy():
    start_time = time.time()
    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
    deleted_count = 0
    
    logger.info(f"⏳ STARTING RETENTION CLEANER (Cutoff: {cutoff_date.strftime('%Y-%m-%d')})")
    
    archive_root = DATA_STORE_DIR / "archive"
    if not archive_root.exists():
        logger.warning("No archive directory found.")
        return

    
    
    
    
    for date_dir in archive_root.rglob("*"):
        if date_dir.is_dir() and is_date_format(date_dir.name):
            
            try:
                folder_date = datetime.strptime(date_dir.name, "%Y-%m-%d")
                
                if folder_date < cutoff_date:
                    logger.info(f"🗑️ Purging Old Archive: {date_dir} (Age: {(datetime.now() - folder_date).days} days)")
                    
                    import shutil
                    shutil.rmtree(date_dir)
                    deleted_count += 1
            except Exception as e:
                logger.error(f"❌ Error deleting {date_dir}: {e}")

    log_execution_summary(
        logger, 
        start_time, 
        total_items=deleted_count, 
        status="Completed",
        extra_info={"Policy": f"Delete older than {RETENTION_DAYS} days"}
    )

def is_date_format(string):
    try:
        datetime.strptime(string, "%Y-%m-%d")
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    run_retention_policy()