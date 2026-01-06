import sys
import shutil
import time
from pathlib import Path
from datetime import datetime

# Setup Root Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.logger import setup_logger, log_execution_summary
from src.utils.path_manager import VALIDATION_DIR

logger = setup_logger("Cleanup_Raw_Data", "99_sys")

def cleanup_raw_files():
    start_time = time.time()
    today_str = datetime.now().strftime("%Y-%m-%d")
    deleted_count = 0
    
    logger.info(f"🧹 STARTING RAW DATA CLEANUP (Keep only {today_str})")

    
    target_sources = ["Financial_Times", "Yahoo_Finance", "Stock_Analysis"]
    
    for source in target_sources:
        source_dir = VALIDATION_DIR / source
        if not source_dir.exists(): continue
        
        
        for category_dir in source_dir.iterdir():
            if not category_dir.is_dir(): continue
            
            
            for date_dir in category_dir.iterdir():
                if not date_dir.is_dir(): continue
                
                folder_date = date_dir.name
                
                
                if folder_date != today_str:
                    try:
                        shutil.rmtree(date_dir)
                        logger.info(f"🗑️ Deleted Raw Data: {source}/{category_dir.name}/{folder_date}")
                        deleted_count += 1
                    except Exception as e:
                        logger.error(f"❌ Failed to delete {date_dir}: {e}")

    log_execution_summary(
        logger, 
        start_time, 
        total_items=deleted_count, 
        status="Completed",
        extra_info={"Action": "Cleaned old raw CSV folders"}
    )

if __name__ == "__main__":
    cleanup_raw_files()