import logging
import sys
import os
import time
from pathlib import Path
from datetime import datetime, timedelta

# Import LOG_DIR
try:
    from src.utils.path_manager import LOG_DIR
except ImportError:
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    LOG_DIR = BASE_DIR / "logs"

# ==============================================================================
# LOG CATEGORY MAPPING 
# ==============================================================================
LOG_CATEGORY_MAP = {
    "01_master":   "01_master_list_acquisition",   
    "02_perf":     "02_daily_performance",         
    "03_static":   "03_master_detail_static",      
    "04_holdings": "04_holdings_acquisition",      
    "05_sync":     "05_db_synchronization",
    "99_sys":      "99_system_maintenance"
}

def setup_logger(name, log_level=logging.INFO):
    
    category_folder = "general"
    for prefix, folder_name in LOG_CATEGORY_MAP.items():
        if name.startswith(prefix):
            category_folder = folder_name
            break
            
    
    target_log_dir = LOG_DIR / category_folder
    target_log_dir.mkdir(parents=True, exist_ok=True)
    
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    
    log_file_path = target_log_dir / f"{name}_{today}.log"
    
    
    error_file_path = target_log_dir / f"{name}_{today}_error.log"

    
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    
    err_handler = logging.FileHandler(error_file_path, encoding='utf-8')
    err_handler.setFormatter(formatter)
    err_handler.setLevel(logging.ERROR) 
    logger.addHandler(err_handler)

    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    return logger

def log_execution_summary(logger, start_time, total_items=0, success_count=0, error_count=0, status="Completed", extra_info=None):
    end_time = datetime.now()
    if isinstance(start_time, float):
        duration = timedelta(seconds=int(time.time() - start_time))
    else:
        duration = end_time - start_time
    
    
    def safe_int(val):
        try:
            return int(str(val).replace(',', ''))
        except (ValueError, TypeError):
            return 0

    
    total_safe = safe_int(total_items)
    success_safe = safe_int(success_count)
    error_safe = safe_int(error_count)

    logger.info("="*60)
    logger.info(f"🏁 EXECUTION SUMMARY")
    logger.info("="*60)
    logger.info(f"⏱️  Duration:    {duration}")
    logger.info(f"📊 Total Items: {total_safe}")
    
    
    if success_safe > 0 or error_safe > 0:
        logger.info(f"✅ Success:     {success_safe}")
        logger.info(f"❌ Errors:      {error_safe}")
        
    logger.info(f"📈 Status:      {status}")
    
    if extra_info:
        for key, value in extra_info.items():
            logger.info(f"ℹ️  {key}: {value}")
            
    logger.info("="*60)