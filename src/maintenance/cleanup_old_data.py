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
    """
    ลบข้อมูล Raw Data (CSV) เก่าที่ผ่านการ Process และ Archive ไปแล้ว
    Logic: ลบโฟลเดอร์วันที่ที่ไม่ใช่ 'วันนี้' ทิ้งทั้งหมด (เพราะถือว่าจบ process วันนั้นแล้ว)
    """
    start_time = time.time()
    today_str = datetime.now().strftime("%Y-%m-%d")
    deleted_count = 0
    
    logger.info(f"🧹 STARTING RAW DATA CLEANUP (Keep only {today_str})")

    # รายชื่อ Source หลักๆ ที่ต้องเข้าไปเช็ค
    target_sources = ["Financial_Times", "Yahoo_Finance", "Stock_Analysis"]
    
    for source in target_sources:
        source_dir = VALIDATION_DIR / source
        if not source_dir.exists(): continue
        
        # วนลูปดูทุก Category ใน Source (เช่น 01_List_Master, 02_Daily_NAV)
        for category_dir in source_dir.iterdir():
            if not category_dir.is_dir(): continue
            
            # วนลูปดูโฟลเดอร์วันที่ (YYYY-MM-DD)
            for date_dir in category_dir.iterdir():
                if not date_dir.is_dir(): continue
                
                folder_date = date_dir.name
                
                # ถ้าวันที่ของโฟลเดอร์ ไม่ใช่วันนี้ -> ลบทิ้ง
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