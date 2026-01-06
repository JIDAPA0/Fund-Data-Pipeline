import shutil
import sys
import os
from pathlib import Path
from datetime import datetime

# ==========================================
# 1. SETUP ROOT PATH
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.path_manager import DATA_MASTER_LIST_DIR, DATA_STORE_DIR, VALIDATION_DIR
from src.utils.logger import setup_logger, log_execution_summary

# [Updated] Logger Name
logger = setup_logger("05_sync_Archiver")

def archive_daily_files():
    start_time = datetime.now().timestamp()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    logger.info("📦 STARTING MASTER DATA ARCHIVER (All Stages)")
    
    # ==============================================================================
    # 📝 [POINT OF CHANGE] เปลี่ยนชื่อโฟลเดอร์ตรงนี้ครับ
    # จาก "master_list" -> "01_master_sync"
    # ==============================================================================
    archive_root = DATA_STORE_DIR / "archive" / "01_master_sync" / today_str
    archive_root.mkdir(parents=True, exist_ok=True)
    
    total_archived = 0

    # =========================================================
    # PART 1: Archive Processed Data (Data Store)
    # =========================================================
    stages = [
        "01_cleaned_stage",
        "02_consolidated_stage",
        "03_validated_stage",
        "04_ready_to_load"
    ]
    
    for stage_name in stages:
        source_dir = DATA_MASTER_LIST_DIR / stage_name / today_str
        dest_zip_base = archive_root / stage_name
        
        if source_dir.exists() and any(source_dir.iterdir()):
            try:
                shutil.make_archive(base_name=str(dest_zip_base), format='zip', root_dir=str(source_dir))
                shutil.rmtree(source_dir)
                total_archived += 1
                logger.info(f"✅ Archived Processed: {stage_name} -> {dest_zip_base.name}.zip")
                try: source_dir.parent.rmdir() 
                except: pass 
            except Exception as e:
                logger.error(f"❌ Failed to archive processed {stage_name}: {e}")

    # =========================================================
    # PART 2: Archive Raw Data (Validation Output)
    # =========================================================
    raw_sources = ["Financial_Times", "Yahoo_Finance", "Stock_Analysis"]
    
    for source in raw_sources:
        raw_dir = VALIDATION_DIR / source / "01_List_Master" / today_str
        dest_zip_base = archive_root / f"raw_{source}"
        
        if raw_dir.exists() and any(raw_dir.iterdir()):
            try:
                shutil.make_archive(base_name=str(dest_zip_base), format='zip', root_dir=str(raw_dir))
                shutil.rmtree(raw_dir)
                total_archived += 1
                logger.info(f"✅ Archived Raw: {source} -> {dest_zip_base.name}.zip")
                try: raw_dir.parent.rmdir()
                except: pass
            except Exception as e:
                logger.error(f"❌ Failed to archive raw {source}: {e}")

    log_execution_summary(
        logger, 
        start_time, 
        total_items=total_archived, 
        status="Completed",
        extra_info={"Destination": str(archive_root)}
    )

if __name__ == "__main__":
    archive_daily_files()