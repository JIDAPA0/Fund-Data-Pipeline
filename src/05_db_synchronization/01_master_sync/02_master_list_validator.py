import pandas as pd
import sys
import os
from pathlib import Path
from datetime import datetime

# ==========================================
# 1. SETUP ROOT PATH
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.path_manager import DATA_MASTER_LIST_DIR
from src.utils.logger import setup_logger, log_execution_summary

# ✅ [FIXED] แก้ชื่อ Logger และลบพารามิเตอร์ตัวที่ 2
logger = setup_logger("05_sync_Validator")

def validate_data():
    start_time = datetime.now().timestamp()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    logger.info("🔍 STARTING MASTER LIST VALIDATOR")
    
    input_path = DATA_MASTER_LIST_DIR / "02_consolidated_stage" / today_str / "consolidated_master_list.csv"
    
    if not input_path.exists():
        logger.error(f"❌ Input file not found: {input_path}")
        return

    df = pd.read_csv(input_path)
    total_rows = len(df)
    
    valid_rows = []
    invalid_rows = []
    
    for index, row in df.iterrows():
        issues = []
        # Rule 1: Ticker ห้ามว่าง
        if pd.isna(row.get('ticker')) or str(row.get('ticker')).strip() == "":
            issues.append("Missing Ticker")
            
        # Rule 2: Asset Type ต้องเป็น FUND หรือ ETF (Case Insensitive)
        asset_val = str(row.get('asset_type')).strip().upper()
        if asset_val not in ['FUND', 'ETF']:
             issues.append(f"Invalid Asset Type: {asset_val}")
             
        # Rule 3: Source ห้ามว่าง
        if pd.isna(row.get('source')) or str(row.get('source')).strip() == "":
            issues.append("Missing Source")
            
        # Rule 4: Status ต้องเป็น active, inactive หรือ new
        status_val = str(row.get('status')).lower().strip()
        if status_val not in ['active', 'inactive', 'new']:
            issues.append(f"Invalid Status: {status_val}")

        if not issues:
            valid_rows.append(row)
        else:
            row_dict = row.to_dict()
            row_dict['validation_issues'] = "; ".join(issues)
            invalid_rows.append(row_dict)

    # Output Paths
    output_dir_03 = DATA_MASTER_LIST_DIR / "03_validated_stage" / today_str
    output_dir_04 = DATA_MASTER_LIST_DIR / "04_ready_to_load" / today_str
    
    output_dir_03.mkdir(parents=True, exist_ok=True)
    output_dir_04.mkdir(parents=True, exist_ok=True)
    
    # Save Valid (บันทึกทั้ง Stage 03 และ 04)
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows)
        # 1. Save to Validated Stage (Archive purpose)
        valid_path = output_dir_03 / "valid_master_list.csv"
        valid_df.to_csv(valid_path, index=False)
        
        # 2. Save to Ready to Load (For Loader)
        final_path = output_dir_04 / "master_list_final.csv"
        valid_df.to_csv(final_path, index=False)
        
        logger.info(f"✅ VALID Data: {len(valid_df):,} rows -> {final_path.name}")
    
    # Save Invalid
    if invalid_rows:
        invalid_path = output_dir_03 / "invalid_master_list.csv"
        pd.DataFrame(invalid_rows).to_csv(invalid_path, index=False)
        logger.warning(f"🚫 INVALID Data: {len(invalid_rows):,} rows -> {invalid_path.name}")

    log_execution_summary(logger, start_time, total_items=total_rows, status="Completed",
                          extra_info={"Passed": len(valid_rows), "Failed": len(invalid_rows)})

if __name__ == "__main__":
    validate_data()