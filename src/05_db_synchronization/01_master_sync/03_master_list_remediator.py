import pandas as pd
import sys
import os
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.path_manager import DATA_MASTER_LIST_DIR
from src.utils.logger import setup_logger, log_execution_summary


logger = setup_logger("05_sync_Remediator")

def remediate_data():
    start_time = datetime.now().timestamp()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    logger.info("🔧 STARTING MASTER LIST REMEDIATOR")
    
    
    
    
    
    input_path = DATA_MASTER_LIST_DIR / "03_validated_stage" / today_str / "valid_master_list.csv"
    output_dir = DATA_MASTER_LIST_DIR / "04_ready_to_load" / today_str
    output_path = output_dir / "master_list_final.csv"
    
    if output_path.exists():
        logger.info("ℹ️  File already exists in '04_ready_to_load'. Skipping copy.")
    elif input_path.exists():
        logger.info("ℹ️  Copying valid file to '04_ready_to_load'...")
        output_dir.mkdir(parents=True, exist_ok=True)
        df = pd.read_csv(input_path)
        df.to_csv(output_path, index=False)
        logger.info(f"✅ Data Ready for Loading: {output_path}")
    else:
        logger.warning(f"⚠️ No valid input file found: {input_path}")

    log_execution_summary(logger, start_time, status="Completed")

if __name__ == "__main__":
    remediate_data()