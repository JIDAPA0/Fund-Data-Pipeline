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

# [Updated] Logger Name
logger = setup_logger("05_sync_Consolidator")

# ==========================================
# 2. CONSOLIDATION LOGIC
# ==========================================
def consolidate_sources():
    start_time = datetime.now().timestamp()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    logger.info("🔗 STARTING SOURCE CONSOLIDATOR (Allow Cross-Source Duplicates)")
    
    
    clean_stage_dir = DATA_MASTER_LIST_DIR / "01_cleaned_stage" / today_str
    
    if not clean_stage_dir.exists():
        logger.error(f"❌ Cleaned stage directory not found: {clean_stage_dir}")
        return

    
    all_files = list(clean_stage_dir.glob("clean_*.csv"))
    
    if not all_files:
        logger.warning("⚠️ No cleaned files found.")
        return

    logger.info(f"Found {len(all_files)} files to merge.")
    
    df_list = []
    
    
    for f in all_files:
        try:
            df = pd.read_csv(f)
            
            # Fallback for missing source column
            if 'source' not in df.columns:
                if "ft_" in f.name: src = "Financial Times"
                elif "yf_" in f.name: src = "Yahoo Finance"
                elif "sa_" in f.name: src = "Stock Analysis"
                else: src = "Unknown"
                df['source'] = src
                
            df_list.append(df)
        except Exception as e:
            logger.error(f"Error reading {f.name}: {e}")

    if not df_list:
        return

    
    full_df = pd.concat(df_list, ignore_index=True)
    initial_count = len(full_df)
    
    # ==============================================================================
    # 5. DEDUPLICATION LOGIC (UPDATED)
    
    # ==============================================================================
    
    
    full_df.sort_values(by=['source', 'asset_type', 'ticker'], ascending=True, inplace=True)
    
    
    full_df.drop_duplicates(subset=['ticker', 'asset_type', 'source'], keep='first', inplace=True)
    
    
    if 'priority' in full_df.columns:
        full_df.drop(columns=['priority'], inplace=True)
    
    final_count = len(full_df)
    duplicates_removed = initial_count - final_count
    
    logger.info(f"✅ Merged Result: {final_count:,} rows (from {initial_count:,} raw rows)")
    
    # 6. Save Output
    output_dir = DATA_MASTER_LIST_DIR / "02_consolidated_stage" / today_str
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / "consolidated_master_list.csv"
    full_df.to_csv(output_path, index=False)
    
    logger.info(f"✅ Saved merged list to: {output_path}")

    log_execution_summary(
        logger, 
        start_time, 
        total_items=final_count, 
        status="Completed",
        extra_info={
            "Strategy": "Dedup within Source only",
            "Duplicates Removed": duplicates_removed
        }
    )

if __name__ == "__main__":
    consolidate_sources()