import pandas as pd
import sys
import os
from pathlib import Path
from datetime import datetime

# ==========================================
# 1. SETUP ROOT PATH & IMPORTS
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.path_manager import DATA_MASTER_LIST_DIR
from src.utils.logger import setup_logger, log_execution_summary

# [Updated] Logger Name
logger = setup_logger("05_sync_Cleaner")

# ==========================================
# 2. CONFIGURATION
# ==========================================
REQUIRED_COLUMNS = ['ticker', 'asset_type', 'name', 'status', 'source', 'date_added']

SOURCES_CONFIG = [
    ("Financial_Times_Fund", "Financial_Times", "ft_funds_master.csv"),
    ("Financial_Times_ETF",  "Financial_Times", "ft_etfs_master.csv"),
    ("Yahoo_Finance_Fund",   "Yahoo_Finance",   "yf_fund_master.csv"),
    ("Yahoo_Finance_ETF",    "Yahoo_Finance",   "yf_etf_master.csv"),
    ("Stock_Analysis_ETF",   "Stock_Analysis",  "sa_etf_master.csv")
]

# ==========================================
# 3. CLEANING LOGIC
# ==========================================
def clean_dataframe(df, source_name):
    try:
        df.columns = [c.strip().lower() for c in df.columns]
        
        rename_map = {
            'symbol': 'ticker',
            'fund name': 'name', 
            'company name': 'name',
            'full_ticker': 'ticker'
        }
        df.rename(columns=rename_map, inplace=True)
        
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = None
        
        df = df[REQUIRED_COLUMNS].copy()

        if 'ticker' in df.columns:
            df['ticker'] = df['ticker'].astype(str).str.upper().str.strip()
            
        # [ASSET_TYPE]
        if 'asset_type' in df.columns:
            df['asset_type'] = df['asset_type'].astype(str).str.upper().str.strip() # [Updated] Force Uppercase (FUND, ETF)
            # Map Common Variations
            df['asset_type'] = df['asset_type'].replace({'MUTUAL FUND': 'FUND', 'MUTUALFUND': 'FUND'})

        # [STATUS] 
        # Normalize to 'new', 'active', 'inactive'
        if 'status' in df.columns:
            df['status'] = df['status'].astype(str).str.lower().str.strip()

        # [SOURCE] Fix common typos
        if 'source' in df.columns:
             df['source'] = df['source'].replace({
                 'FinancialTimes': 'Financial Times', 
                 'YahooFinance': 'Yahoo Finance',
                 'StockAnalysis': 'Stock Analysis'
             })

        # Internal Dedup
        initial_len = len(df)
        df.drop_duplicates(subset=['ticker', 'asset_type'], inplace=True)
        
        dupes = initial_len - len(df)
        if dupes > 0:
            logger.info(f"[{source_name}] ✂️ Removed {dupes} internal duplicates")

        return df

    except Exception as e:
        logger.error(f"Error cleaning data from {source_name}: {e}")
        return None

# ==========================================
# 4. MAIN EXECUTION
# ==========================================
def run_cleaner():
    start_time = datetime.now().timestamp()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    logger.info("🧹 STARTING MASTER LIST CLEANER")
    
    total_processed = 0
    cleaned_files_count = 0
    
    clean_stage_dir = DATA_MASTER_LIST_DIR / "01_cleaned_stage" / today_str
    clean_stage_dir.mkdir(parents=True, exist_ok=True)

    for source_key, folder_name, filename in SOURCES_CONFIG:
        try:
            raw_file_path = BASE_DIR / "validation_output" / folder_name / "01_List_Master" / today_str / filename
            
            if not raw_file_path.exists():
                logger.warning(f"⚠️ Raw file missing: {filename}")
                continue
                
            logger.info(f"Processing: {source_key}...")
            df = pd.read_csv(raw_file_path)
            cleaned_df = clean_dataframe(df, source_key)
            
            if cleaned_df is not None and not cleaned_df.empty:
                output_filename = f"clean_{filename}"
                output_path = clean_stage_dir / output_filename
                cleaned_df.to_csv(output_path, index=False)
                
                row_count = len(cleaned_df)
                total_processed += row_count
                cleaned_files_count += 1
                logger.info(f"✅ Saved: {output_filename} ({row_count:,} rows)")
            else:
                logger.warning(f"❌ Result empty for {source_key}")

        except Exception as e:
            logger.error(f"🔥 Critical fail on {source_key}: {e}")

    log_execution_summary(
        logger, 
        start_time, 
        total_items=total_processed, 
        status="Completed",
        extra_info={"Files Created": cleaned_files_count}
    )

if __name__ == "__main__":
    run_cleaner()