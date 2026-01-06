import sys
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

# ==========================================
# 0. SETUP
# ==========================================
print("🚀 Script Initializing...")

current_file = Path(__file__).resolve()
project_root = current_file.parent
while not (project_root / 'src').exists():
    if project_root == project_root.parent:
        print("❌ Critical Error: Could not find Project Root.")
        sys.exit(1)
    project_root = project_root.parent

print(f"ℹ️  Project Root: {project_root}")
sys.path.append(str(project_root))

try:
    from src.utils.path_manager import DATA_PERFORMANCE_DIR, DATA_STORE_DIR
except ImportError as e:
    print(f"❌ Import Error: {e}")
    sys.exit(1)

# ==========================================
# 1. CONFIGURATION
# ==========================================
CURRENT_DATE = datetime.now().strftime('%Y-%m-%d')
print(f"📅 Target Date: {CURRENT_DATE}")

RAW_DIRS = [
    DATA_PERFORMANCE_DIR / "financial_times" / CURRENT_DATE,
    DATA_PERFORMANCE_DIR / "yahoo_finance" / CURRENT_DATE,
    DATA_PERFORMANCE_DIR / "stock_analysis" / CURRENT_DATE
]

CLEAN_DIR = DATA_STORE_DIR / "03_staging" / "daily_nav" / CURRENT_DATE
CLEAN_FILE = CLEAN_DIR / f"merged_daily_nav_{CURRENT_DATE}.csv"

# ==========================================
# 2. CORE LOGIC
# ==========================================

def load_and_merge_csvs():
    all_dfs = []
    print(f"🔍 Scanning folders...")
    
    for folder in RAW_DIRS:
        # เช็คว่าโฟลเดอร์มีอยู่จริงไหม (เผื่อยังไม่ได้รัน Stock Analysis)
        if not folder.exists():
            print(f"   ⚠️  Folder not found (Skipping): {folder.name}")
            continue
            
        csv_files = list(folder.glob("*.csv"))
        if not csv_files:
            print(f"   ⚠️  Folder exists but empty: {folder.name}")

        for csv_file in csv_files:
            if "error" in csv_file.name or "log" in csv_file.name:
                continue
            
            try:
                df = pd.read_csv(csv_file)
                
                # 1. Fix Columns Name
                df.columns = [c.strip().lower() for c in df.columns]
                
                # 2. Assign Source (เพิ่ม Logic ของ Stock Analysis)
                if 'source' not in df.columns:
                    folder_str = str(folder).lower()
                    if 'financial_times' in folder_str:
                        df['source'] = 'Financial Times'
                    elif 'yahoo_finance' in folder_str:
                        df['source'] = 'Yahoo Finance'
                    elif 'stock_analysis' in folder_str:
                        df['source'] = 'Stock Analysis'
                
                df['origin_file'] = csv_file.name
                all_dfs.append(df)
                print(f"      ✅ Loaded: {csv_file.name} ({len(df)} rows)")
            except Exception as e:
                print(f"      ❌ Error reading {csv_file.name}: {e}")

    if not all_dfs:
        return pd.DataFrame()

    merged_df = pd.concat(all_dfs, ignore_index=True)
    return merged_df

def clean_data(df):
    if df.empty: return df
    
    initial_count = len(df)
    
    # Select Cols
    target_cols = ['ticker', 'asset_type', 'source', 'nav_price', 'currency', 'as_of_date', 'scrape_date']
    for col in target_cols:
        if col not in df.columns: df[col] = None
    df = df[target_cols]

    # Clean Strings
    df['ticker'] = df['ticker'].astype(str).str.upper().str.strip()
    df['asset_type'] = df['asset_type'].astype(str).str.upper().str.strip()
    # เช็คว่า source ไม่เป็น None ก่อนเรียก .str
    if df['source'].notna().all():
        df['source'] = df['source'].astype(str).str.strip()
    
    # Drop Duplicates (Keep All Sources)
    df = df.drop_duplicates(subset=['ticker', 'asset_type', 'source', 'as_of_date'], keep='last')
    
    # Handle NAV
    df['nav_price'] = pd.to_numeric(df['nav_price'], errors='coerce')
    df = df.dropna(subset=['nav_price'])
    df = df[df['nav_price'] > 0]

    # Handle Dates
    df['as_of_date'] = pd.to_datetime(df['as_of_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['scrape_date'] = pd.to_datetime(df['scrape_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    
    cleaned_count = len(df)
    print(f"✨ Cleaning Summary: {initial_count} -> {cleaned_count} rows")
    
    return df

def main():
    df = load_and_merge_csvs()
    
    if df.empty:
        print("🚫 No data found to merge.")
        return

    df_clean = clean_data(df)
    
    if not df_clean.empty:
        CLEAN_DIR.mkdir(parents=True, exist_ok=True)
        df_clean.to_csv(CLEAN_FILE, index=False)
        print(f"💾 Saved Combined File: {CLEAN_FILE}")
        
        print("-" * 30)
        print("📊 Breakdown by Source:")
        print(df_clean['source'].value_counts())
        print("-" * 30)
        
        print("✅ SUCCESS")
    else:
        print("⚠️ Result empty after cleaning.")

if __name__ == "__main__":
    main()