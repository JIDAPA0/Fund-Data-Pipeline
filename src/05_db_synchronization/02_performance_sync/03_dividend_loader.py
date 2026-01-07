import sys
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from sqlalchemy import text

# ==========================================
# 0. AUTOMATIC PATH SETUP
# ==========================================
current_file = Path(__file__).resolve()
project_root = current_file.parent
while not (project_root / 'src').exists():
    if project_root == project_root.parent:
        break
    project_root = project_root.parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.utils.db_connector import insert_dataframe, get_db_engine, init_dividend_history_table

# ==========================================
# 1. CONFIGURATION
# ==========================================
HASHED_DIR = project_root / "data" / "04_hashed" / "dividend_history"
TARGET_TABLE = "stg_dividend_history"

# ==========================================
# 2. CORE LOADER LOGIC
# ==========================================

def load_dividend_to_db(df):
    if df.empty:
        return 0

    mapping = {
        'ex_dividend_date': 'ex_date',
        'pay_date': 'payment_date',
        'cash_amount': 'amount',
        'dividend': 'amount',
        'ex_date': 'ex_date',
        'payment_date': 'payment_date'
    }
    df = df.rename(columns=mapping)
    
    for col in ['ex_date', 'payment_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    
    if 'type' not in df.columns:
        df['type'] = 'Cash'
    if 'currency' not in df.columns:
        df['currency'] = None
    if 'updated_at' in df.columns:
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
        df['updated_at'] = df['updated_at'].fillna(pd.Timestamp.utcnow())
    else:
        df['updated_at'] = pd.Timestamp.utcnow()
    if 'row_hash' in df.columns:
        df['row_hash'] = df['row_hash'].fillna("").astype(str).str.strip()
        df = df[df['row_hash'] != ""]
    else:
        df['row_hash'] = None

    required_cols = [
        'ticker',
        'asset_type',
        'source',
        'ex_date',
        'payment_date',
        'amount',
        'currency',
        'type',
        'row_hash',
        'updated_at',
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    df = df[required_cols]
    df = df.dropna(subset=['ticker', 'asset_type', 'source', 'ex_date', 'amount'])
    df = df.drop_duplicates(subset=['ticker', 'asset_type', 'source', 'ex_date', 'amount', 'type', 'payment_date'])

    try:
        insert_dataframe(df, TARGET_TABLE)
        return len(df)
    except Exception as e:
        print(f"      ❌ Load Error: {e}")
        return 0

def main():
    print("🚀 Starting Flexible Dividend Loader")
    
    try:
        engine = get_db_engine()
        init_dividend_history_table(engine)
    except Exception as e:
        print(f"❌ Cannot connect to Database: {e}")
        return

    if not HASHED_DIR.exists():
        print(f"⚠️ Hashed directory not found: {HASHED_DIR}")
        return

    all_files = list(HASHED_DIR.rglob("*.csv"))
    print(f"📂 Found {len(all_files)} files to process.")

    total_uploaded_rows = 0
    processed_files = 0

    for csv_file in all_files:
        try:
            df = pd.read_csv(csv_file)
            if df.empty:
                continue

            rows_added = load_dividend_to_db(df)
            total_uploaded_rows += rows_added
            processed_files += 1

            if processed_files % 100 == 0:
                print(f"   📤 Progress: {processed_files} files uploaded... ({total_uploaded_rows} rows)")

        except Exception as e:
            print(f"   ❌ Error processing {csv_file.name}: {e}")

    print(f"\n✨ {'='*35}")
    print(f"✅ FLEXIBLE LOAD COMPLETED!")
    print(f"📊 Files Processed: {processed_files}")
    print(f"📈 Total Rows Inserted: {total_uploaded_rows}")
    print(f"🎯 Target Table: {TARGET_TABLE}")
    print(f"{'='*35}")

if __name__ == "__main__":
    main()
