import sys
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from sqlalchemy import text

# ==========================================
# 0. SETUP & DB CONNECTION
# ==========================================
print("🚀 Script: Price History Loader Starting...")

current_file = Path(__file__).resolve()
project_root = current_file.parent
while not (project_root / 'src').exists():
    if project_root == project_root.parent: break
    project_root = project_root.parent
sys.path.append(str(project_root))

from src.utils.db_connector import get_db_connection

# ==========================================
# 1. CONFIGURATION
# ==========================================
HASHED_BASE_DIR = project_root / "data" / "04_hashed" / "price_history"
TARGET_TABLE = "stg_price_history"

# ==========================================
# 2. CORE LOADER LOGIC
# ==========================================

def upsert_to_db(df, engine):
    if df.empty: return

    temp_table = f"temp_{TARGET_TABLE}_{int(datetime.now().timestamp())}"
    
    try:
        df.to_sql(temp_table, engine, if_exists='replace', index=False)
        
        upsert_query = f"""
        INSERT INTO {TARGET_TABLE} (ticker, asset_type, source, date, open, high, low, close, adj_close, volume, row_hash, updated_at)
        SELECT ticker, asset_type, source, date, open, high, low, close, adj_close, volume, row_hash, updated_at 
        FROM {temp_table}
        ON CONFLICT (ticker, asset_type, source, date) 
        DO UPDATE SET 
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            adj_close = EXCLUDED.adj_close,
            volume = EXCLUDED.volume,
            row_hash = EXCLUDED.row_hash,
            updated_at = EXCLUDED.updated_at
        WHERE {TARGET_TABLE}.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
        """
        
        with engine.begin() as conn:
            conn.execute(text(upsert_query))
        
        return len(df)
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

def main():
    engine = get_db_connection()
    
    print(f"📂 Scanning hashed files in: {HASHED_BASE_DIR}")
    all_hashed_files = list(HASHED_BASE_DIR.rglob("*.csv"))
    
    if not all_hashed_files:
        print("⚠️ No hashed files found to upload.")
        return

    total_rows = 0
    file_count = 0
    
    for csv_file in all_hashed_files:
        try:
            df = pd.read_csv(csv_file)
            if df.empty:
                continue

            # Align column names with DB schema and remove unusable rows
            rename_map = {
                "adj close": "adj_close",
                "Adj Close": "adj_close",
                "change %": "change_pct",
            }
            df = df.rename(columns=lambda c: c.strip())
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            if "change_pct" in df.columns:
                df = df.drop(columns=["change_pct"])
            if "updated_at" in df.columns:
                df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
                df["updated_at"] = df["updated_at"].fillna(pd.Timestamp.utcnow())
            else:
                df["updated_at"] = pd.Timestamp.utcnow()
            if "row_hash" in df.columns:
                df["row_hash"] = df["row_hash"].fillna("").astype(str).str.strip()
                df = df[df["row_hash"] != ""]
            if df.empty:
                continue
            
            rows_added = upsert_to_db(df, engine)
            total_rows += rows_added
            file_count += 1
            
            if file_count % 50 == 0:
                print(f"   📤 Uploaded {file_count} files... (Total rows: {total_rows})")
                
        except Exception as e:
            print(f"   ❌ Error uploading {csv_file.name}: {e}")

    print(f"\n✨ {'='*30}")
    print(f"✅ LOAD COMPLETED!")
    print(f"📊 Total Files Processed: {file_count}")
    print(f"📈 Total Rows Upserted: {total_rows}")
    print(f"{'='*30}")

if __name__ == "__main__":
    main()
