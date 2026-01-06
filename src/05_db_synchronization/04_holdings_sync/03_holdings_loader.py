import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from pathlib import Path
import sys

# Setup Path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[1]
sys.path.append(str(project_root))

try:
    from src.utils.db_connector import get_db_engine
except ImportError:
    print("❌ Error: ไม่พบไฟล์ 'src/utils/db_connector.py'")
    sys.exit(1)

# ----------------------------------------------------
# 1. SCHEMA DEFINITIONS (Updated)
# ----------------------------------------------------

TABLE_HOLDINGS = "staging_holdings"
TABLE_ALLOCATIONS = "staging_allocations"

# ✅ 1. ตาราง Holdings (เพิ่ม row_hash และ source_update_date)
COLS_HOLDINGS = [
    'fund_key', 
    'source_name', 
    'scrape_date',          # วันที่บอททำงาน
    'source_update_date',   # 👈 วันที่ข้อมูลอัปเดตบนหน้าเว็บ (As of...)
    'row_hash',             # 👈 Hash เพื่อตรวจสอบการเปลี่ยนแปลงรายบรรทัด
    'ticker', 
    'asset_type', 
    'no',
    'pct_weight', 
    'shares', 
    'pct_asset_individ',
    'pct_top_10_portfol', 
    'is_top_10'
]

# ✅ 2. ตาราง Allocations (เพิ่ม row_hash และ source_update_date)
COLS_ALLOCATIONS = [
    'fund_key',
    'source_name',
    'scrape_date',          # วันที่บอททำงาน
    'source_update_date',   # 👈 วันที่ข้อมูลอัปเดตบนหน้าเว็บ (As of...)
    'row_hash',             # 👈 Hash เพื่อตรวจสอบการเปลี่ยนแปลง
    'allocation_type',      # 'SECTOR', 'GEOGRAPHIC', 'ASSET_CLASS'
    'category_name',        # เช่น 'Technology', 'US Government', 'Cash'
    'pct_weight'
]

# ----------------------------------------------------
# 2. MAIN FUNCTION
# ----------------------------------------------------

def force_create_staging_tables_v2():
    start_time = datetime.now()
    
    try:
        engine = get_db_engine()
        print("\n--- 🛠️ กำลังสร้างตาราง Staging V2 (รองรับ Hash & Update Date) ---")
    except Exception as e:
        print(f"❌ DB Connection Failed: {e}")
        return

    try:
        with engine.begin() as conn:
            
            # Helper for SQL Types
            from sqlalchemy.types import Date, Numeric, Text
            
            # ==========================================
            # 1. สร้างตาราง staging_holdings
            # ==========================================
            print(f"1️⃣  กำลังสร้าง/รีเซ็ตตาราง: {TABLE_HOLDINGS} ...")
            df_holdings_empty = pd.DataFrame(columns=COLS_HOLDINGS)
            
            df_holdings_empty.to_sql(
                TABLE_HOLDINGS, 
                conn, 
                if_exists='replace', 
                index=False,
                dtype={
                    'scrape_date': Date(),
                    'source_update_date': Date(), # เก็บวันที่หน้าเว็บ
                    'row_hash': Text(),           # เก็บ Hash String
                    'pct_weight': Numeric(10, 4),
                    'shares': Numeric(20, 2)
                }
            )
            print(f"   ✅ สร้าง {TABLE_HOLDINGS} เรียบร้อย")

            # ==========================================
            # 2. สร้างตาราง staging_allocations
            # ==========================================
            print(f"2️⃣  กำลังสร้าง/รีเซ็ตตาราง: {TABLE_ALLOCATIONS} ...")
            df_alloc_empty = pd.DataFrame(columns=COLS_ALLOCATIONS)
            
            df_alloc_empty.to_sql(
                TABLE_ALLOCATIONS, 
                conn, 
                if_exists='replace', 
                index=False,
                dtype={
                    'scrape_date': Date(),
                    'source_update_date': Date(), # เก็บวันที่หน้าเว็บ
                    'row_hash': Text(),           # เก็บ Hash String
                    'pct_weight': Numeric(10, 4)
                }
            )
            print(f"   ✅ สร้าง {TABLE_ALLOCATIONS} เรียบร้อย")
            print(f"   👉 เพิ่มคอลัมน์: source_update_date, row_hash แล้ว")

    except Exception as e:
        print(f"\n❌ เกิดข้อผิดพลาด: {e}")
    finally:
        print(f"\n⏱️ เสร็จสิ้นใน: {datetime.now() - start_time}")

if __name__ == "__main__":
    force_create_staging_tables_v2()