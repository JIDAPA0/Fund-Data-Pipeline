import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ==========================================
# 0. SETUP (Debug Mode)
# ==========================================
print("🚀 Validator Initializing...")

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
    from src.utils.path_manager import DATA_STORE_DIR
except ImportError as e:
    print(f"❌ Import Error: {e}")
    sys.exit(1)

# ==========================================
# 1. CONFIGURATION
# ==========================================
CURRENT_DATE = datetime.now().strftime('%Y-%m-%d')
# CURRENT_DATE = '2025-12-26' # <--- ถ้าจะรันย้อนหลัง แก้ตรงนี้

INPUT_DIR = DATA_STORE_DIR / "03_staging" / "daily_nav" / CURRENT_DATE
INPUT_FILE = INPUT_DIR / f"merged_daily_nav_{CURRENT_DATE}.csv"

OUTPUT_FILE = INPUT_DIR / f"validated_daily_nav_{CURRENT_DATE}.csv"
ERROR_FILE = INPUT_DIR / f"validation_errors_{CURRENT_DATE}.csv"

# ==========================================
# 2. CORE LOGIC
# ==========================================

def validate_data(df):
    print(f"🔍 Validating {len(df)} rows...")
    
    # 1. Check Missing Data
    # ข้อมูลสำคัญห้ามหาย: Ticker, Asset Type, Source, NAV, Date
    critical_cols = ['ticker', 'asset_type', 'source', 'nav_price', 'as_of_date']
    missing_mask = df[critical_cols].isnull().any(axis=1)
    
    # 2. Check Non-Positive NAV (ราคา <= 0)
    negative_nav_mask = df['nav_price'] <= 0
    
    # 3. Check Future Dates (ราคาในอนาคต)
    today = datetime.now() + timedelta(days=1) # เผื่อ Timezone นิดหน่อย
    df['as_of_date_dt'] = pd.to_datetime(df['as_of_date'], errors='coerce')
    future_date_mask = df['as_of_date_dt'] > today
    
    # รวม Error ทั้งหมด
    error_mask = missing_mask | negative_nav_mask | future_date_mask
    
    # แยกข้อมูล ดี vs เสีย
    df_valid = df[~error_mask].copy()
    df_error = df[error_mask].copy()
    
    # ระบุสาเหตุที่เสีย (เพื่อเอาไปดู Log ทีหลัง)
    if not df_error.empty:
        df_error.loc[missing_mask, 'error_reason'] = 'Missing Data'
        df_error.loc[negative_nav_mask, 'error_reason'] = 'Non-Positive NAV'
        df_error.loc[future_date_mask, 'error_reason'] = 'Future Date'
    
    # ลบคอลัมน์ช่วยคำนวณทิ้ง
    df_valid = df_valid.drop(columns=['as_of_date_dt'], errors='ignore')
    df_error = df_error.drop(columns=['as_of_date_dt'], errors='ignore')

    print(f"   ✅ Valid Rows: {len(df_valid)}")
    print(f"   ❌ Invalid Rows: {len(df_error)}")
    
    return df_valid, df_error

def main():
    if not INPUT_FILE.exists():
        print(f"❌ Input file not found: {INPUT_FILE}")
        print("   Please run '00_performance_data_cleaner.py' first.")
        return

    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"📄 Loaded: {INPUT_FILE.name}")
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return

    if df.empty:
        print("⚠️ Input file is empty.")
        return

    df_valid, df_error = validate_data(df)

    # Save Valid Data (ตัวจริงที่จะเอาไปใช้)
    if not df_valid.empty:
        df_valid.to_csv(OUTPUT_FILE, index=False)
        print(f"💾 Saved Valid Data: {OUTPUT_FILE.name}")
    
    # Save Errors (ตัวเสีย เก็บไว้ดูเล่น)
    if not df_error.empty:
        df_error.to_csv(ERROR_FILE, index=False)
        print(f"⚠️ Saved {len(df_error)} errors to: {ERROR_FILE.name}")
    else:
        print("✨ Perfect! No errors found.")

    print("✅ Module 01 (Validator) Completed.")

if __name__ == "__main__":
    main()