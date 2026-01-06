import sys
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

# ==========================================
# 0. SETUP (Debug Mode)
# ==========================================
print("🚀 Loader Initializing...")

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
    from src.utils.db_connector import insert_dataframe, test_connection
except ImportError as e:
    print(f"❌ Import Error: {e}")
    sys.exit(1)

# ==========================================
# 1. CONFIGURATION
# ==========================================
INPUT_DIR = DATA_STORE_DIR / "03_staging"
INPUT_FILE = INPUT_DIR / "validated_daily_nav.csv"
TABLE_NAME = "stg_daily_nav"

# ==========================================
# 2. CORE LOGIC
# ==========================================

def main():
    print("🔌 Testing DB Connection...")
    if not test_connection():
        print("❌ DB Connection Failed. Aborting.")
        return

    if not INPUT_FILE.exists():
        print(f"❌ Input file not found: {INPUT_FILE}")
        print("   Did you run '01_performance_validator.py'?")
        return

    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"📄 Loaded: {INPUT_FILE.name} ({len(df)} rows)")
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return

    if df.empty:
        print("⚠️ Input file is empty. Nothing to upload.")
        return

    print(f"📤 Uploading to table '{TABLE_NAME}'...")
    print("   (This might take a moment...)")
    
    try:
        insert_dataframe(df, TABLE_NAME)
        
        print("="*40)
        print(f"✅ SUCCESS: Uploaded {len(df)} rows to DB.")
        print("="*40)
        
    except Exception as e:
        print(f"❌ Upload Failed: {e}")

if __name__ == "__main__":
    main()
