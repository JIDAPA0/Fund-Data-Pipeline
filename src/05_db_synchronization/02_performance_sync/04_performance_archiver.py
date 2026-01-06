import sys
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

# ==========================================
# 0. SETUP
# ==========================================
print("📦 Archiver Initializing...")

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
SOURCE_DIR = DATA_STORE_DIR / "03_staging"
ARCHIVE_DIR = DATA_STORE_DIR / "99_archive" / "daily_nav"
ZIP_NAME = f"daily_nav_processed_{CURRENT_DATE}.zip"

# ==========================================
# 2. CORE LOGIC
# ==========================================

def archive_files():
    if not SOURCE_DIR.exists():
        print(f"⚠️ Source directory not found: {SOURCE_DIR}")
        return

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = ARCHIVE_DIR / ZIP_NAME

    csv_files = list(SOURCE_DIR.glob("*daily_nav*.csv"))
    
    if not csv_files:
        print("⚠️ No CSV files found to archive.")
        try:
            SOURCE_DIR.rmdir()
            print("🗑️  Removed empty source folder.")
        except: pass
        return

    print(f"🗜️  Zipping {len(csv_files)} files to: {zip_path.name}")

    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in csv_files:
                zipf.write(file_path, arcname=file_path.name)
                print(f"   ➕ Added to zip: {file_path.name}")
        
        print("   ✅ Zip created successfully.")

        
    except Exception as e:
        print(f"❌ Archiving Failed: {e}")
        return

    print("-" * 30)
    print(f"🏁 Archived successfully to: {zip_path}")

if __name__ == "__main__":
    archive_files()
