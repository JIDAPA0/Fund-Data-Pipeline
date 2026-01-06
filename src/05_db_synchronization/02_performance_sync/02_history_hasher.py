import sys
import os
import pandas as pd
import hashlib
from datetime import datetime
from pathlib import Path

# ==========================================
# 0. SETUP
# ==========================================
print("🚀 Script: Price History Hasher Starting...")

current_file = Path(__file__).resolve()
project_root = current_file.parent
while not (project_root / 'src').exists():
    if project_root == project_root.parent: break
    project_root = project_root.parent
sys.path.append(str(project_root))

# ==========================================
# 1. CONFIGURATION
# ==========================================
TIMESTAMP = datetime.now().strftime('%Y-%m-%d')
STAGING_DIR = project_root / "data" / "03_staging" / "price_history"
HASHED_DIR = project_root / "data" / "04_hashed" / "price_history" / TIMESTAMP

# ==========================================
# 2. HASHING FUNCTION
# ==========================================

def calculate_row_hash(row):
    """
    คำนวณค่า Hash จากข้อมูลราคา เพื่อใช้เช็คความเปลี่ยนแปลงใน DB
    """
    hash_columns = ['open', 'high', 'low', 'close', 'adj_close', 'volume']
    
    combined = "".join([str(row.get(col, "")) for col in hash_columns])
    
    return hashlib.sha256(combined.encode()).hexdigest()

def process_hashing():
    print(f"🔍 Scanning cleaned files in: {STAGING_DIR}")
    
    all_clean_files = list(STAGING_DIR.rglob("*.csv"))
    
    if not all_clean_files:
        print("⚠️ No cleaned history files found to hash.")
        return

    processed_count = 0
    for csv_file in all_clean_files:
        try:

            df = pd.read_csv(csv_file, low_memory=False)
            
            if df.empty: continue

            df['row_hash'] = df.apply(calculate_row_hash, axis=1)
            
            df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            rel_path = csv_file.relative_to(STAGING_DIR)
            save_path = HASHED_DIR / rel_path
            save_path.parent.mkdir(parents=True, exist_ok=True)
            
            df.to_csv(save_path, index=False)
            processed_count += 1
            
            if processed_count % 100 == 0:
                print(f"   ✅ Hashed {processed_count} files...")

        except Exception as e:
            print(f"   ❌ Error hashing {csv_file.name}: {e}")

    print(f"✨ Hashing Completed: {processed_count} files are ready for Database Upload.")

if __name__ == "__main__":
    process_hashing()