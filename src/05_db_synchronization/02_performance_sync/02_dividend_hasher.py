import pandas as pd
import hashlib
from datetime import datetime
from pathlib import Path

# CONFIG
STAGING_DIR = Path("data/03_staging/dividend_history")
HASHED_DIR = Path("data/04_hashed/dividend_history")
HASHED_DIR.mkdir(parents=True, exist_ok=True)

def calculate_dvd_hash(row):
    
    combined = f"{row.get('ex_date', '')}{row.get('amount', '')}{row.get('type', '')}"
    return hashlib.sha256(combined.encode()).hexdigest()

def run_hashing():
    files = list(STAGING_DIR.rglob("*.csv"))
    for f in files:
        df = pd.read_csv(f)
        if df.empty: continue
        df['row_hash'] = df.apply(calculate_dvd_hash, axis=1)
        df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        rel_path = f.relative_to(STAGING_DIR)
        save_path = HASHED_DIR / rel_path
        save_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(save_path, index=False)

if __name__ == "__main__":
    run_hashing()
    print("✅ Dividend Hashing Completed")
