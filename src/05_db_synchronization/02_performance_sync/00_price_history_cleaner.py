import sys
import os
import pandas as pd
from pathlib import Path

# ==========================================
# 0. SETUP
# ==========================================
print("🚀 Script: Price History Cleaner (Focus Mode)")

current_file = Path(__file__).resolve()
project_root = current_file.parent
while not (project_root / 'src').exists():
    if project_root == project_root.parent: break
    project_root = project_root.parent
sys.path.append(str(project_root))

# ==========================================
# 1. CONFIGURATION
# ==========================================
DATA_PERFORMANCE_DIR = project_root / "validation_output"
CLEAN_BASE_DIR = project_root / "data" / "03_staging"


SOURCES = ['sa', 'yf', 'ft']
source_config = {
    'ft': {'path': DATA_PERFORMANCE_DIR / "Financial_Times", 'name': 'Financial Times'},
    'yf': {'path': DATA_PERFORMANCE_DIR / "Yahoo_Finance", 'name': 'Yahoo Finance'},
    'sa': {'path': DATA_PERFORMANCE_DIR / "Stock_Analysis", 'name': 'Stock Analysis'}
}

# ==========================================
# 2. CORE LOGIC
# ==========================================

def process_history(csv_path, source_name, source_key):
    try:
        
        df = pd.read_csv(csv_path, low_memory=False)
        df.columns = [c.strip().lower() for c in df.columns]
        
        
        ticker = csv_path.stem.split('_')[0].upper()
        
        asset_type = 'ETF' if 'etf' in str(csv_path).lower() else 'FUND'
        
        
        if 'source' not in df.columns: df.insert(0, 'source', source_name)
        if 'asset_type' not in df.columns: df.insert(0, 'asset_type', asset_type)
        if 'ticker' not in df.columns: df.insert(0, 'ticker', ticker)

        
        date_col = next((c for c in ['date', 'as_of_date'] if c in df.columns), None)
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
            if date_col != 'date':
                df = df.rename(columns={date_col: 'date'})

        
        save_dir = CLEAN_BASE_DIR / "price_history" / source_key
        save_dir.mkdir(parents=True, exist_ok=True)
        
        
        df.to_csv(save_dir / csv_path.name, index=False)
        return True
    except Exception as e:
        print(f"      ❌ Skip {csv_path.name}: {e}")
        return False

def main():
    for skey in SOURCES:
        config = source_config[skey]
        print(f"\n📂 Scanning {config['name']}...")
        if not config['path'].exists(): continue
        
        
        files = list(config['path'].rglob("*.csv"))
        count = 0
        for f in files:
            fname = f.name.lower()
            
            if any(x in fname for x in ["history", "historical"]) and "holdings" not in fname:
                if process_history(f, config['name'], skey):
                    count += 1
                    if count % 500 == 0: print(f"   ✅ Cleaned {count} files...")
        
        print(f"✨ Finished {config['name']}: {count} files ready for DB.")

if __name__ == "__main__":
    main()
