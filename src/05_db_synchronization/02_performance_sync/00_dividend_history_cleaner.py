import sys
import os
import pandas as pd
from pathlib import Path

current_file = Path(__file__).resolve()
project_root = current_file.parent
while not (project_root / 'src').exists():
    project_root = project_root.parent
sys.path.append(str(project_root))

DATA_DIR = project_root / "validation_output"
STAGING_DIR = project_root / "data" / "03_staging" / "dividend_history"

SOURCES = {
    'sa': DATA_DIR / "Stock_Analysis" / "02_Price_And_Dividend_History" / "Dividend_History",
    'yf': DATA_DIR / "Yahoo_Finance" / "02_Price_And_Dividend_History" / "Dividend_History"
}

def clean_dvd():
    for skey, spath in SOURCES.items():
        if not spath.exists(): continue
        print(f"📂 Cleaning Dividends from: {skey.upper()}")
        
        files = list(spath.rglob("*.csv"))
        for f in files:
            try:
                df = pd.read_csv(f)
                df.columns = [c.strip().lower() for c in df.columns]
                
                
                df.insert(0, 'source', 'Stock Analysis' if skey == 'sa' else 'Yahoo Finance')
                df.insert(0, 'asset_type', 'ETF' if 'etf' in str(f).lower() else 'FUND')
                df.insert(0, 'ticker', f.stem.split('_')[0].upper())
                
                
                date_col = next((c for c in ['date', 'ex_date'] if c in df.columns), None)
                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
                    df = df.rename(columns={date_col: 'ex_date'})

                save_path = STAGING_DIR / skey / f.name
                save_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(save_path, index=False)
            except Exception as e:
                print(f"❌ Error {f.name}: {e}")

if __name__ == "__main__":
    clean_dvd()
    print("✅ Dividend Cleaning Completed")
