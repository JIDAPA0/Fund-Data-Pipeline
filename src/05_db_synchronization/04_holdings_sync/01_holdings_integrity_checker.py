import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

TODAY = datetime.now().strftime("%Y-%m-%d")
STAGING_DIR = BASE_DIR / "data" / "03_staging" / "holdings" / TODAY

FILES = {
    "holdings_clean.csv": "holdings_validated.csv",
    "allocations_clean.csv": "allocations_validated.csv",
    "sectors_clean.csv": "sectors_validated.csv",
    "regions_clean.csv": "regions_validated.csv",
}


def validate(src, dst):
    src_path = STAGING_DIR / src
    if not src_path.exists():
        print(f"⚠️ Missing {src_path}")
        return
    df = pd.read_csv(src_path)
    if df.empty:
        print(f"⚠️ {src_path} empty")
        return
    # Keep rows with ticker and item_name/value_net present
    if "ticker" in df.columns:
        df = df.dropna(subset=["ticker"])
    if "item_name" in df.columns:
        df = df.dropna(subset=["item_name"])
    df.to_csv(STAGING_DIR / dst, index=False)
    print(f"✅ Validated {dst} ({len(df)} rows)")


def main():
    for src, dst in FILES.items():
        validate(src, dst)


if __name__ == "__main__":
    main()
