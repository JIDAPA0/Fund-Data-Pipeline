import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

STAGING_DIR = BASE_DIR / "data" / "03_staging" / "holdings"

FILES = {
    "holdings_clean.csv": ("holdings_validated.csv", ["ticker", "item_name"]),
    "allocations_clean.csv": ("allocations_validated.csv", ["ticker", "item_name"]),
    "sectors_clean.csv": ("sectors_validated.csv", ["ticker", "item_name"]),
    "regions_clean.csv": ("regions_validated.csv", ["ticker", "item_name"]),
    "fund_metrics_clean.csv": ("fund_metrics_validated.csv", ["ticker", "metric_type", "metric_name"]),
}


def validate(src, dst, required_cols):
    src_path = STAGING_DIR / src
    if not src_path.exists():
        print(f"⚠️ Missing {src_path}")
        return
    df = pd.read_csv(src_path)
    if df.empty:
        print(f"⚠️ {src_path} empty")
        return
    for col in required_cols:
        if col in df.columns:
            df = df.dropna(subset=[col])
    df.to_csv(STAGING_DIR / dst, index=False)
    print(f"✅ Validated {dst} ({len(df)} rows)")


def main():
    for src, (dst, required_cols) in FILES.items():
        validate(src, dst, required_cols)


if __name__ == "__main__":
    main()
