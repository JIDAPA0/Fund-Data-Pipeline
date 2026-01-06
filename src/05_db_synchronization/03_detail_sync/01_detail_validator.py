import sys
from pathlib import Path
import pandas as pd

# Basic validator: drop rows missing ticker/source, normalize asset_type, and re-save validated files.

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

STAGING_DIR = BASE_DIR / "data" / "03_static_details"

FILES = {
    "fund_info_clean.csv": "fund_info_validated.csv",
    "fund_fees_clean.csv": "fund_fees_validated.csv",
    "fund_risk_clean.csv": "fund_risk_validated.csv",
    "fund_policy_clean.csv": "fund_policy_validated.csv",
}


def validate_file(src_name, dst_name):
    src_path = STAGING_DIR / src_name
    if not src_path.exists():
        print(f"⚠️ Missing {src_path}, skip.")
        return

    df = pd.read_csv(src_path)
    if df.empty:
        print(f"⚠️ {src_path} empty, skip.")
        return

    # Drop rows without ticker/source and standardize asset_type casing
    df = df.dropna(subset=["ticker", "source"])
    if "asset_type" in df.columns:
        df["asset_type"] = df["asset_type"].fillna("").str.upper()

    out_path = STAGING_DIR / dst_name
    df.to_csv(out_path, index=False)
    print(f"✅ Validated: {out_path} ({len(df)} rows)")


def main():
    for src, dst in FILES.items():
        validate_file(src, dst)


if __name__ == "__main__":
    main()
