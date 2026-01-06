import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.hasher import calculate_row_hash

TODAY = datetime.now().strftime("%Y-%m-%d")
STAGING_DIR = BASE_DIR / "data" / "03_static_details" / TODAY
HASHED_DIR = BASE_DIR / "data" / "04_hashed" / "static_details" / TODAY
HASHED_DIR.mkdir(parents=True, exist_ok=True)

FILES = [
    ("fund_info_validated.csv", "fund_info_hashed.csv"),
    ("fund_fees_validated.csv", "fund_fees_hashed.csv"),
    ("fund_risk_validated.csv", "fund_risk_hashed.csv"),
    ("fund_policy_validated.csv", "fund_policy_hashed.csv"),
]


def add_hash(df: pd.DataFrame) -> pd.DataFrame:
    # Use all columns except existing hash/update to compute a deterministic hash
    cols = [c for c in df.columns if c not in ["row_hash", "updated_at"]]
    df["row_hash"] = df[cols].astype(str).apply(lambda row: calculate_row_hash(*row), axis=1)
    df["updated_at"] = datetime.utcnow()
    return df


def process_file(src_name: str, dst_name: str):
    src_path = STAGING_DIR / src_name
    if not src_path.exists():
        print(f"⚠️ Missing {src_path}, skip.")
        return

    df = pd.read_csv(src_path)
    if df.empty:
        print(f"⚠️ {src_path} empty, skip.")
        return

    df = add_hash(df)
    out_path = HASHED_DIR / dst_name
    df.to_csv(out_path, index=False)
    print(f"✅ Hashed: {out_path} ({len(df)} rows)")


def main():
    for src, dst in FILES:
        process_file(src, dst)


if __name__ == "__main__":
    main()
