import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.hasher import calculate_row_hash

STAGING_DIR = BASE_DIR / "data" / "03_staging" / "holdings"
HASHED_DIR = BASE_DIR / "data" / "04_hashed" / "holdings"
HASHED_DIR.mkdir(parents=True, exist_ok=True)

FILES = {
    "holdings_validated.csv": "holdings_hashed.csv",
    "allocations_validated.csv": "allocations_hashed.csv",
    "sectors_validated.csv": "sectors_hashed.csv",
    "regions_validated.csv": "regions_hashed.csv",
    "fund_metrics_validated.csv": "fund_metrics_hashed.csv",
}


def hash_file(src, dst):
    src_path = STAGING_DIR / src
    if not src_path.exists():
        print(f"⚠️ Missing {src_path}")
        return
    df = pd.read_csv(src_path)
    if df.empty:
        print(f"⚠️ {src_path} empty")
        return

    cols = [c for c in df.columns if c not in ["row_hash", "updated_at"]]
    df["row_hash"] = df[cols].astype(str).apply(lambda row: calculate_row_hash(*row), axis=1)
    df["updated_at"] = datetime.utcnow()
    out_path = HASHED_DIR / dst
    df.to_csv(out_path, index=False)
    print(f"✅ Hashed {dst} ({len(df)} rows)")


def main():
    for src, dst in FILES.items():
        hash_file(src, dst)


if __name__ == "__main__":
    main()
