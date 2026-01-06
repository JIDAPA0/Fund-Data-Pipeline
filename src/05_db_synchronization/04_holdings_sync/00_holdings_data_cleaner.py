import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Consolidate holdings-related CSVs from validation_output/Financial_Times/04_Holdings
# into staging under data/03_staging/holdings (flat, no date folder).

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

VALIDATION_DIR = BASE_DIR / "validation_output" / "Financial_Times" / "04_Holdings"

STAGING_DIR = BASE_DIR / "data" / "03_staging" / "holdings"
STAGING_DIR.mkdir(parents=True, exist_ok=True)


def load_and_normalize(pattern_dir: Path, allocation_type: str, outfile: str):
    files = list(pattern_dir.glob("*.csv"))
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        df.columns = [c.strip().lower() for c in df.columns]
        df["allocation_type"] = allocation_type
        frames.append(df)

    if not frames:
        print(f"⚠️ No files in {pattern_dir}")
        return

    df_all = pd.concat(frames, ignore_index=True)
    out_path = STAGING_DIR / outfile
    df_all.to_csv(out_path, index=False)
    print(f"✅ Saved cleaned {outfile} ({len(df_all)} rows)")


def main():
    load_and_normalize(
        VALIDATION_DIR / "Holdings",
        "holdings",
        "holdings_clean.csv",
    )
    load_and_normalize(
        VALIDATION_DIR / "Asset_Allocation",
        "asset_allocation",
        "allocations_clean.csv",
    )
    load_and_normalize(
        VALIDATION_DIR / "Sectors",
        "sector",
        "sectors_clean.csv",
    )
    load_and_normalize(
        VALIDATION_DIR / "Regions",
        "region",
        "regions_clean.csv",
    )


if __name__ == "__main__":
    main()
