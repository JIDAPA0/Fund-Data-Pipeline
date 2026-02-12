import sys
from pathlib import Path

import pandas as pd

print("Script: Price History Loader Starting...")

current_file = Path(__file__).resolve()
project_root = current_file.parent
while not (project_root / "src").exists():
    if project_root == project_root.parent:
        break
    project_root = project_root.parent
sys.path.append(str(project_root))

from src.utils.db_connector import get_db_engine, init_price_history_table, insert_dataframe

HASHED_BASE_DIR = project_root / "data" / "04_hashed" / "price_history"
TARGET_TABLE = "stg_price_history"


def normalize_price_history(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "adj close": "adj_close",
        "Adj Close": "adj_close",
        "change %": "change_pct",
    }

    df = df.rename(columns=lambda c: c.strip())
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "change_pct" in df.columns:
        df = df.drop(columns=["change_pct"])

    required_cols = [
        "ticker",
        "asset_type",
        "source",
        "date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "row_hash",
        "updated_at",
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
        df["updated_at"] = df["updated_at"].fillna(pd.Timestamp.utcnow())
    else:
        df["updated_at"] = pd.Timestamp.utcnow()

    df["row_hash"] = df["row_hash"].fillna("").astype(str).str.strip()
    df = df[df["row_hash"] != ""]

    df = df[required_cols]
    df = df.dropna(subset=["ticker", "asset_type", "source", "date"])
    df = df.drop_duplicates(subset=["ticker", "asset_type", "source", "date"])
    return df


def main():
    engine = get_db_engine()
    init_price_history_table(engine)

    print(f"Scanning hashed files in: {HASHED_BASE_DIR}")
    all_hashed_files = list(HASHED_BASE_DIR.rglob("*.csv"))

    if not all_hashed_files:
        print("No hashed files found to upload.")
        return

    total_rows = 0
    file_count = 0

    for csv_file in all_hashed_files:
        try:
            df = pd.read_csv(csv_file)
            if df.empty:
                continue

            cleaned = normalize_price_history(df)
            if cleaned.empty:
                continue

            insert_dataframe(cleaned, TARGET_TABLE)
            total_rows += len(cleaned)
            file_count += 1

            if file_count % 50 == 0:
                print(f"Uploaded {file_count} files... (Total rows: {total_rows})")

        except Exception as e:
            print(f"Error uploading {csv_file.name}: {e}")

    print("=" * 30)
    print("LOAD COMPLETED")
    print(f"Total files processed: {file_count}")
    print(f"Total rows upserted: {total_rows}")
    print("=" * 30)


if __name__ == "__main__":
    main()
