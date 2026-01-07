import sys
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.db_connector import insert_dataframe, init_allocations_table, get_db_engine

HASHED_DIR = BASE_DIR / "data" / "04_hashed" / "holdings"


def ensure_table():
    engine = get_db_engine()
    init_allocations_table(engine)
    return engine


def load_file(filename: str, allocation_type: str):
    path = HASHED_DIR / filename
    if not path.exists():
        print(f"⚠️ Missing {path}")
        return
    df = pd.read_csv(path)
    if df.empty:
        print(f"⚠️ {path} empty")
        return

    df["allocation_type"] = allocation_type
    for col in [
        "ticker",
        "asset_type",
        "source",
        "allocation_type",
        "item_name",
        "value_net",
        "value_category_avg",
        "value_long",
        "value_short",
        "as_of_date",
        "row_hash",
        "updated_at",
    ]:
        if col not in df.columns:
            df[col] = None

    df = df[
        [
            "ticker",
            "asset_type",
            "source",
            "allocation_type",
            "item_name",
            "value_net",
            "value_category_avg",
            "value_long",
            "value_short",
            "as_of_date",
            "row_hash",
            "updated_at",
        ]
    ]
    df["item_name"] = df["item_name"].astype(str).str.strip()
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date
    for col in ["value_net", "value_category_avg", "value_long", "value_short"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["ticker", "asset_type", "source", "allocation_type", "item_name"])
    df = df.drop_duplicates(
        subset=["ticker", "asset_type", "source", "allocation_type", "item_name", "as_of_date"]
    )
    insert_dataframe(df, "stg_allocations")
    print(f"✅ Loaded allocations from {filename}: {len(df)} rows")


def main():
    ensure_table()
    load_file("allocations_hashed.csv", "asset_allocation")
    load_file("sectors_hashed.csv", "sector")
    load_file("regions_hashed.csv", "region")


if __name__ == "__main__":
    main()
