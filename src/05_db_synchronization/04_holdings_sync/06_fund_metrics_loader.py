import sys
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.db_connector import insert_dataframe, get_db_engine, init_fund_metrics_table

HASHED_DIR = BASE_DIR / "data" / "04_hashed" / "holdings"


def ensure_table():
    engine = get_db_engine()
    init_fund_metrics_table(engine)
    return engine


def load_metrics():
    path = HASHED_DIR / "fund_metrics_hashed.csv"
    if not path.exists():
        print(f"⚠️ Missing {path}")
        return
    df = pd.read_csv(path)
    if df.empty:
        print(f"⚠️ {path} empty")
        return

    for col in [
        "ticker",
        "asset_type",
        "source",
        "metric_type",
        "metric_name",
        "column_name",
        "value_raw",
        "value_num",
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
            "metric_type",
            "metric_name",
            "column_name",
            "value_raw",
            "value_num",
            "as_of_date",
            "row_hash",
            "updated_at",
        ]
    ]
    df["metric_name"] = df["metric_name"].astype(str).str.strip()
    df["column_name"] = df["column_name"].astype(str).str.strip()
    df.loc[df["column_name"].isin(["", "nan", "None"]), "column_name"] = None
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date
    df["value_num"] = pd.to_numeric(df["value_num"], errors="coerce")
    df = df.dropna(subset=["ticker", "asset_type", "source", "metric_type", "metric_name"])
    df = df.drop_duplicates(
        subset=["ticker", "asset_type", "source", "metric_type", "metric_name", "column_name", "as_of_date"]
    )
    insert_dataframe(df, "stg_fund_metrics")
    print(f"✅ Loaded fund metrics: {len(df)} rows")


def main():
    ensure_table()
    load_metrics()


if __name__ == "__main__":
    main()
