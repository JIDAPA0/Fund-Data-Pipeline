import sys
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.db_connector import insert_dataframe, get_db_engine, init_fund_holdings_table

HASHED_DIR = BASE_DIR / "data" / "04_hashed" / "holdings"


def ensure_tables():
    engine = get_db_engine()
    init_fund_holdings_table(engine)
    return engine


def load_holdings():
    path = HASHED_DIR / "holdings_hashed.csv"
    if not path.exists():
        print(f"⚠️ Missing {path}")
        return
    df = pd.read_csv(path)
    if df.empty:
        print(f"⚠️ {path} empty")
        return

    # Map to stg_fund_holdings schema
    rename_map = {
        "item_name": "holding_name",
        "value_net": "holding_percentage",
    }
    df = df.rename(columns=rename_map)
    for col in [
        "holding_ticker",
        "holding_name",
        "holding_percentage",
        "shares_held",
        "market_value",
        "sector",
        "country",
        "as_of_date",
        "row_hash",
        "updated_at",
        "ticker",
        "asset_type",
        "source",
    ]:
        if col not in df.columns:
            df[col] = None
    df = df[
        [
            "ticker",
            "asset_type",
            "source",
            "holding_ticker",
            "holding_name",
            "holding_percentage",
            "shares_held",
            "market_value",
            "sector",
            "country",
            "as_of_date",
            "row_hash",
            "updated_at",
        ]
    ]
    df["holding_name"] = df["holding_name"].astype(str).str.strip()
    df["holding_ticker"] = df["holding_ticker"].astype(str).str.strip()
    df.loc[df["holding_ticker"].isin(["", "nan", "None"]), "holding_ticker"] = None
    df.loc[df["holding_ticker"].str.len() > 20, "holding_ticker"] = None
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date
    for col in ["holding_percentage", "shares_held", "market_value"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["ticker", "asset_type", "source", "holding_name"])
    df = df.drop_duplicates(
        subset=["ticker", "asset_type", "source", "holding_name", "as_of_date"]
    )
    insert_dataframe(df, "stg_fund_holdings")
    print(f"✅ Loaded holdings: {len(df)} rows")


def main():
    ensure_tables()
    load_holdings()


if __name__ == "__main__":
    main()
