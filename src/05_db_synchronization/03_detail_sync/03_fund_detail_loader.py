import sys
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.db_connector import insert_dataframe, get_db_engine, init_fund_info_table, init_fund_fees_table, init_fund_risk_table, init_fund_policy_table

HASHED_DIR = BASE_DIR / "data" / "04_hashed" / "static_details"


def ensure_tables():
    engine = get_db_engine()
    init_fund_info_table(engine)
    init_fund_fees_table(engine)
    init_fund_risk_table(engine)
    init_fund_policy_table(engine)
    return engine


def load_file(filename: str, table: str, required_cols):
    path = HASHED_DIR / filename
    if not path.exists():
        print(f"⚠️ Missing {path}, skip.")
        return
    df = pd.read_csv(path)
    if df.empty:
        print(f"⚠️ {path} empty, skip.")
        return

    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    df = df[required_cols]

    # Coerce and clean common fields
    if "inception_date" in df.columns:
        df["inception_date"] = pd.to_datetime(df["inception_date"], errors="coerce")
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
        df["updated_at"] = df["updated_at"].fillna(pd.Timestamp.utcnow())
    if "shares_out" in df.columns:
        df["shares_out"] = pd.to_numeric(df["shares_out"], errors="coerce")

    insert_dataframe(df, table)
    print(f"✅ Loaded {len(df)} rows into {table}")


def main():
    ensure_tables()

    load_file(
        "fund_info_hashed.csv",
        "stg_fund_info",
        [
            "ticker",
            "asset_type",
            "source",
            "name",
            "isin_number",
            "cusip_number",
            "issuer",
            "category",
            "index_benchmark",
            "inception_date",
            "exchange",
            "region",
            "country",
            "leverage",
            "options",
            "shares_out",
            "market_cap_size",
            "investment_style",
            "row_hash",
            "updated_at",
        ],
    )

    load_file(
        "fund_fees_hashed.csv",
        "stg_fund_fees",
        [
            "ticker",
            "asset_type",
            "source",
            "expense_ratio",
            "initial_charge",
            "exit_charge",
            "assets_aum",
            "top_10_hold_pct",
            "holdings_count",
            "holdings_turnover",
            "row_hash",
            "updated_at",
        ],
    )

    load_file(
        "fund_risk_hashed.csv",
        "stg_fund_risk",
        [
            "ticker",
            "asset_type",
            "source",
            "sharpe_ratio_1y",
            "sharpe_ratio_3y",
            "sharpe_ratio_5y",
            "sharpe_ratio_10y",
            "beta_1y",
            "beta_3y",
            "beta_5y",
            "beta_10y",
            "alpha_1y",
            "alpha_3y",
            "alpha_5y",
            "alpha_10y",
            "standard_dev_1y",
            "standard_dev_3y",
            "standard_dev_5y",
            "standard_dev_10y",
            "r_squared_1y",
            "r_squared_3y",
            "r_squared_5y",
            "r_squared_10y",
            "rsi_daily",
            "moving_avg_200",
            "morningstar_rating",
            "lipper_total_return_3y",
            "lipper_total_return_5y",
            "lipper_total_return_10y",
            "lipper_total_return_overall",
            "lipper_consistent_return_3y",
            "lipper_consistent_return_5y",
            "lipper_consistent_return_10y",
            "lipper_consistent_return_overall",
            "lipper_preservation_3y",
            "lipper_preservation_5y",
            "lipper_preservation_10y",
            "lipper_preservation_overall",
            "lipper_expense_3y",
            "lipper_expense_5y",
            "lipper_expense_10y",
            "lipper_expense_overall",
            "row_hash",
            "updated_at",
        ],
    )

    load_file(
        "fund_policy_hashed.csv",
        "stg_fund_policy",
        [
            "ticker",
            "asset_type",
            "source",
            "dividend_yield",
            "dividend_growth_1y",
            "dividend_growth_3y",
            "dividend_growth_5y",
            "dividend_growth_10y",
            "dividend_consecutive_years",
            "payout_ratio",
            "total_return_ytd",
            "total_return_1y",
            "pe_ratio",
            "row_hash",
            "updated_at",
        ],
    )


if __name__ == "__main__":
    main()
