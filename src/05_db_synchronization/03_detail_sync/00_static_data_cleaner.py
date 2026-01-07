import sys
from pathlib import Path
import pandas as pd

# Consolidates static detail CSVs into data/03_static_details (flat, no date folder).

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

VALIDATION_ROOT = BASE_DIR / "validation_output"
OUTPUT_DIR = BASE_DIR / "data" / "03_static_details"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INFO_FILES = list((VALIDATION_ROOT).glob("*/03_Detail_Static/*fund_info.csv"))
FEES_FILES = list((VALIDATION_ROOT).glob("*/03_Detail_Static/*fund_fees.csv"))
RISK_FILES = list((VALIDATION_ROOT).glob("*/03_Detail_Static/*fund_risk.csv"))
POLICY_FILES = list((VALIDATION_ROOT).glob("*/03_Detail_Static/*fund_policy.csv"))

RISK_NUMERIC_COLS = [
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
]

def _normalize_percent(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")


def _normalize_number(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace(",", "", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")


def load_and_normalize(
    files,
    expected_cols,
    filename,
    rename_map=None,
    percent_cols=None,
    numeric_cols=None,
    percent_scale_cols=None,
):
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        df.columns = [c.strip().lower() for c in df.columns]
        if rename_map:
            rename_subset = {k: v for k, v in rename_map.items() if k in df.columns}
            if rename_subset:
                df = df.rename(columns=rename_subset)
        if "source" in df.columns:
            df["source"] = df["source"].fillna(f.parent.parent.name.replace("_", " "))
        else:
            df["source"] = f.parent.parent.name.replace("_", " ")
        asset_type = df["asset_type"] if "asset_type" in df.columns else None
        if asset_type is not None:
            df["asset_type"] = asset_type.astype(str).str.upper().fillna("ETF")
        else:
            df["asset_type"] = "ETF"
        if percent_cols:
            for col in percent_cols:
                if col in df.columns:
                    df[col] = _normalize_percent(df[col])
        if percent_scale_cols:
            for col in percent_scale_cols:
                if col in df.columns:
                    df[col] = df[col] / 100
        if filename == "fund_risk_clean.csv":
            for col in ["standard_dev_1y", "standard_dev_3y", "standard_dev_5y", "standard_dev_10y"]:
                if col in df.columns:
                    df[col] = df[col].where(df[col].abs() <= 999.99, df[col] / 100)
        if filename == "fund_policy_clean.csv":
            for col in ["total_return_1y", "total_return_ytd"]:
                if col in df.columns:
                    df[col] = df[col].where(df[col].abs() <= 999.99, df[col] / 100)
        if numeric_cols:
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = _normalize_number(df[col])

        frames.append(df)

    if not frames:
        print(f"⚠️ ไม่มีไฟล์สำหรับ {filename}")
        return

    df_all = pd.concat(frames, ignore_index=True)
    for col in expected_cols:
        if col not in df_all.columns:
            df_all[col] = None
    df_all = df_all[expected_cols]
    out_path = OUTPUT_DIR / filename
    df_all.to_csv(out_path, index=False)
    print(f"✅ Saved cleaned file: {out_path} ({len(df_all)} rows)")


def main():
    load_and_normalize(
        INFO_FILES,
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
        ],
        "fund_info_clean.csv",
    )

    load_and_normalize(
        FEES_FILES,
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
        ],
        "fund_fees_clean.csv",
        percent_cols=[
            "expense_ratio",
            "initial_charge",
            "exit_charge",
            "top_10_hold_pct",
            "holdings_turnover",
        ],
        percent_scale_cols=["expense_ratio", "initial_charge", "exit_charge"],
        numeric_cols=["assets_aum", "holdings_count"],
    )

    load_and_normalize(
        RISK_FILES,
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
        ],
        "fund_risk_clean.csv",
        percent_cols=RISK_NUMERIC_COLS,
    )

    load_and_normalize(
        POLICY_FILES,
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
        ],
        "fund_policy_clean.csv",
        rename_map={
            "div_yield": "dividend_yield",
            "div_growth_1y": "dividend_growth_1y",
            "div_growth_3y": "dividend_growth_3y",
            "div_growth_5y": "dividend_growth_5y",
            "div_growth_10y": "dividend_growth_10y",
            "div_consecutive_years": "dividend_consecutive_years",
        },
        percent_cols=[
            "dividend_yield",
            "dividend_growth_1y",
            "dividend_growth_3y",
            "dividend_growth_5y",
            "dividend_growth_10y",
            "payout_ratio",
            "total_return_ytd",
            "total_return_1y",
        ],
        numeric_cols=["dividend_consecutive_years", "pe_ratio"],
    )


if __name__ == "__main__":
    main()
