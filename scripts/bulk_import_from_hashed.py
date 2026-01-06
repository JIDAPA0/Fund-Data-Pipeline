import argparse
import hashlib
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.db_connector import insert_dataframe  # noqa: E402


def iter_files(root: Path, pattern: str, max_files: int | None):
    count = 0
    for p in root.rglob(pattern):
        if max_files and count >= max_files:
            break
        if p.is_file():
            yield p
            count += 1


def load_price_history(root: Path, max_files: int | None):
    loaded = 0
    for csv_file in iter_files(root, "*.csv", max_files):
        try:
            df = pd.read_csv(csv_file)
        except Exception as e:
            print(f"❌ Read error {csv_file}: {e}")
            continue
        if df.empty:
            continue

        df = df.rename(columns=lambda c: c.strip())
        rename_map = {
            "adj close": "adj_close",
            "Adj Close": "adj_close",
            "change %": "change_pct",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        if "change_pct" in df.columns:
            df = df.drop(columns=["change_pct"])
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"].astype(str).str.replace(",", ""), errors="coerce")
        if "updated_at" in df.columns:
            df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
            df["updated_at"] = df["updated_at"].fillna(pd.Timestamp.utcnow())
        else:
            df["updated_at"] = pd.Timestamp.utcnow()
        if "row_hash" in df.columns:
            df["row_hash"] = df["row_hash"].fillna("").astype(str).str.strip()
            df = df[df["row_hash"] != ""]
            df = df.drop_duplicates(subset=["row_hash"])
        if df.empty:
            continue

        insert_dataframe(df, "stg_price_history")
        loaded += len(df)
    return loaded


def load_dividends(root: Path, max_files: int | None):
    loaded = 0
    for csv_file in iter_files(root, "*.csv", max_files):
        try:
            df = pd.read_csv(csv_file)
        except Exception as e:
            print(f"❌ Read error {csv_file}: {e}")
            continue
        if df.empty:
            continue

        df = df.rename(columns=lambda c: c.strip())
        rename_map = {
            "ex_dividend_date": "ex_date",
            "pay_date": "payment_date",
            "payment_date": "payment_date",
            "cash_amount": "amount",
            "ex_date": "ex_date",
            "dividend": "amount",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        # Drop fields not in schema
        for drop_col in ["declaration_date", "record_date"]:
            if drop_col in df.columns:
                df = df.drop(columns=[drop_col])
        if "amount" in df.columns:
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        for col in ["ex_date", "payment_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        if "row_hash" not in df.columns or df["row_hash"].nunique() <= 1:
            def build_hash(row):
                parts = [
                    str(row.get("ticker", "")).strip().lower(),
                    str(row.get("asset_type", "")).strip().lower(),
                    str(row.get("source", "")).strip(),
                    row.get("ex_date").date().isoformat() if pd.notna(row.get("ex_date")) else "",
                    f"{row.get('amount'):.6f}" if pd.notna(row.get("amount")) else "",
                ]
                return hashlib.sha256("|".join(parts).encode()).hexdigest()
            df["row_hash"] = df.apply(build_hash, axis=1)
        if "row_hash" in df.columns:
            df["row_hash"] = df["row_hash"].fillna("").astype(str).str.strip()
            df = df[df["row_hash"] != ""]
            df = df.drop_duplicates(subset=["row_hash"])
        if "updated_at" in df.columns:
            df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
            df["updated_at"] = df["updated_at"].fillna(pd.Timestamp.utcnow())
        else:
            df["updated_at"] = pd.Timestamp.utcnow()
        if df.empty:
            continue

        insert_dataframe(df, "stg_dividend_history")
        loaded += len(df)
    return loaded


def load_static_details(root: Path):
    loaded = {}
    mapping = {
        "fund_info_hashed.csv": ("stg_fund_info", [
            "ticker", "asset_type", "source", "name", "isin_number", "cusip_number",
            "issuer", "category", "index_benchmark", "inception_date", "exchange",
            "region", "country", "leverage", "options", "shares_out",
            "market_cap_size", "investment_style", "row_hash", "updated_at"
        ]),
        "fund_fees_hashed.csv": ("stg_fund_fees", [
            "ticker", "asset_type", "source", "expense_ratio", "initial_charge",
            "exit_charge", "assets_aum", "top_10_hold_pct", "holdings_count",
            "holdings_turnover", "row_hash", "updated_at"
        ]),
        "fund_risk_hashed.csv": ("stg_fund_risk", None),  # already aligned columns
        "fund_policy_hashed.csv": ("stg_fund_policy", None),
    }
    for fname, (table, cols) in mapping.items():
        path = root / fname
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"❌ Read error {path}: {e}")
            continue
        if cols:
            for col in cols:
                if col not in df.columns:
                    df[col] = None
            df = df[cols]

        def parse_number(val: str | float | int):
            """Coerce mixed numeric strings (e.g., '842.33m USD') into floats."""
            if pd.isna(val):
                return None
            s = str(val).strip().lower()
            if s in {"", "none", "nan"}:
                return None
            multiplier = 1
            if s.endswith("m"):
                multiplier = 1_000_000
                s = s[:-1]
            elif s.endswith("b"):
                multiplier = 1_000_000_000
                s = s[:-1]
            s = re.sub(r"[^0-9.\\-]", "", s)
            if s == "":
                return None
            try:
                return float(s) * multiplier
            except Exception:
                return None

        percent_cols = []
        if table == "stg_fund_fees":
            percent_cols = ["expense_ratio", "initial_charge", "exit_charge", "top_10_hold_pct"]
        if table == "stg_fund_policy":
            percent_cols = [
                "dividend_yield", "dividend_growth_1y", "dividend_growth_3y",
                "dividend_growth_5y", "dividend_growth_10y",
                "dividend_consecutive_years", "payout_ratio",
                "total_return_ytd", "total_return_1y", "pe_ratio"
            ]
        for col in percent_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace("%", "").str.replace("+", "").str.replace(",", ""),
                    errors="coerce"
                )
        if table == "stg_fund_policy":
            for col in df.columns:
                if col in {"ticker", "asset_type", "source", "row_hash", "updated_at"}:
                    continue
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df[col] = df[col].where(df[col].abs() < 1000)
        if table == "stg_fund_fees":
            for col in ["expense_ratio", "initial_charge", "exit_charge"]:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: x / 100 if pd.notna(x) and x > 1 else x)
        if table == "stg_fund_fees":
            if "assets_aum" in df.columns:
                df["assets_aum"] = df["assets_aum"].apply(parse_number)
            if "holdings_turnover" in df.columns:
                df["holdings_turnover"] = df["holdings_turnover"].apply(parse_number)
            if "holdings_count" in df.columns:
                df["holdings_count"] = pd.to_numeric(df["holdings_count"].astype(str).str.replace(",", ""), errors="coerce")
        if table == "stg_fund_risk":
            for col in df.columns:
                if col in {"ticker", "asset_type", "source", "row_hash", "updated_at"}:
                    continue
                df[col] = pd.to_numeric(df[col].astype(str).str.replace("%", "").str.replace(",", ""), errors="coerce")
                if col != "moving_avg_200":
                    df[col] = df[col].where(df[col].abs() < 1000)
        if "inception_date" in df.columns:
            df["inception_date"] = pd.to_datetime(df["inception_date"], errors="coerce")
        if "updated_at" in df.columns:
            df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
            df["updated_at"] = df["updated_at"].fillna(pd.Timestamp.utcnow())

        insert_dataframe(df, table)
        loaded[table] = len(df)
    return loaded


def load_holdings(root: Path):
    loaded = {}
    holdings_path = root / "holdings_hashed.csv"
    alloc_files = {
        "allocations_hashed.csv": "asset_allocation",
        "sectors_hashed.csv": "sector",
        "regions_hashed.csv": "region",
    }

    if holdings_path.exists():
        df = pd.read_csv(holdings_path)
        if not df.empty:
            rename_map = {"item_name": "holding_name", "value_net": "holding_percentage"}
            df = df.rename(columns=rename_map)
            for col in [
                "holding_ticker", "holding_name", "holding_percentage",
                "shares_held", "market_value", "sector", "country",
                "as_of_date", "row_hash", "updated_at", "ticker",
                "asset_type", "source",
            ]:
                if col not in df.columns:
                    df[col] = None
            df = df[
                ["ticker", "asset_type", "source", "holding_ticker", "holding_name",
                 "holding_percentage", "shares_held", "market_value", "sector",
                 "country", "as_of_date", "row_hash", "updated_at"]
            ]
            insert_dataframe(df, "stg_fund_holdings")
            loaded["stg_fund_holdings"] = len(df)

    for fname, alloc_type in alloc_files.items():
        path = root / fname
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if df.empty:
            continue
        df["allocation_type"] = alloc_type
        df = df.rename(columns={"item_name": "category_name", "value_net": "value_net"})
        for col in [
            "ticker", "asset_type", "source", "allocation_type", "category_name",
            "value_net", "value_category_avg", "value_long", "value_short",
            "as_of_date", "row_hash", "updated_at",
        ]:
            if col not in df.columns:
                df[col] = None
        df = df[
            ["ticker", "asset_type", "source", "allocation_type", "category_name",
             "value_net", "value_category_avg", "value_long", "value_short",
             "as_of_date", "row_hash", "updated_at"]
        ]
        insert_dataframe(df, "stg_allocations")
        loaded.setdefault("stg_allocations", 0)
        loaded["stg_allocations"] += len(df)

    return loaded


def main():
    parser = argparse.ArgumentParser(description="Bulk import CSV into staging tables using row_hash upsert.")
    parser.add_argument("--price-root", default="data/04_hashed/price_history", type=Path)
    parser.add_argument("--dividend-root", default="data/04_hashed/dividend_history", type=Path)
    parser.add_argument("--static-root", default="data/04_hashed/static_details", type=Path)
    parser.add_argument("--holdings-root", default="data/04_hashed/holdings", type=Path)
    parser.add_argument("--max-price-files", type=int, default=None, help="Limit price files for faster migration")
    parser.add_argument("--max-dividend-files", type=int, default=None, help="Limit dividend files for faster migration")
    args = parser.parse_args()

    total_loaded = {}

    if args.price_root.exists():
        loaded = load_price_history(args.price_root, args.max_price_files)
        total_loaded["stg_price_history"] = loaded
        print(f"✅ Loaded price history rows: {loaded}")
    else:
        print(f"⚠️ Price root not found: {args.price_root}")

    if args.dividend_root.exists():
        loaded = load_dividends(args.dividend_root, args.max_dividend_files)
        total_loaded["stg_dividend_history"] = loaded
        print(f"✅ Loaded dividend rows: {loaded}")
    else:
        print(f"⚠️ Dividend root not found: {args.dividend_root}")

    if args.static_root.exists():
        loaded_map = load_static_details(args.static_root)
        total_loaded.update(loaded_map)
        for k, v in loaded_map.items():
            print(f"✅ Loaded {v} rows into {k}")
    else:
        print(f"⚠️ Static detail hashed not found: {args.static_root}")

    if args.holdings_root.exists():
        loaded_map = load_holdings(args.holdings_root)
        total_loaded.update(loaded_map)
        for k, v in loaded_map.items():
            print(f"✅ Loaded {v} rows into {k}")
    else:
        print(f"⚠️ Holdings hashed not found: {args.holdings_root}")

    print("=== SUMMARY ===")
    for tbl, cnt in total_loaded.items():
        print(f"{tbl}: {cnt} rows")


if __name__ == "__main__":
    main()
