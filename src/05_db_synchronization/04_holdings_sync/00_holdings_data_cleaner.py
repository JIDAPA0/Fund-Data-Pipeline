import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

FT_DIR = BASE_DIR / "validation_output" / "Financial_Times" / "04_Holdings"
YF_DIR = BASE_DIR / "validation_output" / "Yahoo_Finance" / "04_Holdings"
SA_HOLDINGS_DIR = BASE_DIR / "validation_output" / "Stock_Analysis" / "04_Holdings"
SA_ALLOC_DIR = BASE_DIR / "validation_output" / "Stock_Analysis" / "05_Allocations"

STAGING_DIR = BASE_DIR / "data" / "03_staging" / "holdings"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

HOLDINGS_OUT = STAGING_DIR / "holdings_clean.csv"
ALLOC_OUT = STAGING_DIR / "allocations_clean.csv"
SECTOR_OUT = STAGING_DIR / "sectors_clean.csv"
REGION_OUT = STAGING_DIR / "regions_clean.csv"
METRICS_OUT = STAGING_DIR / "fund_metrics_clean.csv"

HOLDINGS_COLUMNS = [
    "ticker",
    "asset_type",
    "source",
    "as_of_date",
    "allocation_type",
    "item_name",
    "value_net",
    "holding_ticker",
    "shares_held",
    "market_value",
    "sector",
    "country",
]

ALLOC_COLUMNS = [
    "ticker",
    "asset_type",
    "source",
    "as_of_date",
    "item_name",
    "value_net",
    "value_category_avg",
    "value_long",
    "value_short",
]

METRICS_COLUMNS = [
    "ticker",
    "asset_type",
    "source",
    "metric_type",
    "metric_name",
    "column_name",
    "value_raw",
    "value_num",
    "as_of_date",
]

def reset_output(path: Path):
    if path.exists():
        path.unlink()


def to_float(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s in {"", "nan", "none", "None"}:
        return None
    s = s.replace("%", "").replace(",", "").replace("+", "")
    try:
        return float(s)
    except Exception:
        return None


def to_date(val):
    dt = pd.to_datetime(val, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date().isoformat()


def infer_date_from_path(path: Path):
    for part in path.parts:
        try:
            return datetime.strptime(part, "%Y-%m-%d").date().isoformat()
        except ValueError:
            continue
    return None


def append_df(df: pd.DataFrame, out_path: Path, columns: list[str]):
    if df.empty:
        return 0
    df = df.reindex(columns=columns)
    header = not out_path.exists()
    df.to_csv(out_path, index=False, mode="a", header=header)
    return len(df)


def safe_read_csv(path: Path, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:
        return None


def process_ft_holdings():
    total = 0
    holdings_dir = FT_DIR / "Holdings"
    if not holdings_dir.exists():
        return total

    for f in holdings_dir.glob("*.csv"):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        df.columns = [c.strip().lower() for c in df.columns]

        out = pd.DataFrame(index=df.index)
        out["ticker"] = df.get("ticker")
        out["asset_type"] = df.get("asset_type")
        out["source"] = df.get("source", "Financial Times")
        out["as_of_date"] = df.get("as_of_date")
        out["allocation_type"] = df.get("allocation_type", "holdings")
        out["item_name"] = df.get("item_name")
        out["value_net"] = df.get("value_net")
        out["holding_ticker"] = df.get("holding_ticker")
        out["shares_held"] = df.get("shares_held")
        out["market_value"] = df.get("market_value")
        out["sector"] = df.get("sector")
        out["country"] = df.get("country")

        if "item_name" in out.columns:
            out = out[~out["item_name"].astype(str).str.contains("per cent of portfolio", case=False, na=False)]

        if "as_of_date" in out.columns:
            out["as_of_date"] = out["as_of_date"].apply(to_date)
        if "value_net" in out.columns:
            out["value_net"] = out["value_net"].apply(to_float)

        out = out.dropna(subset=["ticker", "item_name"])
        total += append_df(out, HOLDINGS_OUT, HOLDINGS_COLUMNS)
    return total


def process_yf_holdings():
    total = 0
    holdings_dir = YF_DIR / "Holdings"
    if not holdings_dir.exists():
        return total

    for f in holdings_dir.glob("*.csv"):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        df.columns = [c.strip().lower() for c in df.columns]

        out = pd.DataFrame(index=df.index)
        out["ticker"] = df.get("ticker")
        out["asset_type"] = df.get("asset_type")
        out["source"] = "Yahoo Finance"
        out["as_of_date"] = df.get("updated_at")
        out["allocation_type"] = "holdings"
        out["item_name"] = df.get("name")
        out["value_net"] = df.get("value")
        out["holding_ticker"] = df.get("symbol")
        out["shares_held"] = None
        out["market_value"] = None
        out["sector"] = None
        out["country"] = None

        out["as_of_date"] = out["as_of_date"].apply(to_date)
        out["value_net"] = out["value_net"].apply(to_float)

        out = out.dropna(subset=["ticker", "item_name"])
        total += append_df(out, HOLDINGS_OUT, HOLDINGS_COLUMNS)
    return total


def process_sa_holdings():
    total = 0
    if not SA_HOLDINGS_DIR.exists():
        return total

    for folder in SA_HOLDINGS_DIR.iterdir():
        if not folder.is_dir():
            continue
        as_of_date = infer_date_from_path(folder)
        for f in folder.glob("*_holdings.csv"):
            df = safe_read_csv(f, encoding="utf-8-sig")
            if df is None or df.empty:
                continue
            df.columns = [c.strip().lower() for c in df.columns]

            symbol_col = next((c for c in df.columns if "symbol" in c), None)
            name_col = next((c for c in df.columns if c == "name"), None)
            weight_col = next((c for c in df.columns if "weight" in c), None)
            shares_col = next((c for c in df.columns if "shares" in c), None)

            out = pd.DataFrame(index=df.index)
            out["ticker"] = f.name.split("_holdings.csv")[0]
            out["asset_type"] = "ETF"
            out["source"] = "Stock Analysis"
            out["as_of_date"] = as_of_date
            out["allocation_type"] = "holdings"
            out["item_name"] = df[name_col] if name_col else df.get(symbol_col)
            out["value_net"] = df[weight_col] if weight_col else None
            out["holding_ticker"] = df.get(symbol_col)
            out["shares_held"] = df[shares_col] if shares_col else None
            out["market_value"] = None
            out["sector"] = None
            out["country"] = None

            out["value_net"] = out["value_net"].apply(to_float)
            if "shares_held" in out.columns:
                out["shares_held"] = out["shares_held"].apply(to_float)

            out = out.dropna(subset=["ticker", "item_name"])
            total += append_df(out, HOLDINGS_OUT, HOLDINGS_COLUMNS)
    return total


def process_ft_allocations():
    total = 0
    alloc_dir = FT_DIR / "Asset_Allocation"
    if not alloc_dir.exists():
        return total

    for f in alloc_dir.glob("*.csv"):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        df.columns = [c.strip().lower() for c in df.columns]

        out = pd.DataFrame(index=df.index)
        out["ticker"] = df.get("ticker")
        out["asset_type"] = df.get("asset_type")
        out["source"] = df.get("source", "Financial Times")
        out["as_of_date"] = df.get("as_of_date")
        out["item_name"] = df.get("item_name")
        out["value_net"] = df.get("value_net")
        out["value_category_avg"] = df.get("value_category_avg")
        out["value_long"] = df.get("value_long")
        out["value_short"] = df.get("value_short")

        out["as_of_date"] = out["as_of_date"].apply(to_date)
        out["value_net"] = out["value_net"].apply(to_float)
        out["value_category_avg"] = out["value_category_avg"].apply(to_float)
        out["value_long"] = out["value_long"].apply(to_float)
        out["value_short"] = out["value_short"].apply(to_float)

        out = out.dropna(subset=["ticker", "item_name"])
        total += append_df(out, ALLOC_OUT, ALLOC_COLUMNS)
    return total


def process_yf_allocations():
    total = 0
    alloc_dir = YF_DIR / "Allocation"
    if not alloc_dir.exists():
        return total

    for f in alloc_dir.glob("*.csv"):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        df.columns = [c.strip().lower() for c in df.columns]

        out = pd.DataFrame(index=df.index)
        out["ticker"] = df.get("ticker")
        out["asset_type"] = df.get("asset_type")
        out["source"] = "Yahoo Finance"
        out["as_of_date"] = df.get("updated_at")
        out["item_name"] = df.get("category")
        out["value_net"] = df.get("value")
        out["value_category_avg"] = None
        out["value_long"] = None
        out["value_short"] = None

        out["as_of_date"] = out["as_of_date"].apply(to_date)
        out["value_net"] = out["value_net"].apply(to_float)

        out = out.dropna(subset=["ticker", "item_name"])
        total += append_df(out, ALLOC_OUT, ALLOC_COLUMNS)
    return total


def process_ft_sectors():
    total = 0
    sector_dir = FT_DIR / "Sectors"
    if not sector_dir.exists():
        return total

    for f in sector_dir.glob("*.csv"):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        df.columns = [c.strip().lower() for c in df.columns]

        out = pd.DataFrame(index=df.index)
        out["ticker"] = df.get("ticker")
        out["asset_type"] = df.get("asset_type")
        out["source"] = df.get("source", "Financial Times")
        out["as_of_date"] = df.get("as_of_date")
        out["item_name"] = df.get("item_name")
        out["value_net"] = df.get("value_net")
        out["value_category_avg"] = df.get("value_category_avg")
        out["value_long"] = df.get("value_long")
        out["value_short"] = df.get("value_short")

        out["as_of_date"] = out["as_of_date"].apply(to_date)
        out["value_net"] = out["value_net"].apply(to_float)
        out["value_category_avg"] = out["value_category_avg"].apply(to_float)
        out["value_long"] = out["value_long"].apply(to_float)
        out["value_short"] = out["value_short"].apply(to_float)

        out = out.dropna(subset=["ticker", "item_name"])
        total += append_df(out, SECTOR_OUT, ALLOC_COLUMNS)
    return total


def process_yf_sectors():
    total = 0
    sector_dir = YF_DIR / "Sectors"
    if not sector_dir.exists():
        return total

    for f in sector_dir.glob("*.csv"):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        df.columns = [c.strip().lower() for c in df.columns]

        out = pd.DataFrame(index=df.index)
        out["ticker"] = df.get("ticker")
        out["asset_type"] = df.get("asset_type")
        out["source"] = "Yahoo Finance"
        out["as_of_date"] = df.get("updated_at")
        out["item_name"] = df.get("sector")
        out["value_net"] = df.get("value")
        out["value_category_avg"] = None
        out["value_long"] = None
        out["value_short"] = None

        out["as_of_date"] = out["as_of_date"].apply(to_date)
        out["value_net"] = out["value_net"].apply(to_float)

        out = out.dropna(subset=["ticker", "item_name"])
        total += append_df(out, SECTOR_OUT, ALLOC_COLUMNS)
    return total


def process_sa_sectors():
    total = 0
    if not SA_ALLOC_DIR.exists():
        return total

    for folder in SA_ALLOC_DIR.iterdir():
        if not folder.is_dir():
            continue
        as_of_date = infer_date_from_path(folder)
        for f in folder.glob("*.csv"):
            df = safe_read_csv(f, encoding="utf-8-sig")
            if df is None or df.empty:
                continue
            df.columns = [c.strip().lower() for c in df.columns]

            out = pd.DataFrame(index=df.index)
            out["ticker"] = df.get("ticker")
            out["asset_type"] = "ETF"
            out["source"] = "Stock Analysis"
            out["as_of_date"] = df.get("scrape_date", as_of_date)
            out["item_name"] = df.get("sector")
            out["value_net"] = df.get("percentage")
            out["value_category_avg"] = None
            out["value_long"] = None
            out["value_short"] = None

            out["as_of_date"] = out["as_of_date"].apply(to_date)
            out["value_net"] = out["value_net"].apply(to_float)

            out = out.dropna(subset=["ticker", "item_name"])
            total += append_df(out, SECTOR_OUT, ALLOC_COLUMNS)
    return total


def process_ft_regions():
    total = 0
    region_dir = FT_DIR / "Regions"
    if not region_dir.exists():
        return total

    for f in region_dir.glob("*.csv"):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        df.columns = [c.strip().lower() for c in df.columns]

        out = pd.DataFrame(index=df.index)
        out["ticker"] = df.get("ticker")
        out["asset_type"] = df.get("asset_type")
        out["source"] = df.get("source", "Financial Times")
        out["as_of_date"] = df.get("as_of_date")
        out["item_name"] = df.get("item_name")
        out["value_net"] = df.get("value_net")
        out["value_category_avg"] = df.get("value_category_avg")
        out["value_long"] = df.get("value_long")
        out["value_short"] = df.get("value_short")

        out["as_of_date"] = out["as_of_date"].apply(to_date)
        out["value_net"] = out["value_net"].apply(to_float)
        out["value_category_avg"] = out["value_category_avg"].apply(to_float)
        out["value_long"] = out["value_long"].apply(to_float)
        out["value_short"] = out["value_short"].apply(to_float)

        out = out.dropna(subset=["ticker", "item_name"])
        total += append_df(out, REGION_OUT, ALLOC_COLUMNS)
    return total


def _extract_metrics(df, metric_type, metric_col, column_col, value_col, date_col):
    out = pd.DataFrame(index=df.index)
    out["ticker"] = df.get("ticker")
    out["asset_type"] = df.get("asset_type")
    out["source"] = "Yahoo Finance"
    out["metric_type"] = metric_type
    out["metric_name"] = df.get(metric_col) if metric_col else None
    out["column_name"] = df.get(column_col) if column_col else None
    out["value_raw"] = df.get(value_col) if value_col else None
    out["value_num"] = df.get(value_col) if value_col else None
    out["as_of_date"] = df.get(date_col) if date_col else None

    out["as_of_date"] = out["as_of_date"].apply(to_date)
    out["value_num"] = out["value_num"].apply(to_float)
    out = out.dropna(subset=["ticker", "metric_name"])
    return out


def process_yf_bond_ratings():
    total = 0
    ratings_dir = YF_DIR / "Bond_Ratings"
    if not ratings_dir.exists():
        return total

    for f in ratings_dir.glob("*.csv"):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        df.columns = [c.strip().lower() for c in df.columns]
        metric_col = "rating" if "rating" in df.columns else "metric" if "metric" in df.columns else None
        date_col = "updated_at" if "updated_at" in df.columns else None
        out = _extract_metrics(df, "bond_rating", metric_col, None, "value", date_col)
        if not out.empty:
            out["column_name"] = "fund"
        total += append_df(out, METRICS_OUT, METRICS_COLUMNS)
    return total


def process_yf_equity_holdings():
    total = 0
    equity_dir = YF_DIR / "Equity_Holdings"
    if not equity_dir.exists():
        return total

    for f in equity_dir.glob("*.csv"):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        df.columns = [c.strip().lower() for c in df.columns]
        metric_col = "metric" if "metric" in df.columns else None
        column_col = "column_name" if "column_name" in df.columns else None
        date_col = "updated_at" if "updated_at" in df.columns else None
        out = _extract_metrics(df, "equity_statistics", metric_col, column_col, "value", date_col)
        total += append_df(out, METRICS_OUT, METRICS_COLUMNS)
    return total


def process_yf_bond_holdings():
    total = 0
    bond_dir = YF_DIR / "Bond_Holdings"
    if not bond_dir.exists():
        return total

    for f in bond_dir.glob("*.csv"):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        df.columns = [c.strip().lower() for c in df.columns]
        metric_col = "metric" if "metric" in df.columns else None
        column_col = "column_name" if "column_name" in df.columns else None
        date_col = "updated_at" if "updated_at" in df.columns else None
        out = _extract_metrics(df, "bond_statistics", metric_col, column_col, "value", date_col)
        total += append_df(out, METRICS_OUT, METRICS_COLUMNS)
    return total


def main():
    reset_output(HOLDINGS_OUT)
    reset_output(ALLOC_OUT)
    reset_output(SECTOR_OUT)
    reset_output(REGION_OUT)
    reset_output(METRICS_OUT)

    total_holdings = 0
    total_alloc = 0
    total_sector = 0
    total_region = 0
    total_metrics = 0

    total_holdings += process_ft_holdings()
    total_holdings += process_yf_holdings()
    total_holdings += process_sa_holdings()

    total_alloc += process_ft_allocations()
    total_alloc += process_yf_allocations()

    total_sector += process_ft_sectors()
    total_sector += process_yf_sectors()
    total_sector += process_sa_sectors()

    total_region += process_ft_regions()

    total_metrics += process_yf_bond_ratings()
    total_metrics += process_yf_equity_holdings()
    total_metrics += process_yf_bond_holdings()

    print(f"✅ Holdings cleaned: {total_holdings} rows")
    print(f"✅ Allocations cleaned: {total_alloc} rows")
    print(f"✅ Sectors cleaned: {total_sector} rows")
    print(f"✅ Regions cleaned: {total_region} rows")
    print(f"✅ Fund metrics cleaned: {total_metrics} rows")


if __name__ == "__main__":
    main()
