import argparse
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from src.utils.db_connector import get_db_engine

SOURCE_MAP = {
    "ft": "Financial Times",
    "yf": "Yahoo Finance",
    "sa": "Stock Analysis",
}

SCRAPERS_M03 = {
    "ft": {
        "info": "src/03_master_detail_static/financial_times/01_ft_info_scraper.py",
        "fees": "src/03_master_detail_static/financial_times/02_ft_fees_scraper.py",
        "risk": "src/03_master_detail_static/financial_times/03_ft_risk_scraper.py",
        "policy": "src/03_master_detail_static/financial_times/04_ft_policy_scraper.py",
    },
    "yf": {
        "info": "src/03_master_detail_static/yahoo_finance/01_yf_info_scraper.py",
        "fees": "src/03_master_detail_static/yahoo_finance/02_yf_fees_scraper.py",
        "risk": "src/03_master_detail_static/yahoo_finance/03_yf_risk_scraper.py",
        "policy": "src/03_master_detail_static/yahoo_finance/04_yf_policy_scraper.py",
    },
    "sa": {
        "detail": "src/03_master_detail_static/stock_analysis/01_sa_detail_scraper.py",
    },
}

SCRAPERS_M04 = {
    "ft": [
        "src/04_holdings_acquisition/financial_times/01_ft_holdings_scraper.py",
    ],
    "yf": [
        "src/04_holdings_acquisition/yahoo_finance/01_yf_holdings_scraper.py",
    ],
    "sa": [
        "src/04_holdings_acquisition/stock_analysis/01_sa_holdings_scraper.py",
        "src/04_holdings_acquisition/stock_analysis/02_sa_allocations_scraper.py",
    ],
}


def parse_list(raw):
    if not raw:
        return []
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


def query_missing(engine, table, source):
    sql = text(f"""
        SELECT m.ticker, m.asset_type
        FROM stg_security_master m
        LEFT JOIN {table} t
            ON t.ticker = m.ticker
            AND t.asset_type = m.asset_type
            AND t.source = :source
        WHERE m.source = :source
            AND m.status = 'active'
            AND t.ticker IS NULL
    """)
    return pd.read_sql(sql, engine, params={"source": source})


def merge_missing(*frames):
    if not frames:
        return pd.DataFrame(columns=["ticker", "asset_type"])
    merged = pd.concat(frames, ignore_index=True)
    if merged.empty:
        return merged
    return merged.drop_duplicates(subset=["ticker", "asset_type"])


def write_missing(df, path):
    if df.empty:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return len(df)


def run_scraper(script_path, tickers_file):
    env = os.environ.copy()
    env["FILTER_TICKERS_FILE"] = str(tickers_file)
    env["PYTHONPATH"] = str(BASE_DIR)
    subprocess.run([sys.executable, str(BASE_DIR / script_path)], check=True, env=env)


def build_missing_lists(engine, sources, only_modules, output_dir):
    missing = {}
    if "03" in only_modules:
        for src in sources:
            src_name = SOURCE_MAP[src]
            if src in ("ft", "yf"):
                missing[src] = missing.get(src, {})
                missing[src]["info"] = query_missing(engine, "stg_fund_info", src_name)
                missing[src]["fees"] = query_missing(engine, "stg_fund_fees", src_name)
                missing[src]["risk"] = query_missing(engine, "stg_fund_risk", src_name)
                missing[src]["policy"] = query_missing(engine, "stg_fund_policy", src_name)
            elif src == "sa":
                info = query_missing(engine, "stg_fund_info", src_name)
                fees = query_missing(engine, "stg_fund_fees", src_name)
                risk = query_missing(engine, "stg_fund_risk", src_name)
                policy = query_missing(engine, "stg_fund_policy", src_name)
                missing[src] = {"detail": merge_missing(info, fees, risk, policy)}

    if "04" in only_modules:
        for src in sources:
            src_name = SOURCE_MAP[src]
            holdings = query_missing(engine, "stg_fund_holdings", src_name)
            allocations = query_missing(engine, "stg_allocations", src_name)
            metrics = None
            if src == "yf":
                metrics = query_missing(engine, "stg_fund_metrics", src_name)
            union = merge_missing(holdings, allocations, metrics) if metrics is not None else merge_missing(holdings, allocations)
            missing.setdefault(src, {})
            missing[src]["holdings"] = union

    results = {}
    for src, groups in missing.items():
        for key, df in groups.items():
            file_path = output_dir / f"m{key}_{src}_missing.csv"
            count = write_missing(df, file_path)
            results[(src, key)] = (file_path, count)
    return results


def main():
    parser = argparse.ArgumentParser(description="Run missing-only scrapers for Module 03/04")
    parser.add_argument("--sources", default="ft,yf,sa", help="Comma-separated: ft,yf,sa")
    parser.add_argument("--only", default="03,04", help="Comma-separated modules: 03,04")
    parser.add_argument("--run", action="store_true", help="Execute scrapers after computing missing lists")
    args = parser.parse_args()

    sources = [s for s in parse_list(args.sources) if s in SOURCE_MAP]
    only_modules = set(parse_list(args.only))
    output_dir = BASE_DIR / "tmp" / "missing_m3_m4"
    output_dir.mkdir(parents=True, exist_ok=True)

    engine = get_db_engine()
    results = build_missing_lists(engine, sources, only_modules, output_dir)

    print("📋 Missing summary:")
    for (src, key), (path, count) in sorted(results.items()):
        print(f"  - {src}:{key} -> {count} ({path})")

    if not args.run:
        return

    if "03" in only_modules:
        for src in sources:
            src_groups = SCRAPERS_M03.get(src, {})
            for key, script in src_groups.items():
                if src == "sa" and key != "detail":
                    continue
                if (src, key) not in results:
                    continue
                path, count = results[(src, key)]
                if count == 0:
                    continue
                print(f"▶️  Running {script} (missing: {count})")
                run_scraper(script, path)

    if "04" in only_modules:
        for src in sources:
            if (src, "holdings") not in results:
                continue
            path, count = results[(src, "holdings")]
            if count == 0:
                continue
            for script in SCRAPERS_M04.get(src, []):
                print(f"▶️  Running {script} (missing: {count})")
                run_scraper(script, path)


if __name__ == "__main__":
    main()
