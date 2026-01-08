import argparse
import sys
from datetime import datetime
from pathlib import Path
import shutil
import zipfile

import pandas as pd
from sqlalchemy import text

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.db_connector import get_db_connection

DATA_DIR = BASE_DIR / "data"

SOURCE_NAME_BY_KEY = {
    "ft": "Financial Times",
    "sa": "Stock Analysis",
    "yf": "Yahoo Finance",
}

KEY_ALIASES = {
    "ft": "ft",
    "sa": "sa",
    "yf": "yf",
    "stock": "sa",
    "yahoo": "yf",
    "financial_times": "ft",
    "stock_analysis": "sa",
    "yahoo_finance": "yf",
}

RENAME_MAP = {"stock": "sa", "yahoo": "yf"}
SCOPE_ALIASES = {
    "price": "price_history",
    "prices": "price_history",
    "price_history": "price_history",
    "div": "dividend_history",
    "dividend": "dividend_history",
    "dividend_history": "dividend_history",
    "static": "static_details",
    "details": "static_details",
    "static_details": "static_details",
    "nav": "daily_nav",
    "daily_nav": "daily_nav",
}

GROUP_TARGETS = {
    "price_history": [
        ("staging_price_history", DATA_DIR / "03_staging" / "price_history"),
        ("hashed_price_history", DATA_DIR / "04_hashed" / "price_history"),
    ],
    "dividend_history": [
        ("staging_dividend_history", DATA_DIR / "03_staging" / "dividend_history"),
        ("hashed_dividend_history", DATA_DIR / "04_hashed" / "dividend_history"),
    ],
    "static_details": [
        ("staging_static_details", DATA_DIR / "03_static_details"),
        ("hashed_static_details", DATA_DIR / "04_hashed" / "static_details"),
    ],
}

ALL_SCOPES = set(GROUP_TARGETS.keys()) | {"daily_nav"}

NAV_FILES = [
    "merged_daily_nav.csv",
    "validated_daily_nav.csv",
    "validation_errors_daily_nav.csv",
]


def detect_source_key(path: Path) -> str | None:
    for part in path.parts:
        key = part.lower()
        if key in KEY_ALIASES:
            return KEY_ALIASES[key]
    return None


def extract_ticker(path: Path) -> str | None:
    stem = path.stem
    if "_" not in stem:
        return None
    return stem.split("_")[0].strip().upper() or None


def collect_tickers(root: Path) -> dict[str, set[str]]:
    tickers = {key: set() for key in SOURCE_NAME_BY_KEY}
    tickers["unknown"] = set()
    if not root.exists():
        return tickers
    for file_path in root.rglob("*.csv"):
        if not file_path.is_file():
            continue
        ticker = extract_ticker(file_path)
        if not ticker:
            continue
        source_key = detect_source_key(file_path)
        if source_key in SOURCE_NAME_BY_KEY:
            tickers[source_key].add(ticker)
        else:
            tickers["unknown"].add(ticker)
    return tickers


def fetch_db_tickers(engine, table: str, source_name: str) -> set[str]:
    query = text(
        f"SELECT DISTINCT ticker FROM {table} WHERE source = :source AND ticker IS NOT NULL"
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"source": source_name}).fetchall()
    return {r[0] for r in rows if r[0]}


def verify_history(table: str, root: Path) -> tuple[bool, dict[str, dict[str, int]]]:
    engine = get_db_connection()
    file_tickers = collect_tickers(root)
    summary: dict[str, dict[str, int]] = {}
    all_ok = True

    for key, source_name in SOURCE_NAME_BY_KEY.items():
        db_tickers = fetch_db_tickers(engine, table, source_name)
        missing = file_tickers[key] - db_tickers
        extra = db_tickers - file_tickers[key]
        summary[key] = {
            "files": len(file_tickers[key]),
            "db": len(db_tickers),
            "missing": len(missing),
            "extra": len(extra),
        }
        if missing:
            all_ok = False

    summary["unknown"] = {"files": len(file_tickers["unknown"]), "db": 0, "missing": 0, "extra": 0}
    return all_ok, summary


def count_file_rows(path: Path) -> tuple[int, int]:
    df = pd.read_csv(path)
    if "row_hash" in df.columns:
        hashes = df["row_hash"].astype(str).str.strip()
        hashes = hashes[hashes != ""]
        return len(df), hashes.nunique()
    return len(df), len(df)


def count_db_rows(engine, table: str) -> int:
    query = text(f"SELECT COUNT(DISTINCT row_hash) FROM {table} WHERE row_hash IS NOT NULL AND row_hash <> ''")
    with engine.connect() as conn:
        result = conn.execute(query).scalar()
    return int(result or 0)


def verify_static_details(root: Path) -> tuple[bool, dict[str, dict[str, int]]]:
    engine = get_db_connection()
    mapping = {
        "fund_info_hashed.csv": "stg_fund_info",
        "fund_fees_hashed.csv": "stg_fund_fees",
        "fund_risk_hashed.csv": "stg_fund_risk",
        "fund_policy_hashed.csv": "stg_fund_policy",
    }
    summary: dict[str, dict[str, int]] = {}
    all_ok = True

    for filename, table in mapping.items():
        path = root / filename
        if not path.exists():
            summary[filename] = {"file_rows": 0, "file_hashes": 0, "db_hashes": 0}
            all_ok = False
            continue
        file_rows, file_hashes = count_file_rows(path)
        db_hashes = count_db_rows(engine, table)
        summary[filename] = {
            "file_rows": file_rows,
            "file_hashes": file_hashes,
            "db_hashes": db_hashes,
        }
        if db_hashes < file_hashes:
            all_ok = False
    return all_ok, summary


def list_nav_files() -> list[Path]:
    staging_dir = DATA_DIR / "03_staging"
    return [staging_dir / name for name in NAV_FILES if (staging_dir / name).exists()]


def verify_daily_nav() -> tuple[bool, dict[str, int]]:
    engine = get_db_connection()
    nav_files = list_nav_files()
    summary = {"file_rows": 0, "file_keys": 0, "db_matches": 0}
    if not nav_files:
        return False, summary

    target = next((p for p in nav_files if p.name == "validated_daily_nav.csv"), nav_files[0])
    df = pd.read_csv(target)
    if df.empty:
        return False, summary

    key_cols = ["ticker", "asset_type", "source", "as_of_date"]
    for col in key_cols:
        if col not in df.columns:
            return False, summary
    keys = df[key_cols].drop_duplicates()
    summary["file_rows"] = len(df)
    summary["file_keys"] = len(keys)

    temp_table = f"temp_nav_verify_{int(datetime.now().timestamp())}"
    with engine.begin() as conn:
        keys.to_sql(temp_table, conn, if_exists="replace", index=False)
        match_query = text(
            f"""
            SELECT COUNT(*)
            FROM {temp_table} t
            JOIN stg_daily_nav d
              ON d.ticker = t.ticker
             AND d.asset_type = t.asset_type
             AND d.source = t.source
             AND d.as_of_date = t.as_of_date
            """
        )
        matched = conn.execute(match_query).scalar()
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

    summary["db_matches"] = int(matched or 0)
    return summary["db_matches"] >= summary["file_keys"], summary


def archive_directory(source: Path, zip_path: Path) -> int:
    if not source.exists():
        return 0
    files = [p for p in source.rglob("*") if p.is_file()]
    if not files:
        return 0
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path in files:
            zipf.write(file_path, arcname=file_path.relative_to(source))
    return len(files)


def archive_file_list(files: list[Path], zip_path: Path, base_dir: Path) -> int:
    if not files:
        return 0
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path in files:
            zipf.write(file_path, arcname=file_path.relative_to(base_dir))
    return len(files)


def archive_data_store(archive_date: str, scopes: list[str]) -> dict[str, int]:
    archive_root = DATA_DIR / "archive" / archive_date
    results: dict[str, int] = {}
    for scope in scopes:
        if scope == "daily_nav":
            staging_dir = DATA_DIR / "03_staging"
            zip_path = archive_root / "staging_daily_nav.zip"
            results["staging_daily_nav"] = archive_file_list(
                list_nav_files(), zip_path, staging_dir
            )
            continue
        for label, src in GROUP_TARGETS[scope]:
            zip_path = archive_root / f"{label}.zip"
            results[label] = archive_directory(src, zip_path)
    return results


def cleanup_csvs(root: Path, dry_run: bool) -> int:
    if not root.exists():
        return 0
    csv_files = [p for p in root.rglob("*.csv") if p.is_file()]
    if dry_run:
        return len(csv_files)
    removed = 0
    for file_path in csv_files:
        file_path.unlink(missing_ok=True)
        removed += 1
    for dir_path in sorted([p for p in root.rglob("*") if p.is_dir()], reverse=True):
        if dir_path == root:
            continue
        if not any(dir_path.iterdir()):
            dir_path.rmdir()
    return removed


def normalize_scopes(values: list[str] | None) -> list[str]:
    if not values:
        return list(ALL_SCOPES)
    scopes: list[str] = []
    for item in values:
        for part in item.split(","):
            key = part.strip().lower()
            if not key:
                continue
            normalized = SCOPE_ALIASES.get(key)
            if normalized and normalized not in scopes and normalized in ALL_SCOPES:
                scopes.append(normalized)
    return scopes


def cleanup_nav_files(dry_run: bool) -> int:
    files = list_nav_files()
    if dry_run:
        return len(files)
    removed = 0
    for file_path in files:
        file_path.unlink(missing_ok=True)
        removed += 1
    return removed


def cleanup_scopes(scopes: list[str], dry_run: bool) -> int:
    total = 0
    for scope in scopes:
        if scope == "daily_nav":
            total += cleanup_nav_files(dry_run)
            continue
        for _, root in GROUP_TARGETS[scope]:
            total += cleanup_csvs(root, dry_run)
    return total


def rename_source_dirs():
    roots = [
        DATA_DIR / "03_staging" / "price_history",
        DATA_DIR / "03_staging" / "dividend_history",
        DATA_DIR / "04_hashed" / "price_history",
        DATA_DIR / "04_hashed" / "dividend_history",
    ]
    renamed = 0
    for root in roots:
        if not root.exists():
            continue
        dirs = [p for p in root.rglob("*") if p.is_dir()]
        for dir_path in sorted(dirs, reverse=True):
            key = dir_path.name.lower()
            if key not in RENAME_MAP:
                continue
            target = dir_path.with_name(RENAME_MAP[key])
            if target.exists():
                for item in dir_path.iterdir():
                    shutil.move(str(item), str(target / item.name))
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()
            else:
                dir_path.rename(target)
            renamed += 1
    return renamed


def main():
    parser = argparse.ArgumentParser(description="Verify, archive, and clean data store files.")
    parser.add_argument("--rename-sources", action="store_true", help="Rename stock/yahoo folders to sa/yf.")
    parser.add_argument("--verify", action="store_true", help="Verify data in DB against hashed files.")
    parser.add_argument("--archive", action="store_true", help="Create zip archives under data/archive/<date>.")
    parser.add_argument("--cleanup", action="store_true", help="Delete CSV files from data staging/hashed directories.")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"), help="Archive date folder (YYYY-MM-DD).")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without deleting files.")
    parser.add_argument("--run", action="store_true", help="Rename sources, verify, archive, and cleanup in order.")
    parser.add_argument("--scope", action="append", help="Limit to groups: price_history, dividend_history, static_details.")
    parser.add_argument("--allow-partial", action="store_true", help="Archive/cleanup groups that pass verification.")
    args = parser.parse_args()
    scopes = normalize_scopes(args.scope)

    if args.rename_sources or args.run:
        renamed = rename_source_dirs()
        print(f"Renamed source folders: {renamed}")

    verify_ok = True
    verify_results: dict[str, bool] = {}
    price_summary = None
    div_summary = None
    static_summary = None
    nav_summary = None
    if args.verify or args.run:
        if "price_history" in scopes:
            price_ok, price_summary = verify_history(
                "stg_price_history", DATA_DIR / "04_hashed" / "price_history"
            )
            verify_results["price_history"] = price_ok
            verify_ok = verify_ok and price_ok
        if "dividend_history" in scopes:
            div_ok, div_summary = verify_history(
                "stg_dividend_history", DATA_DIR / "04_hashed" / "dividend_history"
            )
            verify_results["dividend_history"] = div_ok
            verify_ok = verify_ok and div_ok
        if "static_details" in scopes:
            static_ok, static_summary = verify_static_details(DATA_DIR / "04_hashed" / "static_details")
            verify_results["static_details"] = static_ok
            verify_ok = verify_ok and static_ok
        if "daily_nav" in scopes:
            nav_ok, nav_summary = verify_daily_nav()
            verify_results["daily_nav"] = nav_ok
            verify_ok = verify_ok and nav_ok

        print("Verification summary:")
        if price_summary is not None:
            print(f"  price_history: {price_summary}")
        if div_summary is not None:
            print(f"  dividend_history: {div_summary}")
        if static_summary is not None:
            print(f"  static_details: {static_summary}")
        if nav_summary is not None:
            print(f"  daily_nav: {nav_summary}")
        print(f"  verified_ok: {verify_ok}")

    run_archive = args.archive or args.run
    run_cleanup = args.cleanup or args.run
    if (run_archive or run_cleanup) and not (args.verify or args.run):
        print("Archive/cleanup requires --verify or --run.")
        return

    target_scopes = scopes
    if args.verify or args.run:
        if args.allow_partial:
            target_scopes = [s for s in scopes if verify_results.get(s)]
        elif not verify_ok:
            target_scopes = []

    if run_archive:
        if target_scopes:
            results = archive_data_store(args.date, target_scopes)
            print(f"Archive results: {results}")
        else:
            print("Archive skipped (verification not satisfied).")

    if run_cleanup:
        if not target_scopes:
            print("Cleanup skipped (verification not satisfied).")
            return
        total = cleanup_scopes(target_scopes, args.dry_run)
        action = "Would remove" if args.dry_run else "Removed"
        print(f"{action} CSV files: {total}")


if __name__ == "__main__":
    main()
