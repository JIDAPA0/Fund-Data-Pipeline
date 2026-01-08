import logging
import os
import subprocess
import sys
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import text

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.utils.db_connector import get_db_engine
from src.utils.logger import setup_logger, log_execution_summary
from src.utils.path_manager import (
    DATA_MASTER_LIST_DIR,
    DATA_STORE_DIR,
    SYNC_DETAIL_ORCHESTRATOR,
    SYNC_HOLDINGS_ORCHESTRATOR,
    SYNC_MASTER_ORCHESTRATOR,
    SYNC_PERF_ORCHESTRATOR,
)

logger = setup_logger("05_sync_IntegrityGate", logging.INFO)

MASTER_FINAL_NAME = "master_list_final.csv"
NAV_FILE_PREFERRED = "validated_daily_nav.csv"
NAV_FILE_FALLBACK = "merged_daily_nav.csv"


def get_today_date() -> date:
    try:
        import pytz

        tz = pytz.timezone("Asia/Bangkok")
        return datetime.now(tz).date()
    except Exception:
        return datetime.now().date()


def get_last_business_date(today_date: date) -> date:
    last_date = today_date - timedelta(days=1)
    while last_date.weekday() >= 5:
        last_date -= timedelta(days=1)
    return last_date


def run_orchestrator(path: Path, label: str) -> bool:
    if not path.exists():
        logger.error("Orchestrator not found: %s", path)
        return False

    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR)

    start = time.time()
    logger.info("Starting %s", label)
    try:
        subprocess.run([sys.executable, str(path)], check=True, env=env)
        logger.info("Completed %s in %.2fs", label, time.time() - start)
        return True
    except subprocess.CalledProcessError as exc:
        logger.error("Failed %s: %s", label, exc)
        return False


def check_db_connection(engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.error("DB connection failed: %s", exc)
        return False


def find_latest_master_file() -> Tuple[Optional[Path], Optional[date], Optional[str]]:
    ready_dir = DATA_MASTER_LIST_DIR / "04_ready_to_load"
    archive_root = DATA_STORE_DIR / "archive" / "01_master_sync"

    candidates = []
    if ready_dir.exists():
        for item in ready_dir.iterdir():
            if not item.is_dir():
                continue
            try:
                folder_date = datetime.strptime(item.name, "%Y-%m-%d").date()
            except ValueError:
                continue
            file_path = item / MASTER_FINAL_NAME
            if file_path.exists():
                candidates.append((folder_date, file_path, None))

    if archive_root.exists():
        for item in archive_root.iterdir():
            if not item.is_dir():
                continue
            try:
                folder_date = datetime.strptime(item.name, "%Y-%m-%d").date()
            except ValueError:
                continue
            zip_path = item / "04_ready_to_load.zip"
            if zip_path.exists():
                candidates.append((folder_date, zip_path, MASTER_FINAL_NAME))

    if not candidates:
        return None, None, None

    candidates.sort(key=lambda x: x[0], reverse=True)
    file_date, file_path, file_member = candidates[0]
    return file_path, file_date, file_member


def check_master_list_integrity(engine, today_date: date) -> Tuple[bool, dict]:
    summary = {
        "file_path": None,
        "file_member": None,
        "file_date": None,
        "file_keys": 0,
        "db_matches": 0,
        "missing_keys": 0,
        "missing_sample": [],
        "stale": True,
    }

    file_path, file_date, file_member = find_latest_master_file()
    summary["file_path"] = str(file_path) if file_path else None
    summary["file_member"] = file_member
    summary["file_date"] = file_date

    if not file_path or not file_date:
        return False, summary

    try:
        if file_path.suffix == ".zip":
            with zipfile.ZipFile(file_path) as zf:
                member_name = file_member or MASTER_FINAL_NAME
                if member_name not in zf.namelist():
                    matches = [name for name in zf.namelist() if name.endswith(MASTER_FINAL_NAME)]
                    if matches:
                        member_name = matches[0]
                    else:
                        logger.error("Master list file not found in archive: %s", file_path)
                        return False, summary
                summary["file_member"] = member_name
                with zf.open(member_name) as handle:
                    df = pd.read_csv(handle)
        else:
            df = pd.read_csv(file_path)
    except Exception as exc:
        logger.error("Failed reading master list file: %s", exc)
        return False, summary

    if df.empty:
        return False, summary

    key_cols = ["ticker", "asset_type", "source"]
    missing_cols = [c for c in key_cols if c not in df.columns]
    if missing_cols:
        logger.error("Master list file missing columns: %s", ", ".join(missing_cols))
        return False, summary

    keys = df[key_cols].copy()
    keys["ticker"] = keys["ticker"].astype(str).str.upper().str.strip()
    keys["asset_type"] = keys["asset_type"].astype(str).str.upper().str.strip()
    keys["source"] = keys["source"].astype(str).str.strip()
    keys = keys.dropna().drop_duplicates()
    summary["file_keys"] = len(keys)
    if keys.empty:
        return False, summary

    temp_table = f"temp_master_verify_{int(time.time())}"
    try:
        with engine.begin() as conn:
            keys.to_sql(temp_table, conn, if_exists="replace", index=False)
            match_query = text(
                f"""
                SELECT COUNT(*)
                FROM {temp_table} t
                JOIN stg_security_master m
                  ON m.ticker = t.ticker
                 AND m.asset_type = t.asset_type
                 AND m.source = t.source
                 AND m.last_seen = :as_of_date
                """
            )
            matched = conn.execute(match_query, {"as_of_date": file_date}).scalar() or 0
            missing_query = text(
                f"""
                SELECT t.ticker, t.asset_type, t.source
                FROM {temp_table} t
                LEFT JOIN stg_security_master m
                  ON m.ticker = t.ticker
                 AND m.asset_type = t.asset_type
                 AND m.source = t.source
                 AND m.last_seen = :as_of_date
                WHERE m.ticker IS NULL
                LIMIT :limit
                """
            )
            missing_rows = conn.execute(
                missing_query, {"as_of_date": file_date, "limit": 20}
            ).fetchall()
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
    except Exception as exc:
        logger.error("Master list DB check failed: %s", exc)
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        except Exception:
            pass
        return False, summary

    missing_count = summary["file_keys"] - matched
    summary["db_matches"] = matched
    summary["missing_keys"] = missing_count
    summary["missing_sample"] = [
        f"{row[0]}|{row[1]}|{row[2]}" for row in missing_rows
    ]
    summary["stale"] = file_date != today_date

    master_ok = file_date == today_date and missing_count == 0
    return master_ok, summary


def choose_nav_file() -> Optional[Path]:
    staging_dir = DATA_STORE_DIR / "03_staging"
    preferred = staging_dir / NAV_FILE_PREFERRED
    fallback = staging_dir / NAV_FILE_FALLBACK
    if preferred.exists():
        return preferred
    if fallback.exists():
        return fallback
    return None


def check_performance_integrity(engine) -> Tuple[bool, dict]:
    summary = {
        "file_path": None,
        "file_keys": 0,
        "db_matches": 0,
        "missing_keys": 0,
        "missing_sample": [],
        "file_max_date": None,
    }

    nav_file = choose_nav_file()
    if not nav_file:
        return False, summary

    summary["file_path"] = str(nav_file)

    try:
        df = pd.read_csv(nav_file)
    except Exception as exc:
        logger.error("Failed reading NAV file: %s", exc)
        return False, summary

    if df.empty:
        return False, summary

    key_cols = ["ticker", "asset_type", "source", "as_of_date"]
    missing_cols = [c for c in key_cols if c not in df.columns]
    if missing_cols:
        logger.error("NAV file missing columns: %s", ", ".join(missing_cols))
        return False, summary

    df = df.copy()
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date
    summary["file_max_date"] = (
        df["as_of_date"].dropna().max().isoformat()
        if df["as_of_date"].notna().any()
        else None
    )

    keys = df[key_cols].copy()
    keys["ticker"] = keys["ticker"].astype(str).str.upper().str.strip()
    keys["asset_type"] = keys["asset_type"].astype(str).str.upper().str.strip()
    keys["source"] = keys["source"].astype(str).str.strip()
    keys = keys.dropna().drop_duplicates()
    summary["file_keys"] = len(keys)
    if keys.empty:
        return False, summary

    temp_table = f"temp_nav_verify_{int(time.time())}"
    try:
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
            matched = conn.execute(match_query).scalar() or 0
            missing_query = text(
                f"""
                SELECT t.ticker, t.asset_type, t.source, t.as_of_date
                FROM {temp_table} t
                LEFT JOIN stg_daily_nav d
                  ON d.ticker = t.ticker
                 AND d.asset_type = t.asset_type
                 AND d.source = t.source
                 AND d.as_of_date = t.as_of_date
                WHERE d.ticker IS NULL
                LIMIT :limit
                """
            )
            missing_rows = conn.execute(missing_query, {"limit": 20}).fetchall()
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
    except Exception as exc:
        logger.error("Performance DB check failed: %s", exc)
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        except Exception:
            pass
        return False, summary

    missing_count = summary["file_keys"] - matched
    summary["db_matches"] = matched
    summary["missing_keys"] = missing_count
    summary["missing_sample"] = [
        f"{row[0]}|{row[1]}|{row[2]}|{row[3]}" for row in missing_rows
    ]

    perf_ok = missing_count == 0
    return perf_ok, summary


def check_nav_current(engine, reference_date: date) -> Tuple[bool, dict]:
    summary = {
        "total_active": 0,
        "current": 0,
        "stale": 0,
        "missing": 0,
        "sample": [],
        "reference_date": reference_date.isoformat(),
    }

    base_query = """
        SELECT m.ticker, m.asset_type, m.source, MAX(d.as_of_date) AS latest_date
        FROM stg_security_master m
        LEFT JOIN stg_daily_nav d
          ON d.ticker = m.ticker
         AND d.asset_type = m.asset_type
         AND d.source = m.source
        WHERE m.status = 'active'
        GROUP BY m.ticker, m.asset_type, m.source
    """

    try:
        with engine.connect() as conn:
            total = (
                conn.execute(text(f"SELECT COUNT(*) FROM ({base_query}) s")).scalar()
                or 0
            )
            missing = (
                conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM ({base_query}) s WHERE s.latest_date IS NULL"
                    )
                ).scalar()
                or 0
            )
            stale = (
                conn.execute(
                    text(
                        f"""
                        SELECT COUNT(*)
                        FROM ({base_query}) s
                        WHERE s.latest_date IS NOT NULL AND s.latest_date < :ref_date
                        """
                    ),
                    {"ref_date": reference_date},
                ).scalar()
                or 0
            )
            sample_rows = conn.execute(
                text(
                    f"""
                    SELECT s.ticker, s.asset_type, s.source, s.latest_date
                    FROM ({base_query}) s
                    WHERE s.latest_date IS NULL OR s.latest_date < :ref_date
                    ORDER BY s.latest_date NULLS FIRST
                    LIMIT :limit
                    """
                ),
                {"ref_date": reference_date, "limit": 20},
            ).fetchall()
    except Exception as exc:
        logger.error("NAV current DB check failed: %s", exc)
        return False, summary

    summary["total_active"] = total
    summary["missing"] = missing
    summary["stale"] = stale
    summary["current"] = max(total - missing - stale, 0)
    summary["sample"] = [
        f"{row[0]}|{row[1]}|{row[2]}|{row[3]}" for row in sample_rows
    ]

    nav_ok = total > 0 and (missing + stale) == 0
    return nav_ok, summary


def log_check_results(label: str, summary: dict) -> None:
    logger.info("%s summary:", label)
    for key, value in summary.items():
        if key in {"missing_sample", "sample"} and value:
            logger.info("%s: %s", key, ", ".join(value))
        else:
            logger.info("%s: %s", key, value)


def main() -> None:
    start_time = time.time()
    logger.info("Integrity gate pipeline starting.")
    allow_partial_nav = os.getenv("ALLOW_PARTIAL_NAV", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if allow_partial_nav:
        logger.warning("ALLOW_PARTIAL_NAV enabled: will proceed even if NAV is stale/missing.")

    engine = get_db_engine()
    if not check_db_connection(engine):
        log_execution_summary(
            logger, start_time, total_items=0, status="Failed: DB connection"
        )
        sys.exit(1)

    today_date = get_today_date()
    nav_reference_date = get_last_business_date(today_date)
    logger.info("NAV current reference date: %s", nav_reference_date.isoformat())

    master_ok, master_summary = check_master_list_integrity(engine, today_date)
    perf_ok, perf_summary = check_performance_integrity(engine)
    nav_ok, nav_summary = check_nav_current(engine, nav_reference_date)

    log_check_results("Master list check", master_summary)
    log_check_results("Performance check", perf_summary)
    log_check_results("NAV current check", nav_summary)

    needs_master_sync = not master_ok
    needs_perf_sync = (not perf_ok) or (not nav_ok)

    if needs_master_sync:
        logger.info("Master list not current or incomplete. Running Module 1.")
        if not run_orchestrator(SYNC_MASTER_ORCHESTRATOR, "Module 1 Master Sync"):
            log_execution_summary(
                logger, start_time, total_items=0, status="Failed: Module 1"
            )
            sys.exit(1)
        needs_perf_sync = True

    if needs_perf_sync:
        logger.info("Performance data not current or incomplete. Running Module 2.")
        if not run_orchestrator(SYNC_PERF_ORCHESTRATOR, "Module 2 Performance Sync"):
            log_execution_summary(
                logger, start_time, total_items=0, status="Failed: Module 2"
            )
            sys.exit(1)

    if needs_master_sync or needs_perf_sync:
        master_ok, master_summary = check_master_list_integrity(engine, today_date)
        perf_ok, perf_summary = check_performance_integrity(engine)
        nav_ok, nav_summary = check_nav_current(engine, nav_reference_date)

        log_check_results("Master list re-check", master_summary)
        log_check_results("Performance re-check", perf_summary)
        log_check_results("NAV current re-check", nav_summary)

        if not master_ok:
            logger.error("Post-sync master list check failed. Skipping Module 3 and 4.")
            log_execution_summary(
                logger, start_time, total_items=0, status="Failed: master list check"
            )
            sys.exit(1)

        if not perf_ok:
            logger.error("Post-sync performance check failed. Skipping Module 3 and 4.")
            log_execution_summary(
                logger, start_time, total_items=0, status="Failed: performance check"
            )
            sys.exit(1)

        if not nav_ok:
            if allow_partial_nav:
                logger.warning(
                    "NAV current check failed after sync; continuing because ALLOW_PARTIAL_NAV=1."
                )
            else:
                logger.error("Post-sync NAV check failed. Skipping Module 3 and 4.")
                log_execution_summary(
                    logger, start_time, total_items=0, status="Failed: NAV check"
                )
                sys.exit(1)

    logger.info("Running Module 3 then Module 4.")
    if not run_orchestrator(SYNC_DETAIL_ORCHESTRATOR, "Module 3 Detail Sync"):
        log_execution_summary(
            logger, start_time, total_items=0, status="Failed: Module 3"
        )
        sys.exit(1)

    if not run_orchestrator(SYNC_HOLDINGS_ORCHESTRATOR, "Module 4 Holdings Sync"):
        log_execution_summary(
            logger, start_time, total_items=0, status="Failed: Module 4"
        )
        sys.exit(1)

    log_execution_summary(logger, start_time, total_items=0, status="Success")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.exception("Unhandled error: %s", exc)
        sys.exit(1)
