import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

from src.utils.logger import setup_logger, log_execution_summary

logger = setup_logger("run_all_pipeline")

MODULES = [
    {
        "key": "01",
        "name": "Module 01 - Master List",
        "steps": [
            "src/01_master_list_acquisition/01_ft_list_scraper.py",
            "src/01_master_list_acquisition/02_yf_list_scraper.py",
            "src/01_master_list_acquisition/03_sa_list_scraper.py",
        ],
        "sync": "src/05_db_synchronization/01_master_sync/07_master_sync_orchestrator.py",
    },
    {
        "key": "02",
        "name": "Module 02 - Daily Performance",
        "steps": [
            "src/02_daily_performance/financial_times/01_ft_nav_scraper.py",
            "src/02_daily_performance/financial_times/02_ft_history_scraper.py",
            "src/02_daily_performance/yahoo_finance/01_yf_fund_nav_scraper.py",
            "src/02_daily_performance/yahoo_finance/02_yf_etf_nav_scraper.py",
            "src/02_daily_performance/yahoo_finance/03_yf_fund_price_history_scraper.py",
            "src/02_daily_performance/yahoo_finance/04_yf_etf_price_history_scraper.py",
            "src/02_daily_performance/yahoo_finance/05_yf_fund_dividend_scraper.py",
            "src/02_daily_performance/yahoo_finance/06_yf_etf_dividend_scraper.py",
            "src/02_daily_performance/stock_analysis/01_sa_nav_scraper.py",
            "src/02_daily_performance/stock_analysis/02_sa_price_history_scraper.py",
            "src/02_daily_performance/stock_analysis/03_sa_dividend_scraper.py",
        ],
        "sync": "src/05_db_synchronization/02_performance_sync/05_performance_sync_orchestrator.py",
    },
    {
        "key": "03",
        "name": "Module 03 - Detail Static",
        "steps": [
            "src/03_master_detail_static/financial_times/01_ft_info_scraper.py",
            "src/03_master_detail_static/financial_times/02_ft_fees_scraper.py",
            "src/03_master_detail_static/financial_times/03_ft_risk_scraper.py",
            "src/03_master_detail_static/financial_times/04_ft_policy_scraper.py",
            "src/03_master_detail_static/yahoo_finance/01_yf_info_scraper.py",
            "src/03_master_detail_static/yahoo_finance/02_yf_fees_scraper.py",
            "src/03_master_detail_static/yahoo_finance/03_yf_risk_scraper.py",
            "src/03_master_detail_static/yahoo_finance/04_yf_policy_scraper.py",
            "src/03_master_detail_static/stock_analysis/01_sa_detail_scraper.py",
        ],
        "sync": "src/05_db_synchronization/03_detail_sync/05_detail_sync_orchestrator.py",
    },
    {
        "key": "04",
        "name": "Module 04 - Holdings & Allocations",
        "steps": [
            "src/04_holdings_acquisition/financial_times/01_ft_holdings_scraper.py",
            "src/04_holdings_acquisition/yahoo_finance/01_yf_holdings_scraper.py",
            "src/04_holdings_acquisition/stock_analysis/01_sa_holdings_scraper.py",
            "src/04_holdings_acquisition/stock_analysis/02_sa_allocations_scraper.py",
        ],
        "sync": "src/05_db_synchronization/04_holdings_sync/06_holdings_sync_orchestrator.py",
    },
]


def parse_only(value):
    if not value:
        return None
    raw = [v.strip().lower() for v in value.split(",") if v.strip()]
    mapping = {
        "1": "01",
        "01": "01",
        "master": "01",
        "2": "02",
        "02": "02",
        "performance": "02",
        "3": "03",
        "03": "03",
        "detail": "03",
        "4": "04",
        "04": "04",
        "holdings": "04",
    }
    resolved = set()
    for item in raw:
        key = mapping.get(item)
        if key:
            resolved.add(key)
    return resolved or None


def get_env():
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR)
    return env


def run_step(path, label):
    full_path = BASE_DIR / path
    if not full_path.exists():
        logger.error("❌ Missing script: %s", full_path)
        return False
    logger.info("▶️  Running: %s", label)
    start = time.time()
    try:
        subprocess.run([sys.executable, str(full_path)], check=True, env=get_env())
        logger.info("✅ Finished %s (%.2fs)", label, time.time() - start)
        return True
    except subprocess.CalledProcessError:
        logger.error("❌ Failed: %s", label)
        return False


def main():
    parser = argparse.ArgumentParser(description="Run full data pipeline (Modules 01-04 + Sync)")
    parser.add_argument("--only", help="Comma-separated modules: 01,02,03,04 or master,performance,detail,holdings")
    parser.add_argument("--skip-sync", action="store_true", help="Skip sync orchestrators")
    parser.add_argument("--continue-on-fail", action="store_true", help="Continue even if a step fails")
    args = parser.parse_args()

    only_keys = parse_only(args.only)
    pipeline_start = time.time()
    results = []

    for module in MODULES:
        if only_keys and module["key"] not in only_keys:
            continue

        logger.info("🚀 Starting %s", module["name"])
        for step in module["steps"]:
            ok = run_step(step, f"{module['name']} - {Path(step).name}")
            results.append((step, ok))
            if not ok and not args.continue_on_fail:
                logger.critical("🛑 Stop pipeline: step failed (%s)", step)
                log_execution_summary(
                    logger,
                    start_time=pipeline_start,
                    total_items=0,
                    status="Failed",
                    extra_info={"failed_step": step},
                )
                return

        if not args.skip_sync:
            ok = run_step(module["sync"], f"{module['name']} - Sync")
            results.append((module["sync"], ok))
            if not ok and not args.continue_on_fail:
                logger.critical("🛑 Stop pipeline: sync failed (%s)", module["sync"])
                log_execution_summary(
                    logger,
                    start_time=pipeline_start,
                    total_items=0,
                    status="Failed",
                    extra_info={"failed_step": module["sync"]},
                )
                return

    log_execution_summary(
        logger,
        start_time=pipeline_start,
        total_items=0,
        status="Success" if all(r[1] for r in results) else "Partial",
        extra_info={"steps_total": len(results), "steps_failed": len([r for r in results if not r[1]])},
    )


if __name__ == "__main__":
    main()
