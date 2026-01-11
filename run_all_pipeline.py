import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

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

PARALLEL_GROUPS = {
    "03": [
        {
            "label": "FT Detail",
            "steps": [
                "src/03_master_detail_static/financial_times/01_ft_info_scraper.py",
                "src/03_master_detail_static/financial_times/02_ft_fees_scraper.py",
                "src/03_master_detail_static/financial_times/03_ft_risk_scraper.py",
                "src/03_master_detail_static/financial_times/04_ft_policy_scraper.py",
            ],
        },
        {
            "label": "YF Detail",
            "steps": [
                "src/03_master_detail_static/yahoo_finance/01_yf_info_scraper.py",
                "src/03_master_detail_static/yahoo_finance/02_yf_fees_scraper.py",
                "src/03_master_detail_static/yahoo_finance/03_yf_risk_scraper.py",
                "src/03_master_detail_static/yahoo_finance/04_yf_policy_scraper.py",
            ],
        },
        {
            "label": "SA Detail",
            "steps": [
                "src/03_master_detail_static/stock_analysis/01_sa_detail_scraper.py",
            ],
        },
    ],
    "04": [
        {
            "label": "FT Holdings",
            "steps": [
                "src/04_holdings_acquisition/financial_times/01_ft_holdings_scraper.py",
            ],
        },
        {
            "label": "YF Holdings",
            "steps": [
                "src/04_holdings_acquisition/yahoo_finance/01_yf_holdings_scraper.py",
            ],
        },
        {
            "label": "SA Holdings",
            "steps": [
                "src/04_holdings_acquisition/stock_analysis/01_sa_holdings_scraper.py",
                "src/04_holdings_acquisition/stock_analysis/02_sa_allocations_scraper.py",
            ],
        },
    ],
}


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


def _normalize_limit(value):
    if value is None:
        return None
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def get_env(ft_limit=None):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR)
    if ft_limit:
        env["FT_MAX_TICKERS"] = str(ft_limit)
    return env


def run_step(path, label, ft_limit=None):
    full_path = BASE_DIR / path
    if not full_path.exists():
        logger.error("❌ Missing script: %s", full_path)
        return False
    logger.info("▶️  Running: %s", label)
    start = time.time()
    try:
        subprocess.run([sys.executable, str(full_path)], check=True, env=get_env(ft_limit))
        logger.info("✅ Finished %s (%.2fs)", label, time.time() - start)
        return True
    except subprocess.CalledProcessError:
        logger.error("❌ Failed: %s", label)
        return False


def run_steps(steps, label_prefix, ft_limit=None, continue_on_fail=False):
    group_results = []
    group_ok = True
    for step in steps:
        ok = run_step(step, f"{label_prefix} - {Path(step).name}", ft_limit=ft_limit)
        group_results.append((step, ok))
        if not ok:
            group_ok = False
            if not continue_on_fail:
                break
    return group_results, group_ok


def run_parallel_groups(groups, module_name, ft_limit=None, continue_on_fail=False):
    results = []
    overall_ok = True
    with ThreadPoolExecutor(max_workers=len(groups)) as executor:
        futures = {
            executor.submit(
                run_steps,
                group["steps"],
                f"{module_name} - {group['label']}",
                ft_limit,
                continue_on_fail,
            ): group["label"]
            for group in groups
        }
        for future in as_completed(futures):
            group_results, group_ok = future.result()
            results.extend(group_results)
            if not group_ok:
                overall_ok = False
    return results, overall_ok


def main():
    parser = argparse.ArgumentParser(description="Run full data pipeline (Modules 01-04 + Sync)")
    parser.add_argument("--only", help="Comma-separated modules: 01,02,03,04 or master,performance,detail,holdings")
    parser.add_argument("--skip-sync", action="store_true", help="Skip sync orchestrators")
    parser.add_argument("--continue-on-fail", action="store_true", help="Continue even if a step fails")
    parser.add_argument("--ft-limit", type=int, help="Limit FT tickers per FT scraper")
    parser.add_argument("--parallel-sources", action="store_true", help="Run FT/YF/SA steps in parallel for modules 03/04")
    args = parser.parse_args()

    only_keys = parse_only(args.only)
    ft_limit = _normalize_limit(args.ft_limit)
    if ft_limit:
        logger.info("⏱️ FT ticker limit enabled: %s", ft_limit)
    pipeline_start = time.time()
    results = []

    for module in MODULES:
        if only_keys and module["key"] not in only_keys:
            continue

        logger.info("🚀 Starting %s", module["name"])
        if args.parallel_sources and module["key"] in PARALLEL_GROUPS:
            logger.info("⚡ Running sources in parallel for %s", module["name"])
            group_results, ok = run_parallel_groups(
                PARALLEL_GROUPS[module["key"]],
                module["name"],
                ft_limit=ft_limit,
                continue_on_fail=args.continue_on_fail,
            )
            results.extend(group_results)
            if not ok and not args.continue_on_fail:
                logger.critical("🛑 Stop pipeline: parallel group failed (%s)", module["name"])
                log_execution_summary(
                    logger,
                    start_time=pipeline_start,
                    total_items=0,
                    status="Failed",
                    extra_info={"failed_step": module["name"]},
                )
                return
        else:
            for step in module["steps"]:
                ok = run_step(step, f"{module['name']} - {Path(step).name}", ft_limit=ft_limit)
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
            ok = run_step(module["sync"], f"{module['name']} - Sync", ft_limit=ft_limit)
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
