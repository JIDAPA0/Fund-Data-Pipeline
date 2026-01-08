import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd
from playwright.async_api import async_playwright
from sqlalchemy import text

# ==========================================
# 1. SETUP
# ==========================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(BASE_DIR)

from src.utils.path_manager import DATA_PERFORMANCE_DIR
from src.utils.logger import setup_logger
from src.utils.db_connector import insert_dataframe, get_db_engine

logger = setup_logger("02_perf_yf_fund_retry")

ASSET_TYPE = "FUND"
SOURCE_NAME = "Yahoo Finance"


def get_last_business_date(today_date: date) -> date:
    last_date = today_date - timedelta(days=1)
    while last_date.weekday() >= 5:
        last_date -= timedelta(days=1)
    return last_date


def parse_reference_date(value: str | None) -> date:
    if value:
        return datetime.strptime(value, "%Y-%m-%d").date()
    return get_last_business_date(datetime.now().date())


def load_failed_tickers(error_file):
    if not error_file.exists():
        logger.warning("No error file found at %s", error_file)
        return []
    try:
        df = pd.read_csv(error_file)
        if "ticker" not in df.columns:
            logger.warning("Error file missing ticker column: %s", error_file)
            return []
        return sorted(set(df["ticker"].astype(str).str.strip()))
    except Exception as exc:
        logger.warning("Failed reading error file %s: %s", error_file, exc)
        return []


def fetch_db_tickers(reference_date: date, include_missing: bool, include_stale: bool, limit):
    if not (include_missing or include_stale):
        return []

    if include_missing and include_stale:
        where_clause = "latest_date IS NULL OR latest_date < :ref_date"
    elif include_missing:
        where_clause = "latest_date IS NULL"
    else:
        where_clause = "latest_date < :ref_date"

    limit_sql = "LIMIT :limit" if limit else ""

    query = text(
        f"""
        WITH latest AS (
            SELECT m.ticker, MAX(d.as_of_date) AS latest_date
            FROM stg_security_master m
            LEFT JOIN stg_daily_nav d
              ON d.ticker = m.ticker
             AND d.asset_type = m.asset_type
             AND d.source = m.source
            WHERE m.status = 'active'
              AND m.source = :source
              AND m.asset_type = :asset_type
            GROUP BY m.ticker
        )
        SELECT ticker
        FROM latest
        WHERE {where_clause}
        ORDER BY latest_date NULLS FIRST, ticker
        {limit_sql}
        """
    )

    params = {
        "source": SOURCE_NAME,
        "asset_type": ASSET_TYPE,
        "ref_date": reference_date,
    }
    if limit:
        params["limit"] = limit

    engine = get_db_engine()
    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [row[0] for row in rows]


async def scrape_ticker(context, ticker: str, reference_date: date, scrape_date: str):
    page = await context.new_page()
    try:
        url = f"https://finance.yahoo.com/quote/{ticker}"
        await page.goto(url, timeout=30000, wait_until="domcontentloaded")

        data = await page.evaluate(
            """() => {
                const getPrice = (field) => {
                    const el = document.querySelector(`fin-streamer[data-field="${field}"]`);
                    return el ? el.innerText.replace(/,/g, '') : null;
                };
                let price = getPrice('regularMarketPrice') || getPrice('regularMarketOpen') || getPrice('navPrice');
                if (!price) {
                    const el = document.querySelector('fin-streamer[data-test="qsp-price"]');
                    if (el) price = el.innerText.replace(/,/g, '');
                }
                return { price };
            }"""
        )

        raw_price = data.get("price")
        if raw_price and raw_price.replace(".", "", 1).isdigit():
            return {
                "ticker": ticker,
                "asset_type": ASSET_TYPE,
                "source": SOURCE_NAME,
                "nav_price": float(raw_price),
                "currency": "USD",
                "as_of_date": reference_date.isoformat(),
                "scrape_date": scrape_date,
            }
    except Exception:
        pass
    finally:
        await page.close()
    return None


async def run_retry(tickers, reference_date: date, batch_size: int, output_dir):
    if not tickers:
        logger.info("No tickers to retry.")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    repair_log = output_dir / "yf_fund_repair_log.csv"
    scrape_date = datetime.now().strftime("%Y-%m-%d")

    logger.info("Retrying %s tickers (reference_date=%s)...", len(tickers), reference_date)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        success_count = 0

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i : i + batch_size]
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            tasks = [scrape_ticker(context, t, reference_date, scrape_date) for t in batch]
            results = await asyncio.gather(*tasks)
            valid = [r for r in results if r]

            if valid:
                df = pd.DataFrame(valid)
                try:
                    insert_dataframe(df, "stg_daily_nav")
                except Exception:
                    pass
                df.to_csv(repair_log, mode="a", header=not repair_log.exists(), index=False)
                success_count += len(valid)

            await context.close()
            logger.info("Repair Batch %s | Fixed: %s/%s", i // batch_size + 1, len(valid), len(batch))

        await browser.close()
        logger.info("Retry Finished. Rescued: %s/%s", success_count, len(tickers))


def parse_args():
    parser = argparse.ArgumentParser(description="Retry Yahoo Finance FUND NAV scraping.")
    parser.add_argument("--date", help="YYYY-MM-DD folder for error file (default: today).")
    parser.add_argument(
        "--reference-date",
        help="YYYY-MM-DD to use for missing/stale check and as_of_date (default: last business day).",
    )
    parser.add_argument("--from-error", action="store_true", help="Retry tickers from error file.")
    parser.add_argument("--missing", action="store_true", help="Retry tickers missing from DB.")
    parser.add_argument("--stale", action="store_true", help="Retry tickers stale vs reference date.")
    parser.add_argument("--limit", type=int, help="Limit DB missing/stale tickers.")
    parser.add_argument("--batch-size", type=int, default=5, help="Batch size for Playwright retries.")
    return parser.parse_args()


def main():
    args = parse_args()

    current_date = datetime.now().strftime("%Y-%m-%d")
    target_date = args.date or current_date
    reference_date = parse_reference_date(args.reference_date)

    output_dir = DATA_PERFORMANCE_DIR / "yahoo_finance" / target_date
    error_file = output_dir / "yf_errors_fund.csv"

    use_error = args.from_error or not (args.from_error or args.missing or args.stale)
    tickers = set()

    if use_error:
        tickers.update(load_failed_tickers(error_file))

    if args.missing or args.stale:
        tickers.update(
            fetch_db_tickers(
                reference_date=reference_date,
                include_missing=args.missing,
                include_stale=args.stale,
                limit=args.limit,
            )
        )

    tickers = sorted(tickers)
    logger.info("Total retry tickers: %s", len(tickers))

    asyncio.run(run_retry(tickers, reference_date, args.batch_size, output_dir))


if __name__ == "__main__":
    main()
