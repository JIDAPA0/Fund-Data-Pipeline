import sys
import asyncio
import time
import math
import random
import re
from datetime import datetime
from pathlib import Path

import aiohttp
import pandas as pd

# ==========================================
# SYSTEM PATH SETUP
# ==========================================
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[2]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.logger import setup_logger
from src.utils.browser_utils import get_random_headers
from src.utils.db_connector import get_active_tickers
from src.utils.path_manager import get_validation_path

# ==========================================
# CONFIGURATION
# ==========================================
logger = setup_logger("02_perf_ft_nav")

CONCURRENCY = 20
BATCH_SIZE = 100
REQUEST_TIMEOUT = 12

PRICE_RE = re.compile(r"Price\s*\(([A-Z]{3})\)\s*</span>\s*<span[^>]*>([^<]+)</span>", re.IGNORECASE)
ASOF_RE = re.compile(r"as of\s+([A-Za-z]{3}\s+\d{1,2}\s+\d{4})", re.IGNORECASE)

current_date = datetime.now().strftime('%Y-%m-%d')
OUTPUT_FILE = get_validation_path(
    "Financial_Times",
    "02_Daily_NAV",
    f"{current_date}/ft_nav_results.csv"
)
ERROR_FILE = OUTPUT_FILE.parent / "ft_nav_errors.csv"
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# ==========================================
# HELPERS
# ==========================================

def _clean_float(value: str):
    if value is None:
        return None
    cleaned = value.replace(',', '').strip()
    try:
        return float(cleaned)
    except Exception:
        return None


def _parse_html(html: str):
    if not html:
        return None, None, None

    price_match = PRICE_RE.search(html)
    if not price_match:
        return None, None, None

    currency, price_raw = price_match.groups()
    nav_price = _clean_float(price_raw)

    date_match = ASOF_RE.search(html)
    if date_match:
        try:
            as_of_date = datetime.strptime(date_match.group(1), "%b %d %Y").strftime("%Y-%m-%d")
        except Exception:
            as_of_date = current_date
    else:
        as_of_date = current_date

    return nav_price, currency, as_of_date


def _build_url(ticker: str, asset_type: str) -> str:
    base = "etfs" if "ETF" in str(asset_type).upper() else "funds"
    return f"https://markets.ft.com/data/{base}/tearsheet/summary?s={ticker}"


def _load_processed_tickers():
    if not OUTPUT_FILE.exists():
        return set()
    try:
        df = pd.read_csv(OUTPUT_FILE, usecols=["ticker"])
        return set(df["ticker"].astype(str).str.strip())
    except Exception:
        return set()


async def fetch_one(session, item, sem):
    ticker = item["ticker"]
    asset_type = item["asset_type"]
    url = _build_url(ticker, asset_type)

    async with sem:
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                if response.status != 200:
                    return {
                        "ticker": ticker,
                        "asset_type": asset_type,
                        "source": "Financial Times",
                        "nav_price": None,
                        "currency": None,
                        "as_of_date": None,
                        "scrape_date": current_date,
                        "status": f"HTTP {response.status}",
                    }
                html = await response.text()
        except Exception as exc:
            return {
                "ticker": ticker,
                "asset_type": asset_type,
                "source": "Financial Times",
                "nav_price": None,
                "currency": None,
                "as_of_date": None,
                "scrape_date": current_date,
                "status": f"Error: {exc}",
            }

    nav_price, currency, as_of_date = _parse_html(html)
    if nav_price is None:
        return {
            "ticker": ticker,
            "asset_type": asset_type,
            "source": "Financial Times",
            "nav_price": None,
            "currency": currency,
            "as_of_date": as_of_date,
            "scrape_date": current_date,
            "status": "Failed",
        }

    return {
        "ticker": ticker,
        "asset_type": asset_type,
        "source": "Financial Times",
        "nav_price": nav_price,
        "currency": currency,
        "as_of_date": as_of_date,
        "scrape_date": current_date,
        "status": "Success",
    }


def _append_csv(df: pd.DataFrame, path: Path):
    if df.empty:
        return
    use_header = not path.exists()
    df.to_csv(path, mode="a", header=use_header, index=False)


def _append_errors(df: pd.DataFrame):
    if df.empty:
        return
    error_df = df[df["status"] != "Success"].copy()
    if error_df.empty:
        return
    use_header = not ERROR_FILE.exists()
    error_df.to_csv(ERROR_FILE, mode="a", header=use_header, index=False)


async def run_batches(tickers):
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    headers = get_random_headers()
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        sem = asyncio.Semaphore(CONCURRENCY)
        total = len(tickers)
        total_batches = math.ceil(total / BATCH_SIZE)
        completed = 0

        for i in range(total_batches):
            batch = tickers[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
            if not batch:
                continue

            start = time.time()
            results = await asyncio.gather(*[fetch_one(session, t, sem) for t in batch])
            df = pd.DataFrame(results)

            _append_csv(df, OUTPUT_FILE)
            _append_errors(df)

            completed += len(batch)
            logger.info(
                "Batch %s/%s | Saved %s rows | Done %s/%s | %.2fs",
                i + 1,
                total_batches,
                len(df),
                completed,
                total,
                time.time() - start,
            )

            await asyncio.sleep(random.uniform(0.5, 1.2))


async def main():
    start_time = time.time()

    logger.info("📡 Fetching Active FT tickers...")
    all_tickers = get_active_tickers("Financial Times")
    if not all_tickers:
        logger.warning("🚫 No active tickers found for Financial Times.")
        return

    processed = _load_processed_tickers()
    todo = [t for t in all_tickers if t["ticker"] not in processed]

    logger.info("📊 Total: %s | Skipped: %s | Remaining: %s", len(all_tickers), len(processed), len(todo))
    if not todo:
        logger.info("✅ All tickers already processed.")
        return

    await run_batches(todo)

    logger.info("✅ FT NAV scraping finished in %.2f minutes", (time.time() - start_time) / 60)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
