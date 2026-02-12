import os
import sys
import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
from playwright.async_api import async_playwright, TimeoutError
from typing import List, Dict, Any, Set
from dotenv import load_dotenv
from sqlalchemy import text

# --- Setup path and imports ---------------------------------------------------
current_file = Path(__file__).resolve()
PROJECT_ROOT = current_file.parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

print(f"Project Root detected at: {PROJECT_ROOT}")

# --- Load env ----------------------------------------------------------------
env_path = PROJECT_ROOT / ".env"
if env_path.exists():
    print(f"Loading environment variables from: {env_path}")
    load_dotenv(dotenv_path=env_path)
else:
    print(f"Warning: .env file not found at {env_path}")

SA_EMAIL = os.getenv("SA_EMAIL")
SA_PASSWORD = os.getenv("SA_PASSWORD")
LOGIN_URL = "https://stockanalysis.com/login"

if not SA_EMAIL or not SA_PASSWORD:
    print("FATAL ERROR: Missing SA_EMAIL or SA_PASSWORD in .env")
    exit(1)

from src.utils.db_connector import get_db_engine

# --- Scraper settings ---------------------------------------------------------
BASE_OUTPUT_DIR = PROJECT_ROOT / "validation_output/Stock_Analysis/05_Allocations"
BASE_URL = "https://stockanalysis.com/etf/"
MAX_CONCURRENT_TICKERS = 5


def get_processed_tickers(target_dir: Path) -> Set[str]:
    if not target_dir.exists():
        return set()
    processed_files = target_dir.glob("*_allocations.csv")
    processed_tickers = set()
    for file_path in processed_files:
        if file_path.stat().st_size > 0:
            ticker = file_path.name.split('_allocations.csv')[0]
            processed_tickers.add(ticker)
    return processed_tickers


def fetch_tickers_direct_from_db():
    print("Connecting to Database directly...")
    tickers = []
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT ticker FROM stg_security_master WHERE source = :source"),
                {"source": "Stock Analysis"},
            ).fetchall()
        tickers = [row[0] for row in rows]
        print(f"Query success: found {len(tickers)} tickers.")
    except Exception as e:
        print(f"Database error: {e}")
    return tickers


async def login_to_sa(page):
    print(f"Attempting login to {LOGIN_URL} as {SA_EMAIL}...")
    try:
        await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        if "login" in page.url:
            await page.fill('input[type="email"]', SA_EMAIL)
            await page.fill('input[type="password"]', SA_PASSWORD)
            await page.keyboard.press("Enter")
            await page.wait_for_url(lambda url: "login" not in url, timeout=30000)
            if "login" not in page.url:
                print("Login successful")
                return True
            print("Login failed")
            return False

        print("Session already authenticated")
        return True
    except Exception as e:
        print(f"Critical login error: {e}")
        return False


async def extract_sector_allocation(page, ticker, target_dir):
    url = f"{BASE_URL}{ticker.lower()}/holdings/"
    save_path = target_dir / f"{ticker}_allocations.csv"

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        try:
            await page.wait_for_selector('.highcharts-data-label text', state='visible', timeout=10000)
        except TimeoutError:
            return False

        labels_locator = page.locator('.highcharts-data-label text')
        count = await labels_locator.count()
        extracted_data = []

        for i in range(count):
            text_content = await labels_locator.nth(i).text_content()
            if text_content and ":" in text_content:
                parts = text_content.split(":")
                if len(parts) == 2:
                    sector_name = parts[0].strip()
                    percentage_str = parts[1].replace('%', '').strip()
                    try:
                        percentage = float(percentage_str)
                        extracted_data.append({
                            'ticker': ticker,
                            'sector': sector_name,
                            'percentage': percentage,
                            'scrape_date': datetime.now().strftime('%Y-%m-%d')
                        })
                    except ValueError:
                        continue

        if extracted_data:
            pd.DataFrame(extracted_data).to_csv(save_path, index=False, encoding='utf-8')
            return True
        return False
    except Exception:
        return False


def generate_report(output_dir, start_time, total, success, skipped):
    end_time = time.time()
    minutes = int((end_time - start_time) // 60)
    seconds = (end_time - start_time) % 60
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = output_dir / f"Report_Allocations_{timestamp}.txt"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(f"SCRAPING REPORT: {total} tickers\n")
        f.write(f"Success: {success} | Skipped: {skipped}\n")
        f.write(f"Time: {minutes}m {seconds:.2f}s\n")
    print(f"Report: {report_path}")


async def worker(ticker: str, context, today_dir: Path, all_tickers: List[str], counters: Dict[str, Any]):
    page = await context.new_page()
    try:
        async with counters['lock']:
            counters['total_count'] += 1
            current_index = counters['total_count']

        print(f"[{current_index}/{len(all_tickers)}] allocations: {ticker} ... ", end='', flush=True)
        is_saved = await extract_sector_allocation(page, ticker, today_dir)

        async with counters['lock']:
            if is_saved:
                counters['success_count'] += 1
                print("saved")
            else:
                counters['skipped_count'] += 1
                print("no data")
    except Exception as e:
        print(f"Worker error for {ticker}: {e}")
        async with counters['lock']:
            counters['skipped_count'] += 1
    finally:
        await page.close()


async def main():
    print("Starting sector allocation scraper")
    start_time = time.time()

    today_str = datetime.now().strftime('%Y-%m-%d')
    today_dir = BASE_OUTPUT_DIR / today_str
    today_dir.mkdir(parents=True, exist_ok=True)

    all_tickers = fetch_tickers_direct_from_db()
    if not all_tickers:
        print("No tickers found from database")
        return

    processed_tickers = get_processed_tickers(today_dir)
    tickers_to_process = [t for t in all_tickers if t not in processed_tickers]

    print(f"Loaded {len(all_tickers)} tickers. Remaining: {len(tickers_to_process)}")
    if not tickers_to_process:
        print("All tasks completed")
        return

    counters = {
        'total_count': len(processed_tickers),
        'success_count': 0,
        'skipped_count': 0,
        'lock': asyncio.Lock(),
    }
    initial_processed_count = len(processed_tickers)

    async with async_playwright() as p:
        user_data_dir = PROJECT_ROOT / "tmp/sa_session"
        context = await p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=True,
            args=["--start-maximized"],
            accept_downloads=True,
        )

        page = await context.new_page()
        if not await login_to_sa(page):
            await context.close()
            return
        await page.close()

        tasks = [worker(ticker, context, today_dir, all_tickers, counters) for ticker in tickers_to_process]
        for i in range(0, len(tasks), MAX_CONCURRENT_TICKERS):
            batch = tasks[i:i + MAX_CONCURRENT_TICKERS]
            await asyncio.gather(*batch)

        await context.close()

    final_success_count = initial_processed_count + counters['success_count']
    final_skipped_count = counters['skipped_count']
    generate_report(BASE_OUTPUT_DIR, start_time, len(all_tickers), final_success_count, final_skipped_count)
    print("Completed")


if __name__ == "__main__":
    asyncio.run(main())
