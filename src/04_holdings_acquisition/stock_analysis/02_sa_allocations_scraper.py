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

# --- 🛠️ SETUP PATH & IMPORTS ------------------------------------------------
current_file = Path(__file__).resolve()
PROJECT_ROOT = current_file.parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.utils.logger import setup_logger
from src.utils.db_connector import get_active_tickers

logger = setup_logger("02_sa_allocations_scraper")

# --- ⚙️ LOAD CONFIG FROM .ENV ------------------------------------------------
env_path = PROJECT_ROOT / ".env"
if env_path.exists():
    logger.info("📂 Loading environment variables from: %s", env_path)
    load_dotenv(dotenv_path=env_path)
else:
    logger.warning("⚠️ Warning: .env file not found at %s", env_path)

# Config Login
SA_EMAIL = os.getenv("SA_EMAIL")
SA_PASSWORD = os.getenv("SA_PASSWORD")
LOGIN_URL = os.getenv("SA_LOGIN_URL", "https://stockanalysis.com/login")

if not SA_EMAIL or not SA_PASSWORD:
    logger.warning("⚠️ Missing SA_EMAIL or SA_PASSWORD in .env")

# --- ⚙️ SCRAPER SETTINGS -----------------------------------------------------
BASE_OUTPUT_DIR = PROJECT_ROOT / "validation_output/Stock_Analysis/05_Allocations"
BASE_URL = "https://stockanalysis.com/etf/"
MAX_CONCURRENT_TICKERS = 5 

# --- Utility Functions ----------------------------------------------------

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

def fetch_tickers_from_db():
    rows = get_active_tickers("Stock Analysis")
    tickers = [r.get("ticker") for r in rows if r.get("ticker")]
    logger.info("✅ Query Success: Found %s tickers.", len(tickers))
    return tickers

async def login_to_sa(page):
    logger.info("🔐 Attempting Login to %s as %s...", LOGIN_URL, SA_EMAIL or "UNKNOWN")
    try:
        await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        if "login" in page.url:
            await page.fill('input[type="email"]', SA_EMAIL)
            await page.fill('input[type="password"]', SA_PASSWORD)
            await page.keyboard.press("Enter")
            await page.wait_for_url(lambda url: "login" not in url, timeout=30000)
            if "login" not in page.url:
                logger.info("✅ Login Successful!")
                return True 
            else:
                logger.error("❌ Login Failed")
                return False 
        else:
            logger.info("✅ Session already authenticated.")
            return True
    except Exception as e:
        logger.error("❌ Critical Login Error: %s", e)
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
            df = pd.DataFrame(extracted_data)
            df.to_csv(save_path, index=False, encoding='utf-8')
            return True
        else:
            return False
    except Exception as e:
        return False

def generate_report(output_dir, start_time, total, success, skipped):
    end_time = time.time()
    minutes = int((end_time - start_time) // 60)
    seconds = (end_time - start_time) % 60
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = output_dir / f"Report_Allocations_{timestamp}.txt"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(f"📊 SCRAPING REPORT: {total} Tickers\n")
        f.write(f"✅ Success: {success} | ⚠️ Skipped: {skipped}\n")
        f.write(f"⏱️ Time: {minutes}m {seconds:.2f}s\n")
    print(f"\n📝 Report: {report_path}")

async def worker(ticker: str, context, TODAY_DIR: Path, all_tickers: List[str], counters: Dict[str, Any]):
    page = await context.new_page()
    try:
        async with counters['lock']:
            counters['total_count'] += 1
            current_index = counters['total_count']
        print(f"[{current_index}/{len(all_tickers)}] 📊 Allocations: {ticker} ... ", end='', flush=True)
        is_saved = await extract_sector_allocation(page, ticker, TODAY_DIR)
        async with counters['lock']:
            if is_saved:
                counters['success_count'] += 1
                print(f"✅ Extracted")
            else:
                counters['skipped_count'] += 1
                print(f"⚠️  No Data")
    except Exception as e:
        print(f"🚨 Worker Error for {ticker}: {e}")
        async with counters['lock']:
            counters['skipped_count'] += 1    
    finally:
        await page.close()

# --- MAIN LOGIC ---------------------------------------------------------------
async def main():
    logger.info("--- 🚀 STARTING SECTOR ALLOCATION SCRAPER (DIRECT DB MODE) ---")
    start_time = time.time()
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    TODAY_DIR = BASE_OUTPUT_DIR / today_str
    TODAY_DIR.mkdir(parents=True, exist_ok=True)
    
    
    all_tickers = fetch_tickers_from_db()
    
    if not all_tickers:
        logger.error("❌ Still no tickers found from DB.")
        logger.error("💡 ตรวจสอบว่าใน DB คอลัมน์ source เขียนว่า 'Stock Analysis' จริงหรือไม่")
        return

    processed_tickers = get_processed_tickers(TODAY_DIR)
    tickers_to_process = [t for t in all_tickers if t not in processed_tickers]
    
    logger.info("📋 Loaded %s tickers.", len(all_tickers))
    logger.info("⏳ %s tickers remaining.", len(tickers_to_process))
    
    if not tickers_to_process:
        logger.info("🎉 All tasks completed.")
        return

    counters = {
        'total_count': len(processed_tickers), 
        'success_count': 0,
        'skipped_count': 0,
        'lock': asyncio.Lock()
    }
    initial_processed_count = len(processed_tickers)
    
    async with async_playwright() as p:
        user_data_dir = PROJECT_ROOT / "tmp/sa_session" 
        context = await p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=True,
            args=["--start-maximized"],
            accept_downloads=True 
        )

        page = await context.new_page()
        if not await login_to_sa(page):
            await context.close()
            return
        await page.close() 

        tasks = []
        for ticker in tickers_to_process:
            tasks.append(worker(ticker, context, TODAY_DIR, all_tickers, counters))
        
        for i in range(0, len(tasks), MAX_CONCURRENT_TICKERS):
            batch = tasks[i:i + MAX_CONCURRENT_TICKERS]
            await asyncio.gather(*batch)

        await context.close()

    final_success_count = initial_processed_count + counters['success_count']
    final_skipped_count = counters['skipped_count']
    generate_report(BASE_OUTPUT_DIR, start_time, len(all_tickers), final_success_count, final_skipped_count)
    logger.info("--- 🏁 ALL OPERATIONS COMPLETED ---")

if __name__ == "__main__":
    asyncio.run(main())
