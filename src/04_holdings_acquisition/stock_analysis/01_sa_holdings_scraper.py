import os
import sys
import csv
import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
import random
from playwright.async_api import async_playwright, TimeoutError
from typing import List, Dict, Any, Set
from dotenv import load_dotenv

# ==========================================
# SYSTEM PATH SETUP
# ==========================================
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[2]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.logger import setup_logger
from src.utils.db_connector import get_active_tickers

# --- ⚙️ CONFIGURATION ---------------------------------------------------------

INPUT_CSV_PATH = os.getenv("SA_MASTER_CSV")
BASE_OUTPUT_DIR = project_root / "validation_output" / "Stock_Analysis" / "04_Holdings"

# Base URL
BASE_URL = "https://stockanalysis.com/etf/"


BATCH_SIZE = 500
MAX_CONCURRENT_TICKERS = 4

# Load env for Stock Analysis credentials
load_dotenv(project_root / ".env")
LOGIN_URL = os.getenv("SA_LOGIN_URL", "https://stockanalysis.com/login")
EMAIL = os.getenv("SA_EMAIL")
PASS = os.getenv("SA_PASSWORD")

logger = setup_logger("01_sa_holdings_scraper")
if not EMAIL or not PASS:
    logger.warning("⚠️ Missing SA_EMAIL or SA_PASSWORD in .env")


# --- Utility Functions ----------------------------------------------------


def get_processed_tickers(target_dir: Path) -> Set[str]:
    if not target_dir.exists():
        return set()
    
    processed_files = target_dir.glob("*_holdings.csv") 
    processed_tickers = set()
    
    for file_path in processed_files:
        ticker = file_path.name.split('_holdings.csv')[0]
        if file_path.stat().st_size > 0:
             processed_tickers.add(ticker)
            
    return processed_tickers


async def login_to_sa(page):
    logger.info("🔐 Attempting Login as %s", EMAIL or "UNKNOWN")
    try:
        await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        
        
        if "login" in page.url:
            await page.fill('input[type="email"]', EMAIL)
            await page.fill('input[type="password"]', PASS)
            await page.keyboard.press("Enter")
            
            
            await page.wait_for_url(lambda url: "login" not in url, timeout=30000)
            
            if "login" not in page.url:
                logger.info("✅ Login Successful!")
                return True 
            else:
                logger.error("❌ Login Failed (Still on login page)")
                return False 
        else:
            logger.info("✅ Session already authenticated or not required.")
            return True

    except Exception as e:
        logger.error("❌ Critical Login Error: %s", e)
        return False


async def download_holdings(page, ticker, target_dir):
    url = f"{BASE_URL}{ticker.lower()}/holdings/"
    save_path = target_dir / f"{ticker}_holdings.csv" 
    
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        download_btn = page.locator('button:has-text("Download")')
        
        if await download_btn.count() > 0 and await download_btn.first.is_visible():
            await download_btn.first.click()
            csv_option = page.locator('button:has-text("Download to CSV"), div[role="menu"] button:has-text("Download to CSV")')
            
            try:
                await csv_option.first.wait_for(state="visible", timeout=3000)
            except:
                await download_btn.first.click()
                await asyncio.sleep(0.5)

            async with page.expect_download(timeout=15000) as download_info:
                await csv_option.first.click(force=True)
            
            download = await download_info.value
            await download.save_as(save_path)
            
            if save_path.exists() and save_path.stat().st_size > 0:
                return True
        else:
            pass 

    except Exception as e:
        pass
    
    return False

def generate_report(output_dir, start_time, total, success, skipped):
    end_time = time.time()
    minutes = int((end_time - start_time) // 60)
    seconds = (end_time - start_time) % 60
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = output_dir / f"Report_Holdings_{timestamp}.txt"
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("============================================================\n")
        f.write("📊  SCRAPING REPORT: ETF HOLDINGS (DATE FOLDER)\n")
        f.write("============================================================\n")
        f.write(f"🗓️  Execution Date : {datetime.now().strftime('%d %B %Y, %H:%M:%S')}\n")
        f.write(f"📂  Data Location  : {output_dir}\n")
        f.write("-" * 60 + "\n")
        f.write(f"🔹 Total Tickers       : {total:,}\n")
        f.write(f"✅ Downloaded          : {success:,}\n")
        f.write(f"⚠️  No Data / Skipped   : {skipped:,}\n")
        f.write(f"⏱️  Time Taken          : {minutes}m {seconds:.2f}s\n")
        f.write("============================================================\n")
    logger.info("📝 Report: %s", report_path)


def fetch_tickers_from_db():
    rows = get_active_tickers("Stock Analysis")
    return [r.get("ticker") for r in rows if r.get("ticker")]


# 🛠️ NEW: Worker function for concurrent processing
async def worker(ticker: str, context, TODAY_DIR: Path, all_tickers: List[str], counters: Dict[str, Any]):
    
    
    page = await context.new_page()
    
    try:
        
        async with counters['lock']:
            counters['total_count'] += 1
            current_index = counters['total_count']
            
        print(f"[{current_index}/{len(all_tickers)}] 📥 Holdings: {ticker} ... ", end='', flush=True)

        is_saved = await download_holdings(page, ticker, TODAY_DIR)
        
        async with counters['lock']:
            if is_saved:
                counters['success_count'] += 1
                print(f"✅ Saved")
            else:
                counters['skipped_count'] += 1
                print(f"⚠️  No Data")
        
    except Exception as e:
        print(f"🚨 Worker Error for {ticker}: {e}")
        async with counters['lock']:
            counters['skipped_count'] += 1
            
    finally:
        await page.close()



async def main():
    logger.info("--- 🚀 STARTING HOLDINGS DOWNLOADER (SPEED MODE) ---")
    start_time = time.time()
    
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    TODAY_DIR = BASE_OUTPUT_DIR / today_str
    TODAY_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info("📂 Target Folder Created: %s", TODAY_DIR)

    
    all_tickers = fetch_tickers_from_db()
    if not all_tickers:
        if INPUT_CSV_PATH and Path(INPUT_CSV_PATH).exists():
            try:
                df = pd.read_csv(INPUT_CSV_PATH)
                all_tickers = df["ticker"].dropna().astype(str).tolist()
                logger.warning("⚠️ DB tickers empty. Fallback to CSV: %s", INPUT_CSV_PATH)
            except Exception as e:
                logger.error("❌ Error reading fallback CSV: %s", e)
                return
        else:
            logger.error("❌ No tickers available (DB empty and SA_MASTER_CSV not set)")
            return

    processed_tickers = get_processed_tickers(TODAY_DIR)
    tickers_to_process = [t for t in all_tickers if t not in processed_tickers]
    
    logger.info("📄 Loaded %s total tickers.", len(all_tickers))
    logger.info("💾 Found %s tickers already processed (Skipping).", len(processed_tickers))
    logger.info("⏳ %s tickers remaining to process.", len(tickers_to_process))
    
    if not tickers_to_process:
        logger.info("🎉 All tickers for today are already processed. Exiting.")
        return

    
    counters = {
        'total_count': len(processed_tickers), 
        'success_count': 0,
        'skipped_count': 0,
        'lock': asyncio.Lock()
    }
    initial_processed_count = len(processed_tickers)
    
    async with async_playwright() as p:
        
        user_data_dir = project_root / "tmp" / "sa_session"
        context = await p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=True,
            accept_downloads=True
        )

        
        page = await context.new_page()
        if not await login_to_sa(page):
            await context.close()
            logger.error("🚨 CRITICAL: Initial Login Failed. Please check credentials or wait for IP unblock.")
            return
        await page.close() 

        logger.info("--- Starting Data Acquisition with %s workers ---", MAX_CONCURRENT_TICKERS)

        
        tasks = []
        for ticker in tickers_to_process:
            tasks.append(worker(ticker, context, TODAY_DIR, all_tickers, counters))

        
        for i in range(0, len(tasks), MAX_CONCURRENT_TICKERS):
            batch = tasks[i:i + MAX_CONCURRENT_TICKERS]
            await asyncio.gather(*batch)
            
            
            
            # await asyncio.sleep(0.5) 

        await context.close()

    # 7. Final Report
    final_success_count = initial_processed_count + counters['success_count']
    final_skipped_count = counters['skipped_count']

    generate_report(BASE_OUTPUT_DIR, start_time, len(all_tickers), final_success_count, final_skipped_count)
    print("\n--- 🏁 ALL OPERATIONS COMPLETED ---")

if __name__ == "__main__":
    asyncio.run(main())
