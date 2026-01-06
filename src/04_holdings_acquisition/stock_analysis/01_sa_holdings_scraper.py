import os
import csv
import asyncio
import pandas as pd
import configparser
from datetime import datetime
from pathlib import Path
import time
import random
from playwright.async_api import async_playwright, TimeoutError
from typing import List, Dict, Any, Set

# --- ⚙️ CONFIGURATION ---------------------------------------------------------

INPUT_CSV_PATH = "validation_output/Stock_Analysis/01_List_Master/2025-12-03/sa_etf_master.csv"


BASE_OUTPUT_DIR = Path("validation_output/Stock_Analysis/04_Holdings")

# Base URL
BASE_URL = "https://stockanalysis.com/etf/"


BATCH_SIZE = 500 


MAX_CONCURRENT_TICKERS = 4 


def get_config(filename='config/database.ini', section='stock_analysis'):
    parser = configparser.ConfigParser()
    if not os.path.exists(filename):
        filename = os.path.join(os.getcwd(), filename)
        if not os.path.exists(filename):
             raise FileNotFoundError(f"Configuration file not found at: {filename}")
    parser.read(filename)
    if parser.has_section(section):
        return dict(parser.items(section))
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

try:
    CONFIG = get_config()
except Exception as e:
    print(f"FATAL ERROR: Failed to load configuration. Error: {e}")
    exit(1) 

LOGIN_URL = CONFIG.get('login_url')
EMAIL = CONFIG.get('email')
PASS = CONFIG.get('password')


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
    print(f"🔐 Attempting Login as {EMAIL}...")
    try:
        await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        
        
        if "login" in page.url:
            await page.fill('input[type="email"]', EMAIL)
            await page.fill('input[type="password"]', PASS)
            await page.keyboard.press("Enter")
            
            
            await page.wait_for_url(lambda url: "login" not in url, timeout=30000)
            
            if "login" not in page.url:
                print("✅ Login Successful!")
                return True 
            else:
                print("❌ Login Failed (Still on login page)")
                return False 
        else:
            
            print("✅ Session already authenticated or not required.")
            return True

    except Exception as e:
        print(f"❌ Critical Login Error: {e}")
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
    print(f"\n📝 Report: {report_path}")


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
    print("\n--- 🚀 STARTING HOLDINGS DOWNLOADER (SPEED MODE) ---")
    start_time = time.time()
    
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    TODAY_DIR = BASE_OUTPUT_DIR / today_str
    TODAY_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"📂 Target Folder Created: {TODAY_DIR}") 

    
    try:
        df = pd.read_csv(INPUT_CSV_PATH)
        all_tickers = df['ticker'].tolist()
        processed_tickers = get_processed_tickers(TODAY_DIR)
        tickers_to_process = [t for t in all_tickers if t not in processed_tickers]
        
        print(f"📄 Loaded {len(all_tickers)} total tickers.")
        print(f"💾 Found {len(processed_tickers)} tickers already processed (Skipping).")
        print(f"⏳ {len(tickers_to_process)} tickers remaining to process.")
        
        if not tickers_to_process:
            print("🎉 All tickers for today are already processed. Exiting.")
            return
        
    except Exception as e:
        print(f"❌ Error reading Master CSV: {e}")
        return

    
    counters = {
        'total_count': len(processed_tickers), 
        'success_count': 0,
        'skipped_count': 0,
        'lock': asyncio.Lock()
    }
    initial_processed_count = len(processed_tickers)
    
    async with async_playwright() as p:
        
        user_data_dir = "./tmp/sa_session"
        context = await p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=True,
            accept_downloads=True
        )

        
        page = await context.new_page()
        if not await login_to_sa(page):
            await context.close()
            print("🚨 CRITICAL: Initial Login Failed. Please check credentials or wait for IP unblock.")
            return
        await page.close() 

        print(f"\n--- Starting Data Acquisition with {MAX_CONCURRENT_TICKERS} workers ---")

        
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