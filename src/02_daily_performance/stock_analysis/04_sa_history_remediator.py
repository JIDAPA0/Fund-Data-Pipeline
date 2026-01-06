import asyncio
import sys
import os
import pandas as pd
import random
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# ==========================================
# 0. SETUP SYSTEM PATH
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../.."))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import Utilities
from src.utils.path_manager import VAL_SA_HIST, VAL_SA_MASTER
from src.utils.browser_utils import human_mouse_move
from src.utils.db_connector import get_db_connection
from src.utils.hasher import calculate_row_hash
from src.utils.logger import setup_logger, log_execution_summary

# ✅ STEALTH CHECK
USE_STEALTH = False
try:
    from playwright_stealth import stealth_async
    USE_STEALTH = True
    print("✅ Stealth Module Loaded.")
except ImportError:
    print("⚠️  Stealth Module NOT FOUND. Running in Standard Mode.")
    USE_STEALTH = False

# ==========================================
# 1. CONFIGURATION
# ==========================================
load_dotenv()
SA_EMAIL = os.getenv("SA_EMAIL")
SA_PASSWORD = os.getenv("SA_PASSWORD")
LOGIN_URL = "https://stockanalysis.com/login"

ASSET_TYPE = 'etf'
current_date = datetime.now().strftime('%Y-%m-%d')

# Path ที่เราเก็บไฟล์ประวัติราคา
HISTORY_DIR = VAL_SA_HIST / current_date / "Price_History"
# Path เก็บ Error Screenshot
ERROR_SCREENSHOT_DIR = HISTORY_DIR / "remediator_errors"
ERROR_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

logger = setup_logger("SA_Remediator", "02_perf_remediator")

# ==========================================
# 2. ANALYSIS & GAP DETECTION
# ==========================================
def identify_missing_tickers():
    logger.info("🔍 STARTING GAP ANALYSIS...")
    
    # 1. Load Master List (เป้าหมายทั้งหมด)
    try:
        master_path = list(VAL_SA_MASTER.rglob(f"sa_{ASSET_TYPE}_master.csv"))[-1]
        master_df = pd.read_csv(master_path)
        all_tickers = set(master_df['ticker'].astype(str).str.upper().tolist())
        logger.info(f"📋 Master List Total: {len(all_tickers)}")
    except Exception as e:
        logger.error(f"❌ Failed to load Master List: {e}")
        return []

    # 2. Check Existing Files (งานที่เสร็จแล้ว)
    if not HISTORY_DIR.exists():
        logger.warning("⚠️ History directory not found. Assuming 0 files downloaded.")
        existing_files = set()
    else:
        # เช็คไฟล์ที่มีนามสกุล .csv และขนาดต้องมากกว่า 100 bytes (กันไฟล์เปล่า)
        existing_files = {
            f.name.replace('_history.csv', '').upper() 
            for f in HISTORY_DIR.glob('*_history.csv') 
            if f.stat().st_size > 100
        }
        logger.info(f"📂 Found {len(existing_files)} valid history files.")

    # 3. Calculate Gap (งานที่เหลือ)
    missing_tickers = list(all_tickers - existing_files)
    missing_tickers.sort() # เรียงตามตัวอักษร
    
    logger.info(f"🚨 MISSING TICKERS: {len(missing_tickers)}")
    return missing_tickers

# ==========================================
# 3. SCRAPING LOGIC (เหมือนตัวเดิมแต่เน้น Safe)
# ==========================================
async def login_to_sa(context):
    page = await context.new_page()
    if USE_STEALTH: await stealth_async(page)
    try:
        logger.info("🔐 Login...")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        if "Just a moment" in await page.title(): await asyncio.sleep(15)
        
        await page.fill('input[type="email"]', SA_EMAIL)
        await asyncio.sleep(1)
        await page.fill('input[name="password"]', SA_PASSWORD)
        await asyncio.sleep(1)
        await page.click('button:has-text("Log in")')
        
        try:
            await page.wait_for_url(lambda url: "login" not in url, timeout=30000)
            logger.info("✅ Login Success")
            return True
        except:
            return False
    except: return False
    finally: await page.close()

async def perform_download_csv(page, ticker):
    try:
        # พยายามกดปุ่ม Download
        download_btn = page.locator('button:has-text("Download")').first
        if await download_btn.is_visible():
            await download_btn.click()
            await asyncio.sleep(2) # รอเมนูเด้ง
            
            csv_option = page.get_by_text("Download to CSV")
            if await csv_option.is_visible():
                async with page.expect_download(timeout=60000) as download_info:
                    await csv_option.click()
                
                download = await download_info.value
                temp_path = HISTORY_DIR / f"temp_{ticker}.csv"
                await download.save_as(temp_path)
                return temp_path
        return None
    except Exception as e:
        logger.error(f"Download Error: {e}")
        return None

async def process_ticker(context, ticker, progress_str):
    page = await context.new_page()
    if USE_STEALTH: await stealth_async(page)
    
    status = "error"
    try:
        url = f"https://stockanalysis.com/etf/{ticker}/history/"
        await page.goto(url, timeout=90000, wait_until="domcontentloaded")
        
        # Check Cloudflare
        if "Just a moment" in await page.title():
            logger.warning(f"{progress_str} 🛡️ Hit Cloudflare. Resting 45s...")
            await asyncio.sleep(45)
            await page.reload()

        await asyncio.sleep(random.uniform(3, 5))

        # Check for data table
        try:
            await page.wait_for_selector('table tbody tr', timeout=10000)
        except:
            # ถ้าไม่เจอตาราง ให้แคปหน้าจอเก็บไว้เป็นหลักฐาน
            logger.warning(f"{progress_str} ❌ No Data. Screenshot saved.")
            await page.screenshot(path=ERROR_SCREENSHOT_DIR / f"{ticker}_missing.png")
            return "not_found"

        # Download
        temp_csv = await perform_download_csv(page, ticker)
        
        if temp_csv and temp_csv.exists():
            # Process & Save
            df = pd.read_csv(temp_csv)
            
            # Simple Cleaning
            rename_map = {'Adj. Close': 'Adj Close', 'Change': 'Change %'}
            df.rename(columns=rename_map, inplace=True)
            df['row_hash'] = df.apply(lambda row: calculate_row_hash(*row.astype(str).tolist()), axis=1)
            
            final_path = HISTORY_DIR / f"{ticker}_history.csv"
            df.to_csv(final_path, index=False)
            
            try: os.remove(temp_csv) 
            except: pass
            
            logger.info(f"{progress_str} ✅ Fixed: {ticker} ({len(df)} rows)")
            status = "fixed"
        else:
            status = "error"

    except Exception as e:
        logger.error(f"{progress_str} ❌ Error: {e}")
        status = "error"
    finally:
        await page.close()
        return status

# ==========================================
# 4. MAIN REMEDIATION LOOP
# ==========================================
async def main():
    start_time = time.time()
    logger.info("="*50)
    logger.info("🚑 STARTING SA REMEDIATOR (GAP FILLER)")
    logger.info("="*50)

    # 1. หาตัวที่หายไป
    missing_list = identify_missing_tickers()
    
    if not missing_list:
        logger.info("✨ No missing tickers found! Your data is complete.")
        return

    # 2. เริ่มไล่เก็บเฉพาะตัวที่หาย
    stats = {"fixed": 0, "error": 0, "not_found": 0}
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        if not await login_to_sa(context):
            logger.error("❌ Login failed.")
            return

        total = len(missing_list)
        for i, ticker in enumerate(missing_list):
            progress = f"[{i+1}/{total}]"
            
            # Retry Logic (ลอง 2 รอบ)
            for attempt in range(2):
                res = await process_ticker(context, ticker, progress)
                if res != "error":
                    stats[res] += 1
                    break
                else:
                    logger.warning(f"{progress} ⚠️ Retry {ticker}...")
                    await asyncio.sleep(10)
            
            if res == "error": stats["error"] += 1
            
            # พักระหว่างตัว (สุ่ม)
            await asyncio.sleep(random.uniform(5, 10))

        await browser.close()

    # 3. Final Report
    logger.info("="*50)
    logger.info(f"🏁 REMEDIATION COMPLETE")
    logger.info(f"✅ Fixed: {stats['fixed']}")
    logger.info(f"❌ Still Failed: {stats['error']}")
    logger.info(f"🚫 Not Found (Dead): {stats['not_found']}")
    logger.info(f"⏱️ Time: {(time.time()-start_time)/60:.2f} min")
    logger.info("="*50)

if __name__ == "__main__":
    asyncio.run(main())