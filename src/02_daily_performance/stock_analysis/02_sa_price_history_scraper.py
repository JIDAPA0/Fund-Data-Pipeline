import asyncio
import sys
import os
import csv
import pandas as pd
import re
import random
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# ==========================================
# 0. SETUP PATHS & IMPORTS
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../.."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.path_manager import VAL_SA_HIST, VAL_SA_MASTER
from src.utils.browser_utils import human_mouse_move
from src.utils.db_connector import get_db_connection, get_active_tickers
from src.utils.hasher import calculate_row_hash
from src.utils.logger import setup_logger, log_execution_summary

USE_STEALTH = False
try:
    from playwright_stealth import stealth_async
    USE_STEALTH = True
    print("✅ Stealth Module Loaded.")
except ImportError:
    print("⚠️  Stealth Module NOT FOUND.")
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
OUTPUT_DIR = VAL_SA_HIST / current_date / "Price_History"
ERROR_SCREENSHOT_DIR = OUTPUT_DIR / "errors_screenshots"
ERROR_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

logger = setup_logger("SA_Price_History")

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

async def login_to_sa(context):
    page = await context.new_page()
    if USE_STEALTH: await stealth_async(page)
    try:
        logger.info("🔐 Login...")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        if "Just a moment" in await page.title(): await asyncio.sleep(10)
        await page.fill('input[type="email"]', SA_EMAIL); await asyncio.sleep(0.5)
        await page.fill('input[name="password"]', SA_PASSWORD); await asyncio.sleep(0.5)
        await page.click('button:has-text("Log in")')
        try:
            await page.wait_for_url(lambda url: "login" not in url, timeout=30000)
            logger.info("✅ Login Success")
            return True
        except: return False
    except: return False
    finally: await page.close()

def get_db_state():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            SELECT ticker, MAX(date) as last_date 
            FROM stg_price_history 
            WHERE source='stock_analysis' 
            GROUP BY ticker
        """
        cursor.execute(query)
        results = cursor.fetchall()
        db_state = {row[0]: str(row[1]) for row in results}
        cursor.close(); conn.close()
        logger.info(f"💾 Loaded DB State: {len(db_state)} tickers found in DB.")
        return db_state
    except Exception as e:
        logger.warning(f"⚠️ Could not load DB state: {e}")
        return {}

async def perform_download_csv(page, ticker, temp_dir):
    try:
        range_btn = page.locator('button', has_text=re.compile(r'Months|Year|YTD|Max|Full History')).first
        if await range_btn.is_visible():
            if "Full History" not in await range_btn.inner_text():
                await range_btn.click()
                await page.locator('div[role="menu"] button:has-text("Full History")').click()
                await asyncio.sleep(2.5) 
        
        await page.locator('button:has-text("Download")').click()
        async with page.expect_download(timeout=30000) as download_info:
            await page.get_by_text("Download to CSV").click()
        
        download = await download_info.value
        temp_path = temp_dir / f"temp_{ticker}.csv"
        await download.save_as(temp_path)
        return temp_path
    except: return None

async def process_ticker(context, ticker, db_state, progress_str):
    page = await context.new_page()
    if USE_STEALTH: await stealth_async(page)
    
    await page.route("**/*.{png,jpg,jpeg,gif,webp,svg,css,woff,woff2}", lambda route: route.abort())

    status = "error"
    try:
        url = f"https://stockanalysis.com/etf/{ticker}/history/"
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        
        if "Just a moment" in await page.title():
             logger.warning(f"{progress_str} 🛡️ Cloudflare. Sleeping 30s...")
             await asyncio.sleep(30); await page.reload()

        await asyncio.sleep(1.5)

        try:
            await page.wait_for_selector('table tbody tr', timeout=6000)
        except:
            await page.screenshot(path=ERROR_SCREENSHOT_DIR / f"{ticker}_nodata.png")
            return "not_found"

        temp_path = await perform_download_csv(page, ticker, OUTPUT_DIR)
        
        if temp_path and temp_path.exists():
            df = pd.read_csv(temp_path)
            df.rename(columns={'Adj. Close': 'Adj Close', 'Change': 'Change %'}, inplace=True)
            df['row_hash'] = df.apply(lambda row: calculate_row_hash(*row.astype(str).tolist()), axis=1)
            
            final_path = OUTPUT_DIR / f"{ticker}_history.csv"
            df.to_csv(final_path, index=False)
            try: os.remove(temp_path) 
            except: pass
            
            logger.info(f"{progress_str} ✅ {ticker}: {len(df)} rows downloaded.")
            status = "new"
        else:
            status = "error"

    except Exception as e:
        status = "error"
    finally:
        await page.close()
        return status

def get_all_downloaded_tickers(base_path):
    downloaded = set()
    if not base_path.exists(): return downloaded
    for file_path in base_path.rglob("*_history.csv"):
        downloaded.add(file_path.name.replace("_history.csv", ""))
    return downloaded

def load_master_tickers():
    master_files = list(VAL_SA_MASTER.rglob(f"sa_{ASSET_TYPE}_master.csv"))
    if master_files:
        try:
            return pd.read_csv(master_files[-1])['ticker'].astype(str).tolist()
        except Exception:
            pass
    db_rows = get_active_tickers("Stock Analysis")
    tickers = [r["ticker"] for r in db_rows if str(r.get("asset_type", "")).upper() == ASSET_TYPE.upper()]
    logger.info("📋 Loaded %s tickers from DB fallback.", len(tickers))
    return tickers

async def main():
    logger.info(f"🚀 STARTING: SA SCRAPER (Check DB + Local Files)")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_tickers = load_master_tickers()
    if not all_tickers:
        logger.error("❌ No tickers found (master list + DB fallback failed).")
        return

    done_tickers = get_all_downloaded_tickers(VAL_SA_HIST)
    
    db_state = get_db_state()

    queue = [t for t in all_tickers if t not in done_tickers]
    
    logger.info(f"📊 Summary:")
    logger.info(f"   - Total Tickers: {len(all_tickers)}")
    logger.info(f"   - Local Files Found: {len(done_tickers)}")
    logger.info(f"   - To Download: {len(queue)}")

    if not queue: 
        logger.info("✅ All files are up to date.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        
        if not await login_to_sa(context): return

        semaphore = asyncio.Semaphore(2)

        async def worker(t, idx):
            async with semaphore:
                prog = f"[{idx}/{len(queue)}]"
                for attempt in range(2):
                    res = await process_ticker(context, t, db_state, prog)
                    if res != "error": break
                    await asyncio.sleep(5)
                await asyncio.sleep(random.uniform(1.0, 3.0))

        tasks = [worker(t, i+1) for i, t in enumerate(queue)]
        await asyncio.gather(*tasks)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
