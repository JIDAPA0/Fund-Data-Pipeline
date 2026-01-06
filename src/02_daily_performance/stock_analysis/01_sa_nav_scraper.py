import asyncio
import sys
import os
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright
from pathlib import Path
import random
import time
import re 
from dotenv import load_dotenv

# ==========================================
# SYSTEM PATH SETUP
# ==========================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(BASE_DIR)

load_dotenv()

# ==========================================
# IMPORTS
# ==========================================
try:
    from src.utils.path_manager import get_validation_path
    from src.utils.logger import setup_logger 
    from src.utils.browser_utils import (
        get_random_user_agent,
        human_mouse_move,
        human_sleep,
        mimic_reading
    )
    from src.utils.db_connector import get_active_tickers, insert_dataframe
    
    logger = setup_logger("02_perf_sa_nav")
except ImportError as e:
    print(f"❌ Setup Error: {e}")
    sys.exit(1)

# ==========================================
# CONFIGURATION
# ==========================================
FIXED_ASSET_TYPE = 'ETF'
FIXED_CURRENCY = 'USD'
SOURCE_NAME = 'Stock Analysis'

SCREENER_URL = "https://stockanalysis.com/etf/screener/"
LOGIN_URL = "https://stockanalysis.com/login"
REFERENCE_URL = "https://stockanalysis.com/etf/spy/" 

EMAIL = os.getenv("SA_EMAIL")
PASS = os.getenv("SA_PASSWORD")

current_date = datetime.now().strftime('%Y-%m-%d')

OUTPUT_FILE = get_validation_path(
    "Stock_Analysis", 
    "02_Daily_NAV", 
    f"{current_date}/sa_nav_etf.csv"
)
OUTPUT_DIR = OUTPUT_FILE.parent

# ==========================================
# HELPER FUNCTIONS
# ==========================================
async def auto_login(page):
    if not EMAIL or not PASS:
        logger.critical("❌ Missing Credentials!")
        return False
        
    logger.info("🔑 Attempting to login...")
    try:
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await human_sleep(1, 2)

        email_selector = 'input[placeholder*="email address"], input[type="email"]'
        await page.wait_for_selector(email_selector, state="visible", timeout=30000)
        
        email_input = page.locator(email_selector).first 
        await email_input.click()
        await email_input.press_sequentially(EMAIL, delay=random.randint(30, 100))
        await human_sleep(0.5, 1.0)
        
        password_input = page.locator('input[name="password"]') 
        await password_input.click()
        await password_input.press_sequentially(PASS, delay=random.randint(30, 100))
        await human_sleep(0.5, 1.0)

        login_button = page.get_by_role("button", name="Log in").first
        await human_mouse_move(page)
        await login_button.click(delay=random.randint(100, 300))
        
        await page.wait_for_url(lambda url: not url.startswith(LOGIN_URL), timeout=60000)
        await human_sleep(2, 4)
        logger.info("✅ Login Successful!")
        return True
    except Exception as e:
        logger.error(f"❌ Login Failed: {e}")
        return False

async def get_market_date_from_spy(page):
    logger.info("📅 Checking Market Date from SPY...")
    try:
        await page.goto(REFERENCE_URL, wait_until="domcontentloaded")
        date_locator = page.locator("div").filter(has_text=re.compile(r"At close:")).first
        
        if await date_locator.count() > 0:
            text = await date_locator.inner_text()
            match = re.search(r"([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})", text)
            if match:
                month_str, day_str, year_str = match.groups()
                date_obj = datetime.strptime(f"{month_str} {day_str} {year_str}", "%b %d %Y")
                return date_obj.strftime("%Y-%m-%d")
            
        return current_date
    except:
        return current_date

async def download_data(page, temp_dir):
    try:
        logger.info("⬇️ Starting download process...")
        download_main_btn = page.locator('button:has-text("Export"), button:has-text("Download")')
        
        if await download_main_btn.count() > 0 and await download_main_btn.is_visible():
            await human_mouse_move(page)
            await download_main_btn.click(delay=random.randint(100, 300))
            
            csv_btn = page.locator('button:has-text("Download to CSV"), div[role="menu"] button:has-text("CSV"), button:has-text("CSV")').first
            
            async with page.expect_download(timeout=60000) as download_info:
                if await csv_btn.is_visible():
                    await human_sleep(0.5, 1.0)
                    await csv_btn.click(delay=random.randint(100, 300))
                else:
                    await csv_btn.click(force=True)
            
            download = await download_info.value
            temp_path = temp_dir / "sa_etf_nav_raw.csv"
            await download.save_as(temp_path)
            return temp_path, True
        return None, False
    except Exception as e:
        logger.error(f"❌ Error during download: {e}")
        return None, False

def process_and_save(csv_path, valid_date, db_tickers):
    logger.info(f"⚙️ Processing CSV (Date: {valid_date})")
    try:
        df = pd.read_csv(csv_path)
        
        df.columns = [c.strip() for c in df.columns]
        rename_map = {'Symbol': 'ticker', 'Stock Price': 'nav_price', 'Price': 'nav_price'}
        df.rename(columns=rename_map, inplace=True)
        
        if 'nav_price' not in df.columns: 
            logger.error("❌ CSV Missing Price column")
            return
            
        df['ticker'] = df['ticker'].astype(str).str.strip().str.upper()
        df['nav_price'] = df['nav_price'].astype(str).str.replace(',', '', regex=False)
        df['nav_price'] = pd.to_numeric(df['nav_price'], errors='coerce')
        df.dropna(subset=['nav_price'], inplace=True)
        
        active_tickers_list = [t['ticker'] for t in db_tickers]
        df = df[df['ticker'].isin(active_tickers_list)].copy()
        
        logger.info(f"🔍 Filtered: {len(df)} tickers match our DB.")

        df['asset_type'] = FIXED_ASSET_TYPE
        df['source'] = SOURCE_NAME
        df['currency'] = FIXED_CURRENCY
        df['as_of_date'] = valid_date 
        df['scrape_date'] = datetime.now().strftime('%Y-%m-%d')
        
        try:
            insert_dataframe(df[['ticker', 'asset_type', 'source', 'nav_price', 'currency', 'as_of_date', 'scrape_date']], "stg_daily_nav")
        except Exception as e:
            logger.warning(f"⚠️ DB Insert Warning: {e}")

        # Save to Validation Path
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"✅ Backup CSV saved: {OUTPUT_FILE}")
        
    except Exception as e:
        logger.error(f"❌ Error processing CSV: {e}")

# ==========================================
# MAIN EXECUTION
# ==========================================
async def main():
    start_time = time.time()
    logger.info("=" * 50)
    logger.info(f"🚀 STARTING: SA NAV SCRAPER ({FIXED_ASSET_TYPE.upper()})")
    
    db_tickers = get_active_tickers("Stock Analysis")
    if not db_tickers:
        logger.warning("🚫 No active tickers found in DB for Stock Analysis.")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=get_random_user_agent())
        page = await context.new_page()
        
        if not await auto_login(page):
            await browser.close()
            return 

        market_date = await get_market_date_from_spy(page)

        logger.info(f"🌍 Navigating to Screener...")
        await page.goto(SCREENER_URL, wait_until="domcontentloaded")
        await mimic_reading(page, min_sec=2, max_sec=4)
        
        csv_path, success = await download_data(page, OUTPUT_DIR)
        await browser.close()

    if success and csv_path and csv_path.exists():
        process_and_save(csv_path, market_date, db_tickers)
        try: os.remove(csv_path) 
        except: pass
    else:
        logger.error("⚠️ Failed to download data.")
        
    total_duration = time.time() - start_time
    logger.info(f"⏱️ Total Time: {total_duration/60:.2f} min")

if __name__ == "__main__":
    asyncio.run(main())