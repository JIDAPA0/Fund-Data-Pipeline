import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
import random
import time
import shutil
import json
import sys
import logging
from dotenv import load_dotenv

# ==========================================
# SYSTEM PATH SETUP
# ==========================================
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[1]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# ==========================================
# IMPORTS
# ==========================================
from src.utils.path_manager import get_validation_path
from src.utils.logger import setup_logger, log_execution_summary
from src.utils.browser_utils import (
    get_random_user_agent,
    mimic_reading
)

# ==========================================
# CONFIGURATION
# ==========================================
load_dotenv() 

SA_EMAIL = os.getenv("SA_EMAIL")
SA_PASSWORD = os.getenv("SA_PASSWORD")

logger = setup_logger("01_master_SA")

LOGIN_URL = "https://stockanalysis.com/login"
SCREENER_URL = "https://stockanalysis.com/etf/screener/"
FINAL_COLUMNS = ['ticker', 'asset_type', 'name', 'status', 'source', 'date_added']

# ==========================================
# HELPER FUNCTIONS
# ==========================================
async def perform_login(page):
    try:
        if not SA_EMAIL or not SA_PASSWORD:
            logger.error("❌ Credentials missing in .env file!")
            return False

        logger.info("🔐 Navigating to Login Page...")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        
        await page.wait_for_selector('input[type="email"]', state="visible")
        
        logger.info(f"👤 Typing Email...") 
        await page.fill('input[type="email"]', SA_EMAIL)
        await asyncio.sleep(0.5)
        
        await page.fill('input[name="password"]', SA_PASSWORD)
        await asyncio.sleep(0.5)
        
        logger.info("🖱️ Clicking Log in...")
        await page.click('button:has-text("Log in")')
        
        try:
            await page.wait_for_url(lambda url: "login" not in url, timeout=15000)
            logger.info("✅ Login Successful!")
            return True
        except:
            logger.warning("⚠️ Login check timed out. Checking UI elements...")
            if await page.locator('text=Log out').count() > 0:
                return True
            return False

    except Exception as e:
        logger.error(f"❌ Login Error: {e}")
        return False

async def setup_indicators(page):
    try:
        logger.info("🖱️ Clicking 'Indicators' button...")
        btn = page.locator('button:has-text("Indicators")')
        await btn.click()
        
        await page.wait_for_selector('div[role="menu"]', state="visible", timeout=5000)
        
        search_box = page.locator('div[role="menu"] input[placeholder*="Search"]')
        await search_box.fill("Name")
        await asyncio.sleep(1)
        
        name_option = page.locator('div[role="menu"] label').filter(has_text="Name").first
        if await name_option.count() > 0:
            await name_option.click(force=True)
            logger.info("✅ Selected 'Name' column")
            
        await page.mouse.click(0, 0)
        await asyncio.sleep(1)
        
    except Exception as e:
        logger.warning(f"⚠️ Indicator Setup Warning: {e}")

async def download_data(page, temp_dir, asset_type="ETF"):
    try:
        logger.info(f"[{asset_type}] 👀 Looking for Export button...")
        await page.wait_for_load_state('networkidle', timeout=10000)
        
        download_main_btn = page.locator('button:has-text("Export"), button:has-text("Download")').first
        
        if await download_main_btn.is_visible():
            logger.info("🖱️ Clicking 'Export/Download'...")
            await download_main_btn.click()
            
            csv_option = page.get_by_text("Download to CSV", exact=False).first
            
            logger.info("⏳ Waiting for download event...")
            async with page.expect_download(timeout=60000) as download_info:
                await asyncio.sleep(1)
                if await csv_option.is_visible():
                    await csv_option.click(force=True)
                else:
                    await page.get_by_text("CSV", exact=True).first.click(force=True)
            
            download = await download_info.value
            temp_filename = f"raw_{asset_type.lower()}_{int(time.time())}.csv"
            temp_path = temp_dir / temp_filename
            await download.save_as(temp_path)
            logger.info(f"✅ Downloaded: {temp_path.name}")
            return temp_path, True
        else:
            logger.error("❌ Download button NOT FOUND.")
            return None, False

    except Exception as e:
        logger.error(f"❌ Error during download: {e}")
        return None, False

def process_csv_data(csv_path, asset_type):
    try:
        df = pd.read_csv(csv_path)
        rename_map = {'Symbol': 'ticker', 'Fund Name': 'name', 'Name': 'name', 'Company Name': 'name', 'Long Name': 'name'}
        df.rename(columns=rename_map, inplace=True)
        if 'ticker' not in df.columns: 
            logger.error("❌ CSV Missing 'ticker' column")
            return None
        
        df['asset_type'] = asset_type 
        df['status'] = 'new'          
        df['source'] = 'StockAnalysis'
        df['date_added'] = datetime.now().strftime('%Y-%m-%d')
        
        for col in FINAL_COLUMNS:
            if col not in df.columns: df[col] = None
            
        return df[FINAL_COLUMNS]
    except Exception as e:
        logger.error(f"❌ Error processing CSV: {e}")
        return None

# ==========================================
# MAIN EXECUTION
# ==========================================
async def run_sa_scraper():
    logger.info("🎥 STARTING SCRAPER (Direct Login Mode)")
    start_time = time.time()
    
    temp_dir = get_validation_path("Stock_Analysis", "01_List_Master", "temp_downloads")
    temp_dir.mkdir(parents=True, exist_ok=True)
    download_dir = temp_dir.parent 
    final_dfs = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,  
            args=["--start-maximized", "--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        
        context = await browser.new_context(
            user_agent=get_random_user_agent(),
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        
        if await perform_login(page):
            asset_type = "ETF"
            logger.info(f"🌐 Navigating to Screener: {SCREENER_URL}")
            await page.goto(SCREENER_URL, wait_until="domcontentloaded", timeout=60000)
            
            try:
                await page.wait_for_selector('table tbody tr', timeout=15000)
            except:
                logger.warning("⚠️ Table load timeout")

            await mimic_reading(page, min_sec=2, max_sec=3)
            
            await setup_indicators(page)
            csv_path, success = await download_data(page, download_dir, asset_type)
            
            if success and csv_path:
                df = process_csv_data(csv_path, asset_type)
                if df is not None and not df.empty:
                    logger.info(f"📊 Extracted {len(df)} rows")
                    final_dfs.append(df)
                try: os.remove(csv_path)
                except: pass
        else:
             logger.error("❌ Login failed. Aborting scraper.")
        
        logger.info("💤 Done. Closing browser...")
        await asyncio.sleep(2)
        await browser.close()

    total_rows = 0
    if final_dfs:
        final_df = pd.concat(final_dfs, ignore_index=True)
        today_str = datetime.now().strftime("%Y-%m-%d")
        output_filename = f"{today_str}/sa_etf_master.csv"
        final_path = get_validation_path("Stock_Analysis", "01_List_Master", output_filename)
        final_path.parent.mkdir(parents=True, exist_ok=True) 
        final_df.to_csv(final_path, index=False)
        total_rows = len(final_df)
        logger.info(f"🎉 SUCCESS! Saved to: {final_path}")
    else:
        logger.warning("❌ No data scraped.")
    
    try: 
        if temp_dir.exists(): temp_dir.rmdir()
    except: pass
    
    log_execution_summary(
        logger,
        start_time=start_time,
        total_items=total_rows,
        status="Completed",
        extra_info={"Mode": "Direct Login Env"}
    )

if __name__ == "__main__":
    asyncio.run(run_sa_scraper())