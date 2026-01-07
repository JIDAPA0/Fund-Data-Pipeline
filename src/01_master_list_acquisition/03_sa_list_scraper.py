import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
import time
import shutil
import sys
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
load_dotenv(project_root / ".env")

SA_EMAIL = os.getenv("SA_EMAIL")
SA_PASSWORD = os.getenv("SA_PASSWORD")
SA_HEADLESS = os.getenv("SA_HEADLESS", "true").lower() not in {"0", "false", "no"}
SA_STORAGE_STATE = project_root / "tmp" / "sa_storage_state.json"

logger = setup_logger("01_master_SA")

LOGIN_URL = "https://stockanalysis.com/login"
SCREENER_URL = "https://stockanalysis.com/etf/screener/"
FINAL_COLUMNS = ['ticker', 'asset_type', 'name', 'status', 'source', 'date_added']

# ==========================================
# HELPER FUNCTIONS
# ==========================================
async def is_login_required(page):
    if "login" in page.url.lower():
        return True
    if await page.locator('input[type="email"]').count() > 0:
        return True
    return False

async def perform_login(page):
    try:
        if not SA_EMAIL or not SA_PASSWORD:
            logger.error("❌ Credentials missing in .env file!")
            return False

        logger.info("🔐 Navigating to Login Page...")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        
        email_selector = 'input[type="email"], input[name*="mail" i], input[id*="mail" i]'
        password_selector = (
            'input[type="password"], input[name*="pass" i], input[id*="pass" i], '
            'input[autocomplete="current-password"]'
        )
        await page.wait_for_selector(email_selector, state="visible", timeout=30000)
        
        logger.info(f"👤 Typing Email...") 
        email_input = page.locator(email_selector).first
        await email_input.fill(SA_EMAIL)
        await asyncio.sleep(0.5)
        
        if await page.locator(password_selector).count() == 0:
            for label in ("Continue", "Next"):
                btn = page.locator(f'button:has-text("{label}")').first
                if await btn.count() > 0:
                    logger.info(f"🖱️ Clicking {label} to reveal password field...")
                    await btn.click()
                    break
            else:
                submit_btn = page.locator('button[type="submit"]').first
                if await submit_btn.count() > 0:
                    logger.info("🖱️ Submitting email to reveal password field...")
                    await submit_btn.click()
                else:
                    logger.info("🖱️ Pressing Enter to reveal password field...")
                    await email_input.press("Enter")
        try:
            await page.wait_for_selector(password_selector, state="visible", timeout=30000)
            await page.fill(password_selector, SA_PASSWORD)
        except Exception:
            content = await page.content()
            if "You're In! Here's what's next" in content or "\"role\":\"authenticated\"" in content:
                logger.info("✅ Detected authenticated session without password prompt.")
                return True
            if await page.locator('text=Log out, text=Logout').count() > 0:
                logger.info("✅ Detected logged-in state on login page.")
                return True
            raise
        await asyncio.sleep(0.5)
        
        logger.info("🖱️ Clicking Log in...")
        login_btn = page.locator('button:has-text("Log in"), button:has-text("Sign in"), button[type="submit"]').first
        if await login_btn.count() == 0:
            raise RuntimeError("Login button not found")
        await login_btn.click()
        await page.wait_for_load_state("networkidle")
        
        try:
            await page.wait_for_url(lambda url: "login" not in url, timeout=15000)
            logger.info("✅ Login Successful!")
            return True
        except:
            logger.warning("⚠️ Login check timed out. Checking UI elements...")
            if await page.locator('text=Log out, text=Logout, a[href*="logout"]').count() > 0:
                return True
            try:
                screenshot_path = project_root / "logs" / f"sa_login_failed_{int(time.time())}.png"
                screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                await page.screenshot(path=str(screenshot_path), full_page=True)
                logger.warning(f"Saved login failure screenshot: {screenshot_path}")
            except Exception:
                pass
            return False

    except Exception as e:
        try:
            ts = int(time.time())
            screenshot_path = project_root / "logs" / f"sa_login_error_{ts}.png"
            screenshot_path.parent.mkdir(parents=True, exist_ok=True)
            await page.screenshot(path=str(screenshot_path), full_page=True)
            html_path = project_root / "logs" / f"sa_login_error_{ts}.html"
            html_path.write_text(await page.content(), encoding="utf-8")
            logger.warning(f"Saved login error artifacts: {screenshot_path}, {html_path}")
        except Exception:
            pass
        logger.error(f"❌ Login Error: {e}")
        return False

async def setup_indicators(page):
    try:
        logger.info("🖱️ Clicking 'Indicators' button...")
        btn = page.locator('button[aria-haspopup="menu"]').filter(has_text="Indicators")
        if await btn.count() == 0:
            btn = page.locator('button:has-text("Indicators")')
        if await btn.count() == 0:
            raise RuntimeError("Indicators button not found")
        await btn.first.click()
        
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
        try:
            await page.wait_for_load_state('networkidle', timeout=30000)
        except Exception:
            await page.wait_for_load_state('domcontentloaded')
        
        download_main_btn = page.locator('button:has-text("Export"), button:has-text("Download")').first
        try:
            await download_main_btn.wait_for(state="visible", timeout=30000)
        except Exception:
            try:
                screenshot_path = project_root / "logs" / f"sa_download_failed_{int(time.time())}.png"
                screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                await page.screenshot(path=str(screenshot_path), full_page=True)
                logger.warning(f"Saved download failure screenshot: {screenshot_path}")
            except Exception:
                pass
            logger.error("❌ Download button NOT FOUND.")
            return None, False
        
        if await download_main_btn.is_visible():
            logger.info("🖱️ Clicking 'Export/Download'...")
            await download_main_btn.click()
            
            csv_option = page.get_by_text("Download to CSV", exact=False).first
            
            logger.info("⏳ Waiting for download event...")
            async with page.expect_download(timeout=90000) as download_info:
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
        df['source'] = 'Stock Analysis'
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
    
    temp_dir = project_root / "tmp" / "sa_downloads"
    temp_dir.mkdir(parents=True, exist_ok=True)
    download_dir = temp_dir
    final_dfs = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=SA_HEADLESS,
            args=["--start-maximized", "--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        
        context_kwargs = {
            "user_agent": get_random_user_agent(),
            "viewport": {"width": 1920, "height": 1080},
            "accept_downloads": True,
        }
        if SA_STORAGE_STATE.exists():
            context_kwargs["storage_state"] = str(SA_STORAGE_STATE)

        context = await browser.new_context(**context_kwargs)
        page = await context.new_page()
        
        asset_type = "ETF"
        logger.info(f"🌐 Navigating to Screener: {SCREENER_URL}")
        await page.goto(SCREENER_URL, wait_until="domcontentloaded", timeout=60000)
        if await is_login_required(page):
            logger.warning("⚠️ Login required. Attempting re-auth...")
            if not await perform_login(page):
                logger.error("❌ Login failed. Aborting scraper.")
                await browser.close()
                return
            SA_STORAGE_STATE.parent.mkdir(parents=True, exist_ok=True)
            await context.storage_state(path=str(SA_STORAGE_STATE))
            await page.goto(SCREENER_URL, wait_until="domcontentloaded", timeout=60000)

        try:
            await page.wait_for_selector('table tbody tr', timeout=15000)
        except Exception:
            logger.warning("⚠️ Table load timeout")

        await mimic_reading(page, min_sec=2, max_sec=3)
        
        await setup_indicators(page)
        csv_path, success = await download_data(page, download_dir, asset_type)
        
        if success and csv_path:
            df = process_csv_data(csv_path, asset_type)
            if df is not None and not df.empty:
                logger.info(f"📊 Extracted {len(df)} rows")
                final_dfs.append(df)
            try:
                os.remove(csv_path)
            except Exception:
                pass
        
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
    
    shutil.rmtree(temp_dir, ignore_errors=True)
    
    log_execution_summary(
        logger,
        start_time=start_time,
        total_items=total_rows,
        status="Completed",
        extra_info={"Mode": "Direct Login Env"}
    )

if __name__ == "__main__":
    asyncio.run(run_sa_scraper())
