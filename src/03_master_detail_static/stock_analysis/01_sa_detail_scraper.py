import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
import random
import time
import shutil
import sys
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# ==========================================
# SYSTEM PATH SETUP
# ==========================================
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[3] 
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# ==========================================
# LOGGING SETUP
# ==========================================
try:
    from src.utils.path_manager import get_validation_path
    from src.utils.logger import setup_logger, log_execution_summary
    from src.utils.browser_utils import get_random_user_agent, mimic_reading
except (ImportError, ModuleNotFoundError):
    import logging
    
    # Fallback Functions
    def setup_logger(name, category="03_static"):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logger = logging.getLogger(name)
        return logger

    def log_execution_summary(logger, **kwargs):
        logger.info(f"Execution Summary: {kwargs}")

    def get_validation_path(*args): 
        p = Path("validation_output").joinpath(*args)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def get_random_user_agent(): 
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    
    async def mimic_reading(page, min_sec=1, max_sec=2): 
        await asyncio.sleep(random.uniform(min_sec, max_sec))

# ==========================================
# CONFIGURATION
# ==========================================
load_dotenv() 

SA_EMAIL = os.getenv("SA_EMAIL")
SA_PASSWORD = os.getenv("SA_PASSWORD")

logger = setup_logger("SA_Detail_Scraper", "03_static")

LOGIN_URL = "https://stockanalysis.com/login"
SCREENER_URL = "https://stockanalysis.com/etf/screener/"

# --- MAPPINGS CONFIGURATION (FIXED BASED ON ACTUAL CSV) ---

# 1. INFO MAPPING (Identity & Profile)
INFO_MAPPING = {
    "Symbol": "ticker", 
    "Fund Name": "name",           # CSV จริงชื่อ Fund Name
    "ISIN Number": "isin_number", 
    "CUSIP Number": "cusip_number",
    "Issuer": "issuer", 
    "Category": "category", 
    "Index": "index_benchmark",
    "Inception": "inception_date", # CSV จริงชื่อ Inception
    "Exchange": "exchange", 
    "Region": "region", 
    "Country": "country",
    "Leverage": "leverage", 
    "Options": "options", 
    "Shares": "shares_out"         # CSV จริงชื่อ Shares
}
INFO_COLUMNS = [
    'ticker', 'asset_type', 'source', 'name', 
    'isin_number', 'cusip_number', 'issuer', 'category', 'index_benchmark',
    'inception_date', 'exchange', 'region', 'country', 
    'leverage', 'options', 'shares_out', 'market_cap_size'
]

# 2. FEES MAPPING (Fees & Operations)
FEES_MAPPING = {
    "Exp. Ratio": "expense_ratio", # CSV จริงชื่อ Exp. Ratio
    "Assets": "assets_aum",
    "Holdings": "holdings_count"   # CSV จริงชื่อ Holdings
}
FEES_COLUMNS = [
    'ticker', 'asset_type', 'source', 
    'expense_ratio', 'initial_charge', 'exit_charge', 
    'assets_aum', 'top_10_hold_pct', 'holdings_count', 'holdings_turnover'
]

# 3. RISK MAPPING (Risk & Technicals)
RISK_MAPPING = {
    "Sharpe": "sharpe_ratio_5y",   # CSV จริงชื่อ Sharpe
    "Beta (5Y)": "beta_5y",
    "RSI": "rsi_daily",            # CSV จริงชื่อ RSI
    "200 MA": "moving_avg_200"     # CSV จริงชื่อ 200 MA
}
RISK_COLUMNS = [
    'ticker', 'asset_type', 'source',
    'sharpe_ratio_5y', 'beta_5y', 'rsi_daily', 'moving_avg_200'
]

# 4. POLICY MAPPING (Policy & Performance)
POLICY_MAPPING = {
    "Div. Yield": "div_yield",           # CSV จริงชื่อ Div. Yield
    "Div. Growth": "div_growth_1y",      # CSV จริงชื่อ Div. Growth
    "Div. Growth 3Y": "div_growth_3y",
    "Div. Growth 5Y": "div_growth_5y",
    "Div. Growth 10Y": "div_growth_10y",
    "Years": "div_consecutive_years",    # CSV จริงชื่อ Years
    "Payout Ratio": "payout_ratio",
    "Return YTD": "total_return_ytd",    # ใช้ Return YTD แทน Change YTD ตามความเหมาะสมของชื่อ DB (Total Return)
    "Return 1Y": "total_return_1y",      # ใช้ Return 1Y
    "PE Ratio": "pe_ratio"
}
POLICY_COLUMNS = [
    'ticker', 'asset_type', 'source',
    'div_yield', 'div_growth_1y', 'div_growth_3y', 'div_growth_5y', 'div_growth_10y',
    'div_consecutive_years', 'payout_ratio', 'total_return_ytd', 'total_return_1y', 'pe_ratio'
]

# รวม Mapping ทั้งหมด
FULL_MAPPING = {**INFO_MAPPING, **FEES_MAPPING, **RISK_MAPPING, **POLICY_MAPPING}

# ==========================================
# HELPER FUNCTIONS
# ==========================================
async def perform_login(page):
    try:
        if not SA_EMAIL or not SA_PASSWORD:
            logger.error("❌ Credentials missing in .env file!")
            return False

        logger.info("🔐 Navigating to Login Page...")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(1)
        
        if await page.locator('a[href="/pro/account/"]').count() > 0:
            logger.info("✅ Already logged in!")
            return True
        
        await page.wait_for_selector('input[type="email"]', state="visible", timeout=30000)
        await page.fill('input[type="email"]', SA_EMAIL)
        await asyncio.sleep(0.3)
        await page.fill('input[name="password"]', SA_PASSWORD)
        await asyncio.sleep(0.3)
        await page.click('button:has-text("Log in")')
        
        try:
            await page.wait_for_url(lambda url: "login" not in url, timeout=30000)
            logger.info("✅ Login Successful!")
            return True
        except:
            if await page.locator('text=Log out').count() > 0: return True
            logger.error("❌ Login check timed out.")
            return False

    except Exception as e:
        logger.error(f"❌ Login Error: {e}")
        return False

async def switch_to_all_indicators(page):
    """Switch to 'All Indicators' view"""
    logger.info("👀 Switching to 'All Indicators' tab...")
    try:
        tab_btn = page.locator('button').filter(has_text="All Indicators").first
        if await tab_btn.is_visible():
            await tab_btn.click()
            logger.info("✅ Clicked 'All Indicators'. Waiting for table update...")
            await asyncio.sleep(3) 
            await page.wait_for_load_state('networkidle')
            return True
        else:
            logger.error("❌ Tab 'All Indicators' not found!")
            return False
    except Exception as e:
        logger.error(f"⚠️ Error switching tab: {e}")
        return False

async def download_data(page, temp_dir):
    try:
        logger.info("⬇️ Downloading CSV...")
        download_btn = page.locator('button:has-text("Export"), button:has-text("Download")').first
        
        if await download_btn.is_visible():
            await download_btn.click()
            await asyncio.sleep(1)
            
            csv_option = page.locator('li:has-text("CSV"), button:has-text("CSV")').first
            
            async with page.expect_download(timeout=60000) as download_info:
                if await csv_option.is_visible():
                    await csv_option.click()
                else:
                    logger.info("ℹ️ Direct download triggered")
            
            download = await download_info.value
            temp_filename = f"raw_sa_all_{int(time.time())}.csv"
            temp_path = temp_dir / temp_filename
            await download.save_as(temp_path)
            logger.info(f"✅ Downloaded: {temp_path.name}")
            return temp_path, True
        else:
            logger.error("❌ Download button NOT FOUND.")
            return None, False

    except Exception as e:
        logger.error(f"❌ Download Error: {e}")
        return None, False

def prepare_dataframe(df_source, target_columns, extra_defaults={}):
    """Select columns, rename, add defaults, and reorder."""
    df_out = pd.DataFrame()
    
    # Iterate over required columns
    for col in target_columns:
        if col in df_source.columns:
            df_out[col] = df_source[col]
        elif col in extra_defaults:
            df_out[col] = extra_defaults[col]
        else:
            df_out[col] = None
            
    return df_out

def process_csv_and_split(csv_path, output_dir):
    try:
        logger.info("🔄 Processing & Splitting Data...")
        df_raw = pd.read_csv(csv_path)
        
        # Rename columns using FIXED Full Mapping
        df_raw.rename(columns=FULL_MAPPING, inplace=True)
        
        # Add Common Columns
        df_raw['asset_type'] = 'ETF'
        df_raw['source'] = 'Stock Analysis'
        
        # --- 1. Info (Identity) ---
        df_info = prepare_dataframe(df_raw, INFO_COLUMNS)
        path_info = output_dir / "sa_fund_info.csv"
        df_info.to_csv(path_info, index=False)
        logger.info(f"✅ Generated: {path_info.name} ({len(df_info)} rows)")
        
        # --- 2. Fees (Fees & Operations) ---
        fees_defaults = {
            'initial_charge': None,
            'exit_charge': None,
            'top_10_hold_pct': None,
            'holdings_turnover': None
        }
        df_fees = prepare_dataframe(df_raw, FEES_COLUMNS, fees_defaults)
        path_fees = output_dir / "sa_fund_fees.csv"
        df_fees.to_csv(path_fees, index=False)
        logger.info(f"✅ Generated: {path_fees.name}")

        # --- 3. Risk (Risk & Technicals) ---
        df_risk = prepare_dataframe(df_raw, RISK_COLUMNS)
        path_risk = output_dir / "sa_fund_risk.csv"
        df_risk.to_csv(path_risk, index=False)
        logger.info(f"✅ Generated: {path_risk.name}")

        # --- 4. Policy (Policy & Performance) ---
        df_policy = prepare_dataframe(df_raw, POLICY_COLUMNS)
        path_policy = output_dir / "sa_fund_policy.csv"
        df_policy.to_csv(path_policy, index=False)
        logger.info(f"✅ Generated: {path_policy.name}")

        return True

    except Exception as e:
        logger.error(f"❌ Processing Error: {e}")
        return False

# ==========================================
# MAIN EXECUTION
# ==========================================
async def run_sa_full_scraper():
    logger.info("🎥 STARTING FULL SCRAPER (Corrected Headers)")
    start_time = time.time()
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    output_dir = get_validation_path("Stock_Analysis", "03_Detail_Static", today_str)
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = output_dir / "temp_downloads"
    temp_dir.mkdir(exist_ok=True)

    success_process = False

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, args=["--start-maximized"])
        context = await browser.new_context(viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()
        
        try:
            if await perform_login(page):
                logger.info(f"🌐 Navigating to Screener...")
                await page.goto(SCREENER_URL, wait_until="domcontentloaded", timeout=60000)
                try: await page.wait_for_selector('table tbody tr', timeout=30000)
                except: pass
                
                await mimic_reading(page, min_sec=2, max_sec=3)
                
                if await switch_to_all_indicators(page):
                    csv_path, downloaded = await download_data(page, temp_dir)
                    if downloaded and csv_path:
                        await browser.close()
                        success_process = process_csv_and_split(csv_path, output_dir)
                        try: os.remove(csv_path)
                        except: pass
                    else:
                        await browser.close()
                else:
                    logger.error("❌ Could not switch to All Indicators view.")
                    await browser.close()
            else:
                logger.error("❌ Login failed.")
                await browser.close()
        except Exception as e:
            logger.error(f"❌ Critical Error: {e}")
            await browser.close()
    
    try: 
        if temp_dir.exists(): shutil.rmtree(temp_dir)
    except: pass
    
    log_execution_summary(logger, start_time=start_time, total_items=1 if success_process else 0, status="Completed" if success_process else "Failed")

if __name__ == "__main__":
    asyncio.run(run_sa_full_scraper())