import asyncio
import sys
import os
import csv
import pandas as pd
import random
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.async_api import async_playwright


try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    print("⚠️  Warning: 'yfinance' library not found. Will use Direct API only.")

# ==========================================
# 0. SETUP PATHS & IMPORTS
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../.."))
if project_root not in sys.path:
    sys.path.append(project_root)


ASSET_TYPE = 'fund'

from src.utils.path_manager import VAL_YF_HIST, VAL_YF_MASTER
from src.utils.logger import setup_logger, log_execution_summary

# ==========================================
# 1. CONFIGURATION
# ==========================================
load_dotenv()
current_date = datetime.now().strftime('%Y-%m-%d')

OUTPUT_DIR = VAL_YF_HIST / "Dividend_History" / current_date / ASSET_TYPE
ERROR_SCREENSHOT_DIR = OUTPUT_DIR / "errors_screenshots"
ERROR_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

logger = setup_logger("YF_Fund_Dividend_Scraper")

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def download_via_yfinance(ticker):
    if not HAS_YFINANCE: return None
    try:
        
        t = yf.Ticker(ticker)
        divs = t.dividends
        
        if not divs.empty:
            
            df = divs.reset_index()
            df.columns = ['Date', 'Dividend']
            
            
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            return df
    except Exception as e:
        
        pass
    return None

async def download_via_direct_api(context, ticker):
    page = await context.new_page()
    
    await page.route("**/*.{png,jpg,jpeg,gif,webp,svg,css,woff,woff2}", lambda route: route.abort())
    
    final_df = None
    try:
        current_timestamp = int(time.time())
        
        download_url = (
            f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?"
            f"period1=0&period2={current_timestamp}&"
            f"interval=1d&events=div&includeAdjustedClose=true"
        )
        
        
        try:
            async with page.expect_download(timeout=10000) as download_info:
                await page.evaluate(f"window.location.href = '{download_url}'")
            
            download = await download_info.value
            temp_path = OUTPUT_DIR / f"temp_{ticker}_div.csv"
            await download.save_as(temp_path)
            
            
            if temp_path.exists() and temp_path.stat().st_size > 40:
                df = pd.read_csv(temp_path)
                if len(df) > 0:
                    final_df = df
            
            
            try: os.remove(temp_path)
            except: pass

        except Exception:
            
            pass

    except Exception as e:
        logger.error(f"   ❌ Direct API Error: {e}")
    finally:
        await page.close()
    
    return final_df

async def process_ticker(context, ticker, progress_str):
    final_df = None
    status = "no_div" # Default status
    
    
    final_df = download_via_yfinance(ticker)
    
    
    if final_df is None or final_df.empty:
        final_df = await download_via_direct_api(context, ticker)
    
    # Save Result
    if final_df is not None and not final_df.empty:
        final_path = OUTPUT_DIR / f"{ticker}_dividend.csv"
        final_df.to_csv(final_path, index=False)
        logger.info(f"{progress_str} 💰 {ticker}: Found Dividend ({len(final_df)} rows).")
        status = "success"
    else:
        
        logger.info(f"{progress_str} ⚪ {ticker}: No Dividend data.")
        status = "no_data"
        
    return status

def get_all_downloaded_tickers(base_path):
    """Resume Logic"""
    downloaded = set()
    if not base_path.exists(): return downloaded
    for file_path in base_path.rglob("*_dividend.csv"):
        downloaded.add(file_path.name.replace("_dividend.csv", ""))
    return downloaded

async def main():
    logger.info(f"🚀 STARTING: YF FUND DIVIDEND SCRAPER")
    start_time = time.time()
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load Master List (Fund)
    try:
        master_path = list(VAL_YF_MASTER.rglob(f"yf_{ASSET_TYPE}_master.csv"))[-1]
        all_tickers = pd.read_csv(master_path)['ticker'].astype(str).tolist()
        logger.info(f"📋 Master List ({ASSET_TYPE.upper()}): {len(all_tickers)} tickers")
    except Exception as e:
        logger.error(f"❌ Master list error: {e}")
        return

    # Smart Resume
    done_tickers = get_all_downloaded_tickers(VAL_YF_HIST) 
    queue = [t for t in all_tickers if t not in done_tickers]
    logger.info(f"⏭️  Skipped: {len(done_tickers)} | ▶️  Remaining: {len(queue)}")

    if not queue: 
        logger.info("✅ All tickers done!")
        return

    stats = {"success": 0, "no_data": 0, "error": 0}
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

        
        semaphore = asyncio.Semaphore(4)

        async def worker(t, idx):
            async with semaphore:
                prog = f"[{idx}/{len(all_tickers)}]"
                res = await process_ticker(context, t, prog)
                
                if res in stats: stats[res] += 1
                
                
                await asyncio.sleep(random.uniform(0.5, 1.5))

        tasks = [worker(t, i+1+len(done_tickers)) for i, t in enumerate(queue)]
        await asyncio.gather(*tasks)
        await browser.close()

    log_execution_summary(logger, start_time, sum(stats.values()), "Completed", stats)

if __name__ == "__main__":
    asyncio.run(main())