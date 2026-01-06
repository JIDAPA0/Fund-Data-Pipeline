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
    print("⚠️  Warning: 'yfinance' library not found. Will use Table Scraping only.")

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
OUTPUT_DIR = VAL_YF_HIST / "Price_History" / current_date / ASSET_TYPE
ERROR_SCREENSHOT_DIR = OUTPUT_DIR / "errors_screenshots"
ERROR_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

logger = setup_logger("YF_Hybrid_Scraper")

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def download_via_yfinance(ticker):
    if not HAS_YFINANCE: return None
    try:
        logger.info(f"⚡ Using yfinance library for {ticker}...")
        
        df = yf.download(ticker, period="max", progress=False, auto_adjust=False)
        
        if not df.empty:
            
            df = df.reset_index()
            
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            
            cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
            
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
            
            
            available_cols = [c for c in cols if c in df.columns]
            return df[available_cols]
    except Exception as e:
        logger.warning(f"   ⚠️ yfinance failed: {e}")
    return None

async def scrape_table_via_playwright(context, ticker):
    page = await context.new_page()
    
    await page.route("**/*.{png,jpg,jpeg,gif,webp,svg,css,woff,woff2}", lambda route: route.abort())
    
    data = []
    try:
        url = f"https://finance.yahoo.com/quote/{ticker}/history"
        logger.info(f"🕷️ Scraping Table for {ticker}...")
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")

        # Handle Cloudflare / Popups
        if "Just a moment" in await page.title():
             await asyncio.sleep(15)
        
        
        
        
        last_height = await page.evaluate("document.body.scrollHeight")
        retries = 0
        
        
        
        for i in range(30): 
            await page.keyboard.press("End")
            await asyncio.sleep(1.0)
            
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                retries += 1
                if retries >= 3: break 
            else:
                retries = 0
                last_height = new_height
        
        
        rows = page.locator('table[data-test="historical-prices"] tbody tr')
        count = await rows.count()
        
        if count > 0:
            all_texts = await rows.all_inner_texts()
            for text in all_texts:
                
                cols = text.split('\t')
                if len(cols) < 5: cols = text.split('\n')
                
                
                if len(cols) >= 6:
                    
                    try:
                        dt = datetime.strptime(cols[0], "%b %d, %Y")
                        cols[0] = dt.strftime("%Y-%m-%d")
                        data.append(cols[:7])
                    except:
                        pass 

    except Exception as e:
        logger.error(f"   ❌ Table Scraping Error: {e}")
    finally:
        await page.close()
    
    if data:
        headers = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
        return pd.DataFrame(data, columns=headers[:len(data[0])])
    return None

async def process_ticker(context, ticker, progress_str):
    final_df = None
    status = "error"
    
    
    final_df = download_via_yfinance(ticker)
    
    
    if final_df is None or final_df.empty:
        final_df = await scrape_table_via_playwright(context, ticker)
    
    # Save Result
    if final_df is not None and not final_df.empty:
        final_path = OUTPUT_DIR / f"{ticker}_history.csv"
        final_df.to_csv(final_path, index=False)
        logger.info(f"{progress_str} ✅ {ticker}: Saved {len(final_df)} rows.")
        status = "success"
    else:
        logger.warning(f"{progress_str} ❌ {ticker}: Failed all methods.")
        status = "not_found"
        
    return status

def get_all_downloaded_tickers(base_path):
    downloaded = set()
    if not base_path.exists(): return downloaded
    for file_path in base_path.rglob("*_history.csv"):
        downloaded.add(file_path.name.replace("_history.csv", ""))
    return downloaded

async def main():
    logger.info(f"🚀 STARTING: YF HYBRID SCRAPER (LIB + TABLE)")
    start_time = time.time()
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load Master
    try:
        master_path = list(VAL_YF_MASTER.rglob(f"yf_{ASSET_TYPE}_master.csv"))[-1]
        all_tickers = pd.read_csv(master_path)['ticker'].astype(str).tolist()
    except Exception as e:
        logger.error(f"❌ Master list error: {e}")
        return

    # Smart Resume
    done_tickers = get_all_downloaded_tickers(VAL_YF_HIST) 
    queue = [t for t in all_tickers if t not in done_tickers]
    logger.info(f"⏭️  Skipped: {len(done_tickers)} | ▶️  Remaining: {len(queue)}")

    if not queue: return

    stats = {"success": 0, "error": 0, "not_found": 0}
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

        # 🔥 Concurrency = 2
        semaphore = asyncio.Semaphore(2)

        async def worker(t, idx):
            async with semaphore:
                prog = f"[{idx}/{len(all_tickers)}]"
                res = await process_ticker(context, t, prog)
                
                if res in stats: stats[res] += 1
                await asyncio.sleep(random.uniform(1, 3))

        tasks = [worker(t, i+1+len(done_tickers)) for i, t in enumerate(queue)]
        await asyncio.gather(*tasks)
        await browser.close()

    log_execution_summary(logger, start_time, sum(stats.values()), "Completed", stats)

if __name__ == "__main__":
    asyncio.run(main())