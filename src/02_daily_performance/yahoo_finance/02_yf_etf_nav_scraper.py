import sys
import os
import pandas as pd
from datetime import datetime
import time
import yfinance as yf
import warnings
import requests
import random
from bs4 import BeautifulSoup

# ==========================================
# 0. SETUP
# ==========================================
warnings.simplefilter(action='ignore', category=FutureWarning)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(BASE_DIR)

try:
    from src.utils.path_manager import DATA_PERFORMANCE_DIR
    from src.utils.logger import setup_logger
    from src.utils.db_connector import get_active_tickers, insert_dataframe
    logger = setup_logger("02_perf_yf_etf_nav")
except ImportError:
    sys.exit(1)

# ==========================================
# 1. CONFIGURATION (TUNED FOR 3 HOURS LIMIT) ⚡
# ==========================================
ASSET_TYPE = 'ETF'
SOURCE_NAME = 'Yahoo Finance'

BATCH_SIZE = 40       # ปรับเท่า Fund
NORMAL_DELAY = 2      # ปรับเท่า Fund
COOL_DOWN_DELAY = 120 # ปรับเท่า Fund

current_date = datetime.now().strftime('%Y-%m-%d')
OUTPUT_DIR = DATA_PERFORMANCE_DIR / "yahoo_finance" / current_date
OUTPUT_FILE = OUTPUT_DIR / f"yf_nav_{ASSET_TYPE.lower()}.csv"
ERROR_FILE = OUTPUT_DIR / f"yf_errors_{ASSET_TYPE.lower()}.csv"

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def get_custom_session():
    session = requests.Session()
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    ]
    session.headers.update({
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
    })
    return session

def get_processed_tickers():
    if not OUTPUT_FILE.exists(): return set()
    try:
        df = pd.read_csv(OUTPUT_FILE, usecols=['ticker'])
        return set(df['ticker'].astype(str).str.strip().tolist())
    except: return set()

# ==========================================
# 3. CORE LOGIC
# ==========================================

def fetch_via_web_scraping(ticker):
    """Level 3: Web Scraping"""
    url = f"https://finance.yahoo.com/quote/{ticker}"
    try:
        session = get_custom_session()
        response = session.get(url, timeout=10)
        
        if response.status_code != 200: return None
            
        soup = BeautifulSoup(response.text, 'lxml')
        
        price_tag = soup.find('fin-streamer', {'data-field': 'regularMarketPrice'})
        if not price_tag:
            price_tag = soup.find('fin-streamer', {'data-field': 'regularMarketOpen'})

        if price_tag and price_tag.text:
            raw_price = price_tag.text.replace(',', '').strip()
            try:
                nav_price = float(raw_price)
            except ValueError:
                return None
            
            return {
                'ticker': ticker,
                'asset_type': ASSET_TYPE,
                'source': SOURCE_NAME,
                'nav_price': nav_price,
                'currency': 'USD', 
                'as_of_date': current_date,
                'scrape_date': current_date
            }
    except Exception: return None
    return None

def fetch_single_ticker_retry(ticker):
    """Level 2: API Retry"""
    try:
        time.sleep(random.uniform(0.5, 1.0))
        dat = yf.Ticker(ticker, session=get_custom_session())
        hist = dat.history(period="5d")
        
        if not hist.empty:
            last_valid = hist['Close'].dropna().tail(1)
            if not last_valid.empty:
                return {
                    'ticker': ticker,
                    'asset_type': ASSET_TYPE,
                    'source': SOURCE_NAME,
                    'nav_price': float(last_valid.iloc[0]),
                    'currency': dat.fast_info.get('currency', 'USD'),
                    'as_of_date': last_valid.index[0].strftime('%Y-%m-%d'),
                    'scrape_date': current_date
                }
    except: pass
    return fetch_via_web_scraping(ticker)

def fetch_batch_data(tickers):
    results = []
    failed_candidates = []
    need_cool_down = False
    
    # --- Level 1: Batch Download ---
    try:
        tickers_str = " ".join(tickers)
        data = yf.download(tickers_str, period="1mo", group_by='ticker', threads=True, progress=False)
        
        for ticker in tickers:
            try:
                if len(tickers) == 1: df = data
                else: df = data[ticker] if ticker in data else pd.DataFrame()
                
                valid = False
                if not df.empty and 'Close' in df.columns:
                    last_valid = df['Close'].dropna().tail(1)
                    if not last_valid.empty:
                        results.append({
                            'ticker': ticker,
                            'asset_type': ASSET_TYPE,
                            'source': SOURCE_NAME,
                            'nav_price': float(last_valid.iloc[0]),
                            'currency': 'USD', 
                            'as_of_date': last_valid.index[0].strftime('%Y-%m-%d'),
                            'scrape_date': current_date
                        })
                        valid = True
                if not valid: failed_candidates.append(ticker)
            except: failed_candidates.append(ticker)      
    except: failed_candidates = tickers

    # --- Level 2 & 3: Retry ---
    real_fails = []
    if failed_candidates:
        if len(failed_candidates) > (len(tickers) * 0.5):
            need_cool_down = True 

        for t in failed_candidates:
            res = fetch_single_ticker_retry(t)
            if res: results.append(res)
            else: real_fails.append(t)
                
    return results, real_fails, need_cool_down

# ==========================================
# 4. MAIN EXECUTION
# ==========================================
def main():
    start_time = time.time()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"📡 Fetching Active {ASSET_TYPE} Tickers from DB...")
    all_yf_tickers = get_active_tickers(SOURCE_NAME)
    target_tickers = [t['ticker'] for t in all_yf_tickers if t['asset_type'].upper() == ASSET_TYPE]
    
    if not target_tickers:
        logger.warning(f"🚫 No active {ASSET_TYPE} tickers found.")
        return

    processed_tickers = get_processed_tickers()
    todos = [t for t in target_tickers if t not in processed_tickers]
    
    logger.info(f"📊 Summary: Found {len(target_tickers)} total.")
    logger.info(f"⏭️ Skipped: {len(processed_tickers)} (Already done).")
    logger.info(f"🚀 Remaining to run: {len(todos)} items.")

    if not todos:
        logger.info("✅ All tasks completed for today!")
        return

    success_count = 0
    fail_count = 0
    
    for i in range(0, len(todos), BATCH_SIZE):
        batch = todos[i:i + BATCH_SIZE]
        results, fails, need_cool_down = fetch_batch_data(batch)
        
        if results:
            df = pd.DataFrame(results)
            try: insert_dataframe(df, "stg_daily_nav")
            except: pass
            
            use_header = not OUTPUT_FILE.exists()
            df.to_csv(OUTPUT_FILE, mode='a', header=use_header, index=False)
            success_count += len(results)
            
        if fails:
            df_err = pd.DataFrame({'ticker': fails, 'reason': 'Failed L3 Scraping', 'scraped_at': datetime.now()})
            use_header_err = not ERROR_FILE.exists()
            df_err.to_csv(ERROR_FILE, mode='a', header=use_header_err, index=False)
            fail_count += len(fails)
        
        current_batch = i//BATCH_SIZE + 1
        total_batches = (len(todos) // BATCH_SIZE) + 1
        logger.info(f"Batch {current_batch}/{total_batches} | ✅ OK: {len(results)} | ❌ Dead: {len(fails)} | Total Success: {success_count}")
        
        if need_cool_down:
            logger.warning(f"❄️ Cool Down Mode: Sleeping {COOL_DOWN_DELAY}s...")
            time.sleep(COOL_DOWN_DELAY)
        else:
            time.sleep(NORMAL_DELAY)

    total_duration = time.time() - start_time
    logger.info("="*50)
    logger.info(f"🏁 Finished. Total Time: {total_duration/60:.2f} min")
    logger.info(f"✅ New Success: {success_count}")
    logger.info(f"❌ New Failed: {fail_count}")
    logger.info("="*50)

if __name__ == "__main__":
    main()