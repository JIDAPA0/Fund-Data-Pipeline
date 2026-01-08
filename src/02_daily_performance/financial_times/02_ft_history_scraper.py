import sys
import os
import asyncio
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from playwright.async_api import async_playwright

# ==========================================
# 1. CONFIGURATION
# ==========================================
CONCURRENT_TABS = 12
TODAY_STR = datetime.now().strftime("%Y-%m-%d")

BASE_OUTPUT_DIR = Path("validation_output/Financial_Times/02_Price_And_Dividend_History") / TODAY_STR / "02_daily_performance"
ETF_DIR = BASE_OUTPUT_DIR / "ETF"
FUND_DIR = BASE_OUTPUT_DIR / "FUND"

ETF_DIR.mkdir(parents=True, exist_ok=True)
FUND_DIR.mkdir(parents=True, exist_ok=True)

DEBUG_DIR = BASE_OUTPUT_DIR / "debug_evidence"
DEBUG_NOT_FOUND = DEBUG_DIR / "id_not_found"
DEBUG_NO_DATA = DEBUG_DIR / "no_data"

DEBUG_NOT_FOUND.mkdir(parents=True, exist_ok=True)
DEBUG_NO_DATA.mkdir(parents=True, exist_ok=True)

LOG_NO_DATA = BASE_OUTPUT_DIR / "report_no_data.csv"
LOG_ID_NOT_FOUND = BASE_OUTPUT_DIR / "report_id_not_found.csv"

for log_file in [LOG_NO_DATA, LOG_ID_NOT_FOUND]:
    if not log_file.exists():
        with open(log_file, "w", encoding='utf-8') as f:
            f.write("ticker,asset_type,timestamp,note\n")

import logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s | Tab-%(threadName)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("FT_Balanced_Headless")

try:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
    from src.utils.db_connector import get_active_tickers 
except Exception:
    def get_active_tickers(source_name="ft"):
        return []

def append_csv_log(file_path, ticker, asset_type, note=""):
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(file_path, "a", encoding='utf-8') as f:
            f.write(f"{ticker},{asset_type},{timestamp},{note}\n")
    except: pass

async def capture_evidence(page, ticker, folder_path):
    try:
        timestamp = datetime.now().strftime('%H%M%S')
        safe_ticker = str(ticker).replace(":", "_").replace("/", "_")
        img_path = folder_path / f"{safe_ticker}_{timestamp}.png"
        await page.screenshot(path=img_path, full_page=True)
    except Exception as e:
        pass

# ==========================================
# 2. LOGIC CLASS
# ==========================================
class FT_Logic:
    def __init__(self):
        self.api_session = requests.Session()
        self.api_session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

    async def inject_consent_cookies(self, context):
        try:
            cookies = [
                {'name': 'FTConsent', 'value': 'behavioral', 'domain': '.ft.com', 'path': '/'},
                {'name': 'cookieConsent', 'value': 'accepted', 'domain': '.ft.com', 'path': '/'},
                {'name': 'notice_preferences', 'value': '2:', 'domain': '.ft.com', 'path': '/'},
                {'name': 'notice_behavior', 'value': 'expressed,eu', 'domain': '.ft.com', 'path': '/'},
                {'name': 'consentUUID', 'value': 'c00k1e-sp00f-u-u-1-d', 'domain': '.ft.com', 'path': '/'}
            ]
            await context.add_cookies(cookies)
        except: pass

    async def destroy_cookies_dom(self, page):
        try:
            await page.evaluate("""
                () => {
                    const selectors = [
                        '.o-cookie-message',
                        '[id^="sp_message_container"]',
                        '.o-app-layout__overlay',
                        'iframe[title*="Consent"]',
                        'div[class*="cookie"]'
                    ];
                    document.querySelectorAll(selectors.join(',')).forEach(el => el.remove());
                    document.body.style.overflow = 'auto';
                    document.body.style.position = 'static';
                }
            """)
        except: pass

    async def verify_and_get_rid(self, page, ticker, asset_type):
        ticker_clean = ticker.strip()
        
        if 'etf' in str(asset_type).lower():
            categories = ['etfs', 'funds']
        else:
            categories = ['funds', 'etfs']
        
        rid = None
        for cat in categories:
            rid = await self._try_get_id(page, ticker_clean, cat)
            if rid: break 
            
        return rid

    async def _try_get_id(self, page, ticker, category):
        found_id = None
        
        await page.route("**/*", lambda route: route.abort() 
            if route.request.resource_type in ["image", "media", "font"] 
            else route.continue_())

        async def handle_request(request):
            nonlocal found_id
            if "get-historical-prices" in request.url:
                match = re.search(r'symbol=([0-9]+)', request.url)
                if match: found_id = match.group(1)

        page.on("request", handle_request)

        url = f"https://markets.ft.com/data/{category}/tearsheet/historical?s={ticker}"
        
        try:
            try:
                await page.goto(url, timeout=35000, wait_until="domcontentloaded")
            except:
                return None
            
            if "search" in page.url: return None

            await self.destroy_cookies_dom(page)

            filter_btn = page.locator('.mod-icon--filter').first
            try:
                await filter_btn.wait_for(state="visible", timeout=6000)
                await filter_btn.click(force=True)
            except:
                await self.destroy_cookies_dom(page)
                try: await filter_btn.click(force=True)
                except: return None 

            await self.destroy_cookies_dom(page)

            to_input = page.locator('input.picker__input[placeholder="To"]').first
            if await to_input.is_visible():
                await to_input.click(force=True)
                
                valid_days = page.locator('.picker__day--infocus:not(.picker__day--disabled)[role="gridcell"]')
                
                if await valid_days.count() == 0:
                    await self.destroy_cookies_dom(page)
                    await to_input.click(force=True)
                    await asyncio.sleep(0.5)

                if await valid_days.count() > 0:
                    await valid_days.nth(await valid_days.count() - 1).click(force=True)
                    for _ in range(25): 
                        if found_id: break
                        await asyncio.sleep(0.4)
        except: pass
            
        return found_id

    def fetch_api(self, ticker, symbol_id, asset_type):
        all_dfs = []
        end_date = datetime.now()
        empty_streak = 0
        
        while True:
            start_date = end_date - timedelta(days=365)
            params = {
                "startDate": start_date.strftime('%Y/%m/%d'),
                "endDate": end_date.strftime('%Y/%m/%d'),
                "symbol": symbol_id
            }
            try:
                resp = self.api_session.get("https://markets.ft.com/data/equities/ajax/get-historical-prices", params=params, timeout=25)
                if resp.status_code == 200:
                    json_data = resp.json()
                    if "html" in json_data and json_data["html"].strip():
                        df = pd.read_html(StringIO(f"<table>{json_data['html']}</table>"), header=None)[0]
                        if not df.empty: 
                            all_dfs.append(df)
                            empty_streak = 0
                        else: empty_streak += 1
                    else: empty_streak += 1
                else: empty_streak += 1
            except: empty_streak += 1
            
            if empty_streak >= 2 or start_date.year < 1980: break
            end_date = start_date - timedelta(days=1)
        
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            if 0 in final_df.columns:
                final_df[0] = final_df[0].astype(str).str.extract(r'([A-Za-z]+, [A-Za-z]+ \d{1,2}, \d{4})')[0]
            if len(final_df.columns) >= 6:
                final_df = final_df.iloc[:, :6]
                final_df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
            final_df["Date"] = pd.to_datetime(final_df["Date"], errors='coerce').dt.strftime('%Y-%m-%d')
            final_df = final_df.dropna(subset=['Date']).drop_duplicates(subset=['Date']).sort_values("Date", ascending=False)
            
            safe_ticker = str(ticker).replace(":", "_").replace("/", "_")
            filename = f"{safe_ticker}_history.csv"
            
            save_path = (ETF_DIR if 'etf' in str(asset_type).lower() else FUND_DIR) / filename
            final_df.to_csv(save_path, index=False)
            return len(final_df)
        return 0

# ==========================================
# 3. WORKER
# ==========================================
async def tab_worker(worker_id, context, queue, logic, total_count):
    logger.info(f"🔧 Worker {worker_id} started")
    await logic.inject_consent_cookies(context)

    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        ticker = item['ticker']
        asset_type = item.get('asset_type', 'fund')
        index = item['index']
        progress = f"[{index}/{total_count}]"

        safe_ticker = str(ticker).replace(":", "_").replace("/", "_")
        filename = f"{safe_ticker}_history.csv"
        
        if (ETF_DIR / filename).exists() or (FUND_DIR / filename).exists():
            queue.task_done()
            continue

        page = await context.new_page()
        try:
            rid = await logic.verify_and_get_rid(page, ticker, asset_type)
            
            if not rid:
                rid = await logic.verify_and_get_rid(page, ticker, asset_type)

            if rid:
                count = logic.fetch_api(ticker, rid, asset_type)
                if count > 0:
                    logger.info(f"Tab-{worker_id} | {progress} ✅ {ticker}: {count} rows")
                else:
                    logger.warning(f"Tab-{worker_id} | {progress} ⚠️ {ticker}: No Data (Saved Evidence)")
                    await capture_evidence(page, ticker, DEBUG_NO_DATA)
                    append_csv_log(LOG_NO_DATA, ticker, asset_type, "Empty AJAX")
            else:
                logger.error(f"Tab-{worker_id} | {progress} ❌ {ticker}: Not Found (Saved Evidence)")
                await capture_evidence(page, ticker, DEBUG_NOT_FOUND)
                append_csv_log(LOG_ID_NOT_FOUND, ticker, asset_type, "Scan Failed")
        
        except Exception as e:
            logger.error(f"Tab-{worker_id} | {progress} 💥 Error: {e}")
        finally:
            await page.close()
            queue.task_done()

# ==========================================
# 4. MAIN
# ==========================================
async def main():
    logger.info("📡 Connecting to DB...")
    all_tickers = get_active_tickers(source_name="ft")
    if not all_tickers:
        print("⚠️ No data from DB")
        return

    skipped_tickers = set()
    for log_file in [LOG_NO_DATA, LOG_ID_NOT_FOUND]:
        if log_file.exists() and os.path.getsize(log_file) > 0:
            try:
                df = pd.read_csv(log_file)
                if 'ticker' in df.columns:
                    skipped_tickers.update(df['ticker'].astype(str).tolist())
            except: pass
    
    logger.info(f"🚫 Loaded {len(skipped_tickers)} skipped tickers from logs")

    work_items = []
    idx = 0
    total_db = len(all_tickers)
    
    count_etf_done = 0
    count_fund_done = 0
    
    for t in all_tickers:
        t_str = str(t['ticker'])
        safe_t = t_str.replace(":", "_").replace("/", "_")
        filename = f"{safe_t}_history.csv"
        
        if (ETF_DIR / filename).exists():
            count_etf_done += 1
            continue
        elif (FUND_DIR / filename).exists():
            count_fund_done += 1
            continue
            
        if t_str in skipped_tickers: continue
        
        idx += 1
        t['index'] = idx
        work_items.append(t)
    
    total_done = count_etf_done + count_fund_done
    total_to_do = len(work_items)
    
    print("-" * 50)
    print(f"⚖️ BALANCED MODE (12 Tabs + Retry)")
    print("-" * 50)
    print(f"✅ ETF Collected  : {count_etf_done:,}")
    print(f"✅ FUND Collected : {count_fund_done:,}")
    print(f"✨ Total Done     : {total_done:,}")
    print("-" * 50)
    print(f"📦 Remaining      : {total_to_do:,}")
    print("-" * 50)

    if total_to_do == 0: 
        logger.info("🎉 All tasks completed!")
        return

    queue = asyncio.Queue()
    for item in work_items: queue.put_nowait(item)
    for _ in range(CONCURRENT_TABS): queue.put_nowait(None)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,  
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
        )
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        logic = FT_Logic()

        tasks = []
        for i in range(CONCURRENT_TABS):
            task = asyncio.create_task(tab_worker(i+1, context, queue, logic, total_to_do))
            tasks.append(task)

        await asyncio.gather(*tasks)
        logger.info("🎉 Done.")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
