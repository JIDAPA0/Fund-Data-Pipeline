import sys
import os
import asyncio
import csv
import pandas as pd
import re
import time
import math
import random
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

# ==========================================
# SYSTEM PATH SETUP
# ==========================================
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[2]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.logger import setup_logger
from src.utils.db_connector import get_active_tickers

# ==========================================
# CONFIGURATION
# ==========================================
logger = setup_logger("01_yf_holdings_master")
CONCURRENCY = 3
BATCH_SIZE = 20

# Base Output Directory
BASE_OUTPUT_DIR = project_root / "validation_output" / "Yahoo_Finance" / "04_Holdings"
DIR_HOLDINGS = BASE_OUTPUT_DIR / "Holdings"
DIR_SECTORS = BASE_OUTPUT_DIR / "Sectors"
DIR_ALLOCATION = BASE_OUTPUT_DIR / "Allocation"

# ✅ ไฟล์รายงานตัวที่เก็บไม่ได้ (Missing Report)
MISSING_REPORT_FILE = BASE_OUTPUT_DIR / "yf_holdings_missing_report.csv"

# Create Directories
for d in [DIR_HOLDINGS, DIR_SECTORS, DIR_ALLOCATION]:
    d.mkdir(parents=True, exist_ok=True)

class YFHoldingsScraper:
    def __init__(self):
        self.start_time = time.time()
        
        logger.info("📡 Fetching Active Tickers from DB...")
        self.tickers = get_active_tickers("Yahoo Finance") 
        logger.info(f"✅ Total Tickers to Process: {len(self.tickers)}")
        
        self.total_processed = 0
        self.total_success = 0
        
        # User Agents
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
        ]

        # สร้างไฟล์ Report ถ้ายั่งไม่มี
        if not MISSING_REPORT_FILE.exists():
            pd.DataFrame(columns=["ticker", "asset_type", "reason", "timestamp"]).to_csv(MISSING_REPORT_FILE, index=False)

    def get_random_ua(self):
        return random.choice(self.user_agents)

    async def log_missing(self, ticker, asset_type, reason):
        """บันทึกตัวที่เก็บไม่ได้ลงไฟล์แยก"""
        try:
            df = pd.DataFrame([{
                "ticker": ticker,
                "asset_type": asset_type,
                "reason": reason,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }])
            df.to_csv(MISSING_REPORT_FILE, mode='a', header=False, index=False)
        except: pass

    async def dismiss_popups(self, page):
        try:
            await page.keyboard.press("Escape")
            selectors = [
                'button[name="reject"]', 'button[name="agree"]', 'button[value="agree"]',
                'button[aria-label="Close"]', 'button.close', 'div.ox-close', 
                '#consent-page button.reject', 'button:has-text("Maybe later")', 
                'button:has-text("Not now")'
            ]
            for sel in selectors:
                if await page.locator(sel).count() > 0:
                    try: await page.locator(sel).first.click(force=True, timeout=500)
                    except: pass
        except: pass

    async def search_fallback(self, page, ticker):
        try:
            search_box = page.locator('#ybar-sbq')
            if await search_box.count() > 0:
                await search_box.fill(ticker)
                await page.keyboard.press("Enter")
                try: await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except: pass
                
                if "/quote/" in page.url and "lookup" not in page.url:
                    match = re.search(r'/quote/([^/?]+)', page.url)
                    if match: return match.group(1)
                    return ticker
        except: pass
        return None

    async def process_ticker(self, context, item):
        ticker = item['ticker']
        raw_asset_type = item.get('asset_type', 'Fund')
        if not raw_asset_type: raw_asset_type = 'Fund'
        asset_type = str(raw_asset_type).upper().replace('/', '').replace(' ', '')
        
        safe_ticker = ticker.replace('/', '_').replace(':', '_')
        
        f_hold = DIR_HOLDINGS / f"{safe_ticker}_{asset_type}_holdings.csv"
        f_sect = DIR_SECTORS / f"{safe_ticker}_{asset_type}_sectors.csv"
        f_alloc = DIR_ALLOCATION / f"{safe_ticker}_{asset_type}_allocation.csv"
        
        if f_hold.exists() or f_sect.exists() or f_alloc.exists():
            return "SKIPPED"

        page = await context.new_page()
        target_ticker = ticker
        url = f"https://finance.yahoo.com/quote/{target_ticker}/holdings/"
        
        data_found = False
        fail_reason = "UNKNOWN"
        
        try:
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            
            # --- 1. HANDLE REDIRECT / SEARCH ---
            if "lookup" in page.url:
                new_ticker = await self.search_fallback(page, ticker)
                if new_ticker:
                    target_ticker = new_ticker
                    await page.goto(f"https://finance.yahoo.com/quote/{target_ticker}/holdings/", timeout=60000)
                else:
                    await page.close()
                    await self.log_missing(ticker, asset_type, "INVALID_TICKER (Search Failed)")
                    return "INVALID_TICKER"

            if "lookup" in page.url:
                await page.close()
                await self.log_missing(ticker, asset_type, "INVALID_TICKER (Still Lookup)")
                return "INVALID_TICKER"

            await asyncio.sleep(2) 
            await self.dismiss_popups(page)
            
            # --- 2. SCRAPE DATA ---
            # Top Holdings
            holdings_data = []
            section = page.locator('section[data-testid="top-holdings"]')
            if await section.count() > 0:
                rows = section.locator('div[class*="content"]')
                cnt = await rows.count()
                for i in range(cnt):
                    txt = await rows.nth(i).inner_text()
                    parts = txt.split('\n')
                    if len(parts) >= 3:
                        holdings_data.append({'symbol': parts[1], 'name': parts[0], 'value': parts[-1]})
                    elif len(parts) == 2:
                        holdings_data.append({'symbol': '-', 'name': parts[0], 'value': parts[1]})

            if not holdings_data:
                tables = page.locator('table')
                cnt_tbl = await tables.count()
                for i in range(cnt_tbl):
                    rows = tables.nth(i).locator('tbody tr')
                    if await rows.count() == 0: continue
                    first_row = await rows.nth(0).inner_text()
                    if "Symbol" in first_row or "% Assets" in first_row:
                        for r in range(await rows.count()):
                            cols = rows.nth(r).locator('td')
                            if await cols.count() >= 3:
                                sym = await cols.nth(0).inner_text()
                                name = await cols.nth(1).inner_text()
                                val = await cols.nth(2).inner_text()
                                holdings_data.append({'symbol': sym, 'name': name, 'value': val})
                        if holdings_data: break

            if holdings_data:
                df = pd.DataFrame(holdings_data)
                df['ticker'] = ticker
                df['yahoo_ticker'] = target_ticker
                df['asset_type'] = asset_type
                df['updated_at'] = datetime.now().strftime('%Y-%m-%d')
                df.to_csv(f_hold, index=False)
                data_found = True

            # Sector Weightings
            sector_data = []
            sec_section = page.locator('section[data-testid*="sector-weightings"]')
            if await sec_section.count() > 0:
                rows = sec_section.locator('div[class*="content"]')
                cnt = await rows.count()
                for i in range(cnt):
                    txt = await rows.nth(i).inner_text()
                    parts = txt.split('\n')
                    if len(parts) >= 2:
                        sector_data.append({'sector': parts[0], 'value': parts[-1]})
            
            if sector_data:
                df = pd.DataFrame(sector_data)
                df['ticker'] = ticker
                df['asset_type'] = asset_type
                df['updated_at'] = datetime.now().strftime('%Y-%m-%d')
                df.to_csv(f_sect, index=False)
                data_found = True

            # Asset Allocation
            alloc_data = []
            tables = page.locator('table')
            cnt_tbl = await tables.count()
            for i in range(cnt_tbl):
                rows = tables.nth(i).locator('tbody tr')
                if await rows.count() == 0: continue
                first_cell = await rows.nth(0).locator('td').first.inner_text()
                if any(k in first_cell for k in ['Cash', 'Stocks', 'Bonds']):
                    for r in range(await rows.count()):
                        cols = rows.nth(r).locator('td')
                        if await cols.count() >= 2:
                            cat = await cols.nth(0).inner_text()
                            val = await cols.nth(1).inner_text()
                            alloc_data.append({'category': cat, 'value': val})
                    if alloc_data: break

            if alloc_data:
                df = pd.DataFrame(alloc_data)
                df['ticker'] = ticker
                df['asset_type'] = asset_type
                df['updated_at'] = datetime.now().strftime('%Y-%m-%d')
                df.to_csv(f_alloc, index=False)
                data_found = True

            # 🚨 ถ้าหาไม่เจอสักอย่างเดียวเลย ให้บันทึกว่า NO_DATA
            if not data_found:
                fail_reason = "NO_HOLDINGS_DATA (Page loaded but empty)"
                await self.log_missing(ticker, asset_type, fail_reason)

        except Exception as e:
            fail_reason = f"ERROR: {str(e)[:50]}"
            await self.log_missing(ticker, asset_type, fail_reason)
        finally:
            await page.close()
        
        return "SUCCESS" if data_found else "NO_DATA"

    async def run(self):
        if not self.tickers: return
        logger.info(f"🚀 Starting Yahoo Holdings Scraper (With Missing Report)")
        
        total = len(self.tickers)
        batches = math.ceil(total / BATCH_SIZE)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent=self.get_random_ua()
            )
            
            for i in range(batches):
                start_b = time.time()
                batch = self.tickers[i*BATCH_SIZE : (i+1)*BATCH_SIZE]
                
                tasks = [self.process_ticker(context, t) for t in batch]
                results = await asyncio.gather(*tasks)
                
                success_cnt = results.count("SUCCESS")
                skip_cnt = results.count("SKIPPED")
                self.total_success += success_cnt
                self.total_processed += len(batch)
                
                dur = time.time() - start_b
                logger.info(f"Batch {i+1}/{batches} | Saved: {success_cnt} | Skips: {skip_cnt} | Progress: {self.total_processed}/{total} | Time: {dur:.2f}s")
                
                if (i+1) % 10 == 0:
                    await context.close()
                    context = await browser.new_context(
                        viewport={'width': 1280, 'height': 800},
                        user_agent=self.get_random_ua()
                    )

            await browser.close()
        
        logger.info(f"🎉 Finished! Total Saved: {self.total_success} tickers")
        logger.info(f"📄 Check missing tickers at: {MISSING_REPORT_FILE}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(YFHoldingsScraper().run())