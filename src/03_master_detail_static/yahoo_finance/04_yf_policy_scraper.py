import sys
import os
import asyncio
import pandas as pd
import random
from pathlib import Path
from playwright.async_api import async_playwright

# --- Setup Path ---
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[2]
if str(project_root) not in sys.path: 
    sys.path.append(str(project_root))

from src.utils.logger import setup_logger
from src.utils.db_connector import get_active_tickers


logger = setup_logger("03_master_detail_static_policy")

# ✅ FIX PATH
OUTPUT_DIR = project_root / "validation_output" / "Yahoo_Finance" / "03_Detail_Static"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "yf_fund_policy.csv"


COLS = ["ticker", "div_yield", "pe_ratio", "total_return_ytd", "total_return_1y", "updated_at"]

class YFPolicyScraper:
    def __init__(self):
        logger.info("📡 Fetching Tickers...")
        self.tickers = get_active_tickers("Yahoo Finance")
        
        self.processed = set()
        if OUTPUT_FILE.exists():
            try:
                df = pd.read_csv(OUTPUT_FILE)
                self.processed = set(df['ticker'].astype(str))
            except: pass
        else:
            pd.DataFrame(columns=COLS).to_csv(OUTPUT_FILE, index=False)
            logger.info(f"📁 Created new file: {OUTPUT_FILE}")
        
        self.queue = [t for t in self.tickers if t['ticker'] not in self.processed]
        logger.info(f"✅ Total to Process: {len(self.queue)}")
        self.total_count = len(self.queue)
        self.processed_count = 0

    async def scrape_policy(self, page, ticker):
        self.processed_count += 1
        print(f"[{self.processed_count}/{self.total_count}] ⏳ Policy & Returns: {ticker} ...", end='\r', flush=True)
        
        data = {c: None for c in COLS}
        data.update({"ticker": ticker, "updated_at": pd.Timestamp.now().strftime('%Y-%m-%d')})
        
        try:
            
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)
            
            
            summary_items = await page.locator('div[data-testid="quote-statistics"] li, table tr').all()
            for item in summary_items:
                txt = await item.inner_text()
                if not txt or '\t' not in txt.replace('\n', '\t'): continue
                
                
                parts = txt.replace('\n', '\t').split('\t')
                label, val = parts[0].strip(), parts[-1].strip()
                
                if val == "--": continue
                
                if "Yield" in label: data["div_yield"] = val
                elif "PE Ratio" in label: data["pe_ratio"] = val
                elif "YTD Return" in label: data["total_return_ytd"] = val

            
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/performance", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)
            
            
            
            one_year_row = page.locator('tr:has-text("1-Year"), tr:has-text("1Y")')
            if await one_year_row.count() > 0:
                cells = one_year_row.first.locator('td')
                if await cells.count() >= 2:
                    data["total_return_1y"] = await cells.nth(1).inner_text()

            return data
        except Exception:
            return None

    async def run(self):
        if not self.queue:
            logger.info("🙌 No new tickers to process.")
            return

        print("🚀 Launching Browser...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0...")
            page = await context.new_page()
            
            for item in self.queue:
                res = await self.scrape_policy(page, item['ticker'])
                if res:
                    pd.DataFrame([res])[COLS].to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
                
                
                await asyncio.sleep(random.uniform(1, 3))
            
            await browser.close()
        print("\n🎉 Finished Policy Scraping!")

if __name__ == "__main__":
    if sys.platform == 'win32': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(YFPolicyScraper().run())