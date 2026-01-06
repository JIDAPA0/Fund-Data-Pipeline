import asyncio
import os
import sys
import pandas as pd
import random
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

# --- Setup Path ---
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[2]
if str(project_root) not in sys.path: sys.path.append(str(project_root))

from src.utils.logger import setup_logger
from src.utils.db_connector import get_active_tickers

# ⚙️ CONFIG
logger = setup_logger("yf_identity_scraper")
OUTPUT_DIR = project_root / "validation_output" / "Yahoo_Finance" / "03_Detail_Static"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "yf_fund_info.csv"

COLS = ["ticker", "name", "exchange", "issuer", "category", "inception_date", "source", "updated_at"]

class YFIdentityScraper:
    def __init__(self):
        self.tickers_data = get_active_tickers("Yahoo Finance")
        if not OUTPUT_FILE.exists():
            pd.DataFrame(columns=COLS).to_csv(OUTPUT_FILE, index=False)

    async def scrape_ticker(self, context, ticker_info):
        ticker = ticker_info['ticker']
        db_name = ticker_info.get('name', 'N/A')
        page = await context.new_page()
        url = f"https://finance.yahoo.com/quote/{ticker}/profile/"
        
        data = {c: None for c in COLS}
        data.update({"ticker": ticker, "name": db_name, "source": "Yahoo Finance", "updated_at": datetime.now().strftime("%Y-%m-%d")})
        
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(4)

            
            exchange_loc = page.locator('span[class*="exchange"]')
            if await exchange_loc.count() > 0:
                raw_ex = await exchange_loc.first.inner_text()
                data["exchange"] = raw_ex.split(" - ")[0].strip() if raw_ex else None

            
            rows = await page.locator("table tr").all()
            for row in rows:
                text = await row.inner_text()
                if "\t" in text:
                    parts = text.split("\t")
                    label, value = parts[0].strip(), parts[1].strip()
                    if value == "--": continue
                    if "Category" == label: data["category"] = value
                    elif "Fund Family" in label or "Issuer" in label: data["issuer"] = value
                    elif "Inception Date" in label: data["inception_date"] = value

            logger.info(f"✅ {ticker} extracted")
            return data
        except Exception as e:
            logger.error(f"❌ {ticker} error: {str(e)}")
            return None
        finally:
            await page.close()

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            processed = set(pd.read_csv(OUTPUT_FILE)['ticker'].astype(str)) if OUTPUT_FILE.exists() else set()
            queue = [t for t in self.tickers_data if t['ticker'] not in processed]

            for i, item in enumerate(queue, 1):
                res = await self.scrape_ticker(context, item)
                if res:
                    pd.DataFrame([res])[COLS].to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
                if i % 10 == 0: await asyncio.sleep(random.uniform(2, 5))
            await browser.close()

if __name__ == "__main__":
    asyncio.run(YFIdentityScraper().run())