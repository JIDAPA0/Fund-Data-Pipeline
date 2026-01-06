import sys
import os
import asyncio
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

logger = setup_logger("03_master_detail_static_fees")

OUTPUT_DIR = project_root / "validation_output" / "Yahoo_Finance" / "03_Detail_Static"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "yf_fund_fees.csv"

COLS = ["ticker", "expense_ratio", "initial_charge", "exit_charge", "assets_aum", "top_10_hold_pct", "holdings_count", "holdings_turnover"]

class YFFeesScraper:
    def __init__(self):
        self.tickers = get_active_tickers("Yahoo Finance")
        if not OUTPUT_FILE.exists():
            pd.DataFrame(columns=COLS).to_csv(OUTPUT_FILE, index=False)

    async def scrape_data(self, page, ticker):
        data = {c: None for c in COLS}; data["ticker"] = ticker
        try:
            # 1. Profile Page
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/profile", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)
            rows = await page.locator("table tr").all()
            for row in rows:
                txt = await row.inner_text()
                if "\t" in txt:
                    parts = txt.split("\t")
                    label, val = parts[0].strip(), parts[1].strip()
                    if val == "--": continue
                    if "Net Assets" in label: data["assets_aum"] = val
                    elif "Expense Ratio" in label: data["expense_ratio"] = val
                    elif "Front-End" in label or "Front End" in label: data["initial_charge"] = val
                    elif "Deferred" in label: data["exit_charge"] = val
                    elif "Turnover" in label: data["holdings_turnover"] = val

            # 2. Holdings Page
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/holdings", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)
            try:
                header = await page.locator('section[data-testid="top-holdings"] h3').inner_text()
                if "(" in header: data["top_10_hold_pct"] = header.split('(')[1].split('%')[0] + "%"
            except: pass
            
            h_rows = await page.locator("table tr").all()
            for r in h_rows:
                t = await r.inner_text()
                if "\t" in t:
                    p = t.split("\t")
                    if "Total Holdings" in p[0]: data["holdings_count"] = p[1]

            return data
        except: return None

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0...")
            page = await context.new_page()
            processed = set(pd.read_csv(OUTPUT_FILE)['ticker'].astype(str)) if OUTPUT_FILE.exists() else set()
            queue = [t for t in self.tickers if t['ticker'] not in processed]

            for i, item in enumerate(queue, 1):
                print(f"[{i}/{len(queue)}] ⏳ {item['ticker']} ...", end='\r')
                res = await self.scrape_data(page, item['ticker'])
                if res:
                    pd.DataFrame([res])[COLS].to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
                if i % 10 == 0: await asyncio.sleep(random.uniform(2, 4))
            await browser.close()

if __name__ == "__main__":
    asyncio.run(YFFeesScraper().run())