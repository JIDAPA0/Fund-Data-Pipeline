import asyncio
import sys
import os
import csv
import time
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from src.utils.path_manager import VAL_YF_HIST, VAL_YF_MASTER
from src.utils.browser_utils import get_launch_args, get_context_options

ASSET_TYPE = 'etf'
OUTPUT_DIR = VAL_YF_HIST / "Price_History" / datetime.now().strftime('%Y-%m-%d') / ASSET_TYPE

async def process_ticker(context, ticker):
    page = await context.new_page()
    try:
        t_stamp = int(time.time())
        url = f"https://finance.yahoo.com/quote/{ticker}/history?period1=0&period2={t_stamp}"
        
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        
        for _ in range(3):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)

        rows = page.locator('table[data-test="historical-prices"] tbody tr')
        data = []
        if await rows.count() > 0:
            texts = await rows.all_inner_texts()
            for text in texts:
                cols = text.split('\t')
                if len(cols) > 4: data.append(cols)
        
        if data:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            with open(OUTPUT_DIR / f"{ticker}_history.csv", 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(data)
            print(f"✅ {ticker}: {len(data)} rows")
        else:
            print(f"⚠️ {ticker}: No data")
    except Exception as e:
        print(f"❌ {ticker}: {e}")
    finally:
        await page.close()

async def main():
    print(f"🚀 YF {ASSET_TYPE.upper()} HIST")
    try:
        master = list(VAL_YF_MASTER.rglob(f"yf_{ASSET_TYPE}_master.csv"))[-1]
        tickers = pd.read_csv(master)['ticker'].tolist()
    except: return

    async with async_playwright() as p:
        browser = await p.chromium.launch(**get_launch_args(headless=True))
        context = await browser.new_context(**get_context_options())
        await context.route("**/*.{png,jpg,svg}", lambda r: r.abort())

        for i in range(0, len(tickers), 5):
            await asyncio.gather(*[process_ticker(context, t) for t in tickers[i:i+5]])
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())