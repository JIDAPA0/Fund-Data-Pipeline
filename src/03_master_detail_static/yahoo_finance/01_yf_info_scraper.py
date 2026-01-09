import asyncio
import os
import sys
import pandas as pd
import random
import re
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

# ==========================================
# SYSTEM PATH SETUP
# ==========================================
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[2]
if str(project_root) not in sys.path: sys.path.append(str(project_root))

from src.utils.logger import setup_logger
from src.utils.db_connector import get_active_tickers

# ==========================================
# CONFIGURATION
# ==========================================
logger = setup_logger("01_yf_info_scraper")
OUTPUT_DIR = project_root / "validation_output" / "Yahoo_Finance" / "03_Detail_Static"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "yf_fund_info.csv"

# เอา inception_date กลับมา (ถ้าหาเจอเก็บ ถ้าไม่เจอปล่อยว่าง)
COLS = ["ticker", "name", "exchange", "issuer", "category", "inception_date", "source", "updated_at"]

class YFIdentityScraper:
    def __init__(self):
        self.tickers_data = get_active_tickers("Yahoo Finance")
        
        # Resume Logic
        self.processed_tickers = set()
        if OUTPUT_FILE.exists():
            try:
                df = pd.read_csv(OUTPUT_FILE)
                if 'ticker' in df.columns:
                    self.processed_tickers = set(df['ticker'].astype(str))
                logger.info(f"⏭️ Found existing file. Skipping {len(self.processed_tickers)} rows.")
            except: pass

    def _parse_date(self, text):
        if not text: return None
        try:
            clean = text.strip()
            # รองรับ Format: Sep 07, 2010
            dt = datetime.strptime(clean, "%b %d, %Y")
            return dt.strftime("%Y-%m-%d")
        except: return None

    async def scrape_ticker(self, context, ticker_info):
        ticker = ticker_info['ticker']
        yf_ticker = ticker.split(':')[0] 
        
        url = f"https://finance.yahoo.com/quote/{yf_ticker}/profile"
        
        data = {c: None for c in COLS}
        data.update({
            "ticker": ticker, 
            "source": "Yahoo Finance", 
            "updated_at": datetime.now().strftime("%Y-%m-%d")
        })
        
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Check Redirect
            title = await page.title()
            if "Stock Market Live" in title or "Yahoo Finance" == title.strip():
                logger.warning(f"⚠️ {ticker} redirected to Home (Skipping)")
                return None

            # 1. NAME
            if title:
                clean_name = title.split(" Company Profile")[0].split(" Stock Price")[0].split(" - Yahoo")[0]
                clean_name = re.sub(r'\([A-Za-z0-9\.:]+\)$', '', clean_name).strip()
                data['name'] = clean_name

            # ดึง Text ทั้งหน้า
            full_text = await page.inner_text("body")

            # 2. CATEGORY
            match_cat = re.search(r'Category[\s\n]+([A-Za-z\s]+)', full_text)
            if match_cat:
                val = match_cat.group(1).split('\n')[0].strip()
                if len(val) < 50: data['category'] = val

            # 3. ISSUER
            match_issuer = re.search(r'(?:Fund Family|Issuer|Manager)[\s\n]+([A-Za-z\s&]+)', full_text)
            if match_issuer:
                val = match_issuer.group(1).split('\n')[0].strip()
                if len(val) < 50: data['issuer'] = val

            # 4. EXCHANGE
            if "NYSE" in full_text: data['exchange'] = "NYSE"
            elif "Nasdaq" in full_text: data['exchange'] = "Nasdaq"
            elif "CBOE" in full_text: data['exchange'] = "CBOE"

            # 5. INCEPTION DATE (Best Effort)
            # พยายามหา ถ้าเจอคือโชคดี ถ้าไม่เจอ (ETF) ก็ช่างมัน เป็น None ไป
            match_date = re.search(r'(?:Inception|Commencement) Date[\s\S]{0,50}?([A-Z][a-z]{2,9}\s+\d{1,2},\s+\d{4})', full_text, re.IGNORECASE)
            if match_date:
                data['inception_date'] = self._parse_date(match_date.group(1))
            
            # Log ให้ดูหน่อยว่าเจอวันไหม
            date_status = data['inception_date'] if data['inception_date'] else "None"
            logger.info(f"✅ {ticker} extracted: {data['name']} (Date: {date_status})")
            return data

        except Exception as e:
            logger.error(f"❌ {ticker} error: {str(e)}")
            return None
        finally:
            await page.close()

    async def run(self):
        queue = [t for t in self.tickers_data if t['ticker'] not in self.processed_tickers]
        logger.info(f"🚀 Found {len(self.tickers_data)} total. Remaining to scrape: {len(queue)}")
        
        if not queue:
            logger.info("🎉 All done!")
            return

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            
            for i, item in enumerate(queue, 1):
                res = await self.scrape_ticker(context, item)
                
                if res:
                    df = pd.DataFrame([res])[COLS]
                    use_header = not OUTPUT_FILE.exists()
                    df.to_csv(OUTPUT_FILE, mode='a', header=use_header, index=False)
                
                await asyncio.sleep(random.uniform(3, 6))
                
                if i % 20 == 0:
                    await context.close()
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
                    )

            await browser.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(YFIdentityScraper().run())