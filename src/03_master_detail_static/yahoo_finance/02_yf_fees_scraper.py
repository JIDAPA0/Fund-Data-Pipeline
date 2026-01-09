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
logger = setup_logger("02_yf_fees_scraper")
OUTPUT_DIR = project_root / "validation_output" / "Yahoo_Finance" / "03_Detail_Static"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "yf_fund_fees.csv"

COLS = [
    "ticker", 
    "expense_ratio", 
    "initial_charge", 
    "exit_charge", 
    "assets_aum", 
    "top_10_hold_pct", 
    "holdings_count", 
    "holdings_turnover", 
    "updated_at"
]

class YFFeesScraper:
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

    async def scrape_ticker(self, context, ticker_info):
        ticker = ticker_info['ticker']
        yf_ticker = ticker.split(':')[0]
        
        data = {c: None for c in COLS}
        data['ticker'] = ticker
        data['updated_at'] = datetime.now().strftime("%Y-%m-%d")

        page = await context.new_page()
        try:
            # ==================================================
            # 1. SUMMARY PAGE (Priority for Expense Ratio & AUM)
            # ==================================================
            # ETF ส่วนใหญ่ และ Mutual Fund บางตัว ข้อมูล Fees อยู่หน้านี้
            url_summary = f"https://finance.yahoo.com/quote/{yf_ticker}"
            try:
                await page.goto(url_summary, wait_until="domcontentloaded", timeout=45000)
                await page.evaluate("window.scrollBy(0, 500)")
                # ไม่ต้อง sleep นานตรงนี้ เอาแค่พอให้ JS รัน
                
                full_text_sum = await page.inner_text("body")

                # Expense Ratio (Regex Broad)
                match_exp = re.search(r'(?:Net Expense Ratio|Expense Ratio \(net\)|Expense Ratio)[\s\n]*(\d+\.\d+%)', full_text_sum, re.IGNORECASE)
                if match_exp: data['expense_ratio'] = match_exp.group(1)
                
                # Net Assets (AUM)
                match_aum = re.search(r'(?:Net Assets|Fund size|Assets)[\s\n]*([0-9\.\,]+[BMkT]?)', full_text_sum, re.IGNORECASE)
                if match_aum: data['assets_aum'] = match_aum.group(1)
            except: pass

            # ==================================================
            # 2. PROFILE PAGE (Priority for Charges & Turnover)
            # ==================================================
            # ข้อมูล Load Fees (Initial/Exit) และ Turnover มักอยู่หน้านี้
            url_profile = f"https://finance.yahoo.com/quote/{yf_ticker}/profile"
            try:
                await page.goto(url_profile, wait_until="domcontentloaded", timeout=45000)
                full_text_prof = await page.inner_text("body")

                # Turnover
                match_turn = re.search(r'(?:Annual Holdings Turnover|Turnover).*?(\d+\.?\d*%)', full_text_prof, re.IGNORECASE)
                if match_turn: data['holdings_turnover'] = match_turn.group(1)

                # Initial Charge
                match_init = re.search(r'(?:Max Front End Sales Load|Front[- ]?End Load).*?(\d+\.?\d*%)', full_text_prof, re.IGNORECASE)
                if match_init: data['initial_charge'] = match_init.group(1)

                # Exit Charge
                match_exit = re.search(r'(?:Deferred Sales Load|Deferred Load).*?(\d+\.?\d*%)', full_text_prof, re.IGNORECASE)
                if match_exit: data['exit_charge'] = match_exit.group(1)

                # Fallback: ถ้า Summary หา AUM/Exp ไม่เจอ ให้หาใน Profile ด้วย
                if not data['expense_ratio']:
                     match_exp = re.search(r'(?:Net Expense Ratio|Expense Ratio \(net\)|Expense Ratio)[\s\n]*(\d+\.\d+%)', full_text_prof, re.IGNORECASE)
                     if match_exp: data['expense_ratio'] = match_exp.group(1)
                if not data['assets_aum']:
                    match_aum = re.search(r'(?:Net Assets|Fund size|Assets)[\s\n]*([0-9\.\,]+[BMkT]?)', full_text_prof, re.IGNORECASE)
                    if match_aum: data['assets_aum'] = match_aum.group(1)
            except: pass

            # ==================================================
            # 3. HOLDINGS PAGE (Top 10% Risk)
            # ==================================================
            url_holdings = f"https://finance.yahoo.com/quote/{yf_ticker}/holdings"
            try:
                await page.goto(url_holdings, wait_until="domcontentloaded", timeout=45000)
                
                # 🔥 SCROLL DOWN Logic (สำคัญมาก)
                # เลื่อนลง 3 ครั้ง เพื่อให้ Table โหลด (Yahoo Holdings ใช้ Lazy Load)
                for _ in range(3):
                    await page.mouse.wheel(0, 1000)
                    await asyncio.sleep(1)
                
                full_text_hold = await page.inner_text("body")

                # Top 10 Holdings % (Concentration Risk)
                match_top10 = re.search(r'Top 10 Holdings.*?\((\d+\.\d+%)\)', full_text_hold, re.IGNORECASE)
                if not match_top10:
                     # Pattern สำรอง: "39.59% of Total Assets"
                     match_top10 = re.search(r'(\d+\.\d+)%\s+of Total Assets', full_text_hold, re.IGNORECASE)
                
                if match_top10: 
                    val = match_top10.group(1)
                    data['top_10_hold_pct'] = val if "%" in val else val + "%"

                # Total Holdings Count (Best Effort)
                match_count = re.search(r'(?:Total|Stock|Bond) Holdings[\s\n]+(\d{1,3}(?:,\d{3})*)', full_text_hold, re.IGNORECASE)
                if match_count: 
                    data['holdings_count'] = match_count.group(1).replace(',', '')
            except: pass

            # Log ผลลัพธ์
            logger.info(f"✅ {ticker} -> Exp: {data['expense_ratio']}, Init: {data['initial_charge']}, Top10: {data['top_10_hold_pct']}")
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
            logger.info("🎉 All done! No new tickers.")
            return

        async with async_playwright() as p:
            # ใช้ Chrome Headless
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
                
                # Random Delay (3-6s)
                await asyncio.sleep(random.uniform(3, 6))
                
                # Restart Context ทุกๆ 20 ตัว กัน Memory Leak / Cookie สะสม
                if i % 20 == 0:
                    await context.close()
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
                    )

            await browser.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(YFFeesScraper().run())