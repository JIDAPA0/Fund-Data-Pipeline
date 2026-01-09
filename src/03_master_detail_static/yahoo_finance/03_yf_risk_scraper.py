import sys
import asyncio
import pandas as pd
import random
import re
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
logger = setup_logger("03_yf_risk_scraper")

OUTPUT_DIR = project_root / "validation_output" / "Yahoo_Finance" / "03_Detail_Static"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "yf_fund_risk.csv"

# Mapping: ชื่อบนหน้าเว็บ -> ชื่อ prefix ใน database
metrics_map = {
    "alpha": "Alpha",
    "beta": "Beta",
    "mean_annual_return": "Mean Annual Return",
    "r_squared": "R-Squared",
    "standard_deviation": "Standard Deviation",
    "sharpe_ratio": "Sharpe Ratio",
    "treynor_ratio": "Treynor Ratio"
}

# สร้าง Columns Header: ticker, rating, alpha_3y, alpha_5y, ...
COLS = ["ticker", "morningstar_rating"]
for m in metrics_map.keys():
    for y in ["3y", "5y", "10y"]:
        COLS.append(f"{m}_{y}")
COLS.append("updated_at")

class YFRiskScraper:
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

    async def scrape_risk(self, page, ticker):
        # ตัด Suffix เพื่อเข้า URL ให้ถูก (เช่น VOO:PCQ -> VOO)
        yf_ticker = ticker.split(':')[0]
        url = f"https://finance.yahoo.com/quote/{yf_ticker}/risk"
        
        data = {c: None for c in COLS}
        data["ticker"] = ticker
        data["updated_at"] = pd.Timestamp.now().strftime("%Y-%m-%d")

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Scroll เพื่อโหลดตาราง (Lazy Load)
            await page.evaluate("window.scrollBy(0, 500)")
            await asyncio.sleep(2) # รอ Animation

            # --- 1. MORNINGSTAR RATING (STARS) ---
            try:
                # หา span ที่มีดาว ★
                stars_elements = await page.locator('span:has-text("★")').all_inner_texts()
                if stars_elements:
                    # เลือกอันที่มีดาวเยอะสุด (เช่น ★★★)
                    rating_str = max(stars_elements, key=lambda x: x.count("★"))
                    data["morningstar_rating"] = rating_str.count("★")
            except: pass

            # --- 2. RISK METRICS (TABLE STRATEGY) ---
            # ใช้ Locator หาตารางที่มีคำว่า "Alpha" (แม่นยำกว่า data-testid)
            try:
                # หา Table ที่มีคำว่า Alpha อยู่ข้างใน
                table_loc = page.locator("table").filter(has_text="Alpha").first
                
                # วนลูปทุกแถว (tr)
                rows = await table_loc.locator("tr").all()
                
                for row in rows:
                    # ดึง Text ทั้งหมดในแถว (Label + Values)
                    cells = await row.locator("td").all_inner_texts()
                    if not cells: continue # ข้ามแถว header หรือว่าง
                    
                    row_label = cells[0].strip() # ตัวแรกคือ Label (e.g., "Alpha")
                    
                    # เช็คว่า Label ตรงกับ Metric ไหนใน map ของเราไหม
                    for metric_key, web_label in metrics_map.items():
                        # เปรียบเทียบแบบ Case Insensitive
                        if web_label.lower() in row_label.lower():
                            # Yahoo Table Columns Format: [Label, 3Y, 5Y, 10Y]
                            # บางทีมี Benchmark แทรก หรือบางคอลัมน์หายไป
                            
                            # ปกติ: Label | 3Y | 5Y | 10Y
                            if len(cells) >= 2: data[f"{metric_key}_3y"] = cells[1].strip()
                            if len(cells) >= 3: data[f"{metric_key}_5y"] = cells[2].strip()
                            if len(cells) >= 4: data[f"{metric_key}_10y"] = cells[3].strip()
                            
                            break # เจอแล้วหยุดวน map สำหรับแถวนี้
                            
            except Exception as e:
                # ถ้าหาตารางไม่เจอ (เช่น ETF บางตัวไม่มีตาราง Risk) ก็ปล่อยผ่าน
                pass

            # Log ตรวจสอบความถูกต้อง
            log_beta = data.get('beta_3y', '-')
            log_rating = data.get('morningstar_rating', '-')
            logger.info(f"✅ {ticker}: Rating={log_rating}, Beta(3Y)={log_beta}")
            return data

        except Exception as e:
            logger.error(f"❌ {ticker} Error: {e}")
            return None

    async def run(self):
        queue = [t for t in self.tickers_data if t['ticker'] not in self.processed_tickers]
        logger.info(f"🚀 Risk Scraper Started. Remaining: {len(queue)}")
        
        if not queue: 
            logger.info("🎉 All done! No new tickers.")
            return

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            for i, item in enumerate(queue, 1):
                res = await self.scrape_risk(page, item['ticker'])
                if res:
                    df = pd.DataFrame([res])[COLS]
                    use_header = not OUTPUT_FILE.exists()
                    df.to_csv(OUTPUT_FILE, mode='a', header=use_header, index=False)
                
                # Random Delay (ลดความเสี่ยงโดนบล็อก)
                await asyncio.sleep(random.uniform(2, 4))
                
                # Restart Context ทุกๆ 20 ตัว
                if i % 20 == 0:
                    await context.close()
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
                    )
                    page = await context.new_page()

            await browser.close()
        logger.info("🎉 Risk Scraper Finished!")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(YFRiskScraper().run())