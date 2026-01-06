import sys
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

# ✅ ชื่อ Log ตามที่กำหนด
logger = setup_logger("03_master_detail_static_risk")

# ✅ FIX PATH
OUTPUT_DIR = project_root / "validation_output" / "Yahoo_Finance" / "03_Detail_Static"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "yf_fund_risk.csv"

# คอลัมน์ที่ต้องการ
metrics = ["alpha", "beta", "mean_annual_return", "r_squared", "standard_deviation", "sharpe_ratio", "treynor_ratio"]
COLS = ["ticker", "morningstar_rating"]
for m in metrics:
    for y in ["3y", "5y", "10y"]:
        COLS.append(f"{m}_{y}")

class YFRiskScraper:
    def __init__(self):
        self.tickers_data = get_active_tickers("Yahoo Finance")
        if not OUTPUT_FILE.exists():
            pd.DataFrame(columns=COLS).to_csv(OUTPUT_FILE, index=False)

    async def scrape_risk(self, page, ticker):
        print(f"⏳ เจาะข้อมูล Risk (Rating to Number): {ticker} ...{' '*10}", end='\r', flush=True)
        data = {c: None for c in COLS}
        data["ticker"] = ticker

        try:
            # 1. โหลดหน้า Risk
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/risk", wait_until="domcontentloaded", timeout=60000)
            
            # 2. รอ Selector ตารางสถิติ
            target_selector = 'section[data-testid="risk-statistics-table"]'
            try:
                await page.wait_for_selector(target_selector, timeout=20000)
            except:
                # กรณีไม่พบตารางสถิติ (เช่น กองทุนใหม่เกินไป)
                return None

            await asyncio.sleep(2) # รอ Render ตัวเลข

            # 3. ดึงข้อมูลจากตาราง Risk Statistics
            rows = page.locator(f'{target_selector} tbody tr')
            count = await rows.count()
            
            for i in range(count):
                cells = rows.nth(i).locator('td')
                cell_count = await cells.count()
                if cell_count < 2: continue
                
                label = (await cells.nth(0).inner_text()).lower().strip()
                
                for m in metrics:
                    match_label = m.replace('_', ' ')
                    if match_label in label or (m == "beta" and label == "beta"):
                        # ดึงค่า Fund (Index 1, 3, 5)
                        data[f"{m}_3y"] = await cells.nth(1).inner_text() if cell_count > 1 else None
                        data[f"{m}_5y"] = await cells.nth(3).inner_text() if cell_count > 3 else None
                        data[f"{m}_10y"] = await cells.nth(5).inner_text() if cell_count > 5 else None

            # 4. ดึง Morningstar Rating และแปลงดาวเป็นตัวเลข
            try:
                # พยายามหาจากตาราง Risk Overview
                rating_row = page.locator('section[data-testid="risk-overview"] tr:has-text("Morningstar Risk Rating")')
                if await rating_row.count() > 0:
                    raw_rating = await rating_row.locator('td').last.inner_text()
                    
                    # ลอจิกแปลงดาว: นับจำนวนตัวอักษร ★
                    if '★' in raw_rating:
                        data["morningstar_rating"] = raw_rating.count('★')
                    elif raw_rating.isdigit():
                        data["morningstar_rating"] = int(raw_rating)
                    else:
                        data["morningstar_rating"] = None
            except:
                pass

            logger.info(f"✅ {ticker}: Extracted (Rating: {data['morningstar_rating']})")
            return data
        except Exception as e:
            return None

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            # ใช้ User-Agent ของ Mac เพื่อความเสถียร
            context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            page = await context.new_page()
            
            # Resume: ข้ามตัวที่เคยทำแล้ว
            processed = set(pd.read_csv(OUTPUT_FILE)['ticker'].astype(str)) if OUTPUT_FILE.exists() else set()
            queue = [t for t in self.tickers_data if t['ticker'] not in processed]

            print(f"🚀 เริ่มประมวลผล Risk Data... เหลืออีก {len(queue)} รายการ")

            for i, item in enumerate(queue, 1):
                res = await self.scrape_risk(page, item['ticker'])
                if res:
                    pd.DataFrame([res])[COLS].to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
                
                # พักเครื่องกันโดนแบน
                if i % 10 == 0:
                    await asyncio.sleep(random.uniform(3, 6))
                else:
                    await asyncio.sleep(random.uniform(1, 2))
            
            await browser.close()
        print("\n🎉 จบการทำงาน! ข้อมูลพร้อมใช้งานใน CSV แล้วครับ")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(YFRiskScraper().run())