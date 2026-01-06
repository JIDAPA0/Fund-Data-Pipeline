import sys
import os
import asyncio
import pandas as pd
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

from src.utils.path_manager import get_validation_path
from src.utils.logger import setup_logger
from src.utils.browser_utils import get_random_user_agent
from src.utils.db_connector import insert_dataframe

# ==========================================
# CONFIGURATION
# ==========================================
logger = setup_logger("02_perf_ft_nav_repair")

class FTNavRepair:
    def __init__(self):
        self.current_date = datetime.now().strftime('%Y-%m-%d')

        # Match path with 02_perf_ft_nav.py
        self.input_file = get_validation_path(
            "Financial_Times", 
            "02_Daily_NAV", 
            f"{self.current_date}/ft_nav_results.csv"
        )
        self.repair_log = self.input_file.parent / "ft_repair_log.csv"

    def get_failed_tickers(self):
        if not self.input_file.exists():
            logger.error("❌ Main result file not found.")
            return []
        
        try:
            df = pd.read_csv(self.input_file)
            # Filter rows where status is NOT 'Success'
            if 'status' in df.columns:
                failed = df[df['status'] != 'Success']
            else:
                failed = df[df['nav_price'].isna() | (df['nav_price'] == 0)]
            
            return failed[['ticker', 'asset_type']].to_dict('records')
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            return []

    async def process_ticker(self, context, item):
        ticker = item['ticker']
        asset_type = item['asset_type']
        
        page = await context.new_page()
        try:
            url = f"https://markets.ft.com/data/funds/tearsheet/summary?s={ticker}"
            await page.goto(url, timeout=30000, wait_until='domcontentloaded')
            
            data = await page.evaluate("""() => {
                const getVal = (s) => document.querySelector(s)?.innerText.trim() || null;
                const disc = document.querySelector('.mod-disclaimer')?.innerText || "";
                const over = document.querySelector('.mod-tearsheet-overview')?.innerText || "";
                
                const matchDate = (txt) => {
                    const m = txt.match(/as of\\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+(\\d{1,2})\\s+(\\d{4})/i);
                    return m ? `${m[1]} ${m[2]} ${m[3]}` : null;
                };
                
                return {
                    nav: getVal('.mod-tearsheet-overview__quote__value')?.replace(/,/g, ''),
                    date_str: matchDate(disc) || matchDate(over) || getVal('.mod-tearsheet-overview__date'),
                    curr: over.match(/Price\\s*\\(([A-Z]{3})\\)/i)?.[1] || 'USD'
                };
            }""")

            nav_raw = data['nav']
            
            if nav_raw and nav_raw != "-":
                try:
                    nav_date_obj = datetime.strptime(data['date_str'], '%b %d %Y')
                    nav_date = nav_date_obj.strftime('%Y-%m-%d')
                except:
                    nav_date = self.current_date

                return {
                    "ticker": ticker, 
                    "asset_type": asset_type,
                    "source": "Financial Times",
                    "nav_price": float(nav_raw),
                    "currency": data['curr'],
                    "as_of_date": nav_date, 
                    "scrape_date": self.current_date,
                    "status": "Repaired"
                }
        except Exception:
            pass
        finally:
            await page.close()
        return None

    async def run_repair(self):
        to_fix = self.get_failed_tickers()
        if not to_fix:
            logger.info("🎉 No failed items to repair. Perfect run!")
            return

        logger.info(f"🔧 Starting Repair for {len(to_fix)} items using Playwright...")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            BATCH_SIZE = 10
            success_count = 0
            
            for i in range(0, len(to_fix), BATCH_SIZE):
                batch = to_fix[i : i+BATCH_SIZE]
                context = await browser.new_context(user_agent=get_random_user_agent())
                
                tasks = [self.process_ticker(context, item) for item in batch]
                results = await asyncio.gather(*tasks)
                
                valid_data = [r for r in results if r]
                
                if valid_data:
                    df = pd.DataFrame(valid_data)
                    # 1. Save to DB
                    try:
                        insert_dataframe(df.drop(columns=['status']), "stg_daily_nav")
                    except: pass
                    
                    # 2. Save Log
                    df.to_csv(self.repair_log, mode='a', header=not self.repair_log.exists(), index=False)
                    success_count += len(valid_data)

                await context.close()
                logger.info(f"Repair Batch {i//BATCH_SIZE + 1} | Fixed: {len(valid_data)}/{len(batch)}")

            await browser.close()
            logger.info(f"✨ Repair Complete. Total Fixed: {success_count}/{len(to_fix)}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    repair = FTNavRepair()
    asyncio.run(repair.run_repair())