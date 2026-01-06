import sys
import os
import asyncio
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright

# ==========================================
# 1. SETUP
# ==========================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(BASE_DIR)

try:
    from src.utils.path_manager import DATA_PERFORMANCE_DIR
    from src.utils.logger import setup_logger
    from src.utils.db_connector import insert_dataframe
    logger = setup_logger("02_perf_yf_fund_repair")
except ImportError:
    sys.exit(1)

class YFRepair:
    def __init__(self):
        self.current_date = datetime.now().strftime('%Y-%m-%d')
        self.input_dir = DATA_PERFORMANCE_DIR / "yahoo_finance" / self.current_date
        # อ่านจากไฟล์ Error ของ Fund
        self.error_file = self.input_dir / "yf_errors_fund.csv"
        self.repair_log = self.input_dir / "yf_fund_repair_log.csv"

    def get_failed_tickers(self):
        if not self.error_file.exists():
            logger.warning("🚫 No error file found. Nothing to repair.")
            return []
        try:
            df = pd.read_csv(self.error_file)
            return df['ticker'].unique().tolist()
        except: return []

    async def process_ticker(self, context, ticker):
        page = await context.new_page()
        try:
            url = f"https://finance.yahoo.com/quote/{ticker}"
            # Yahoo โหลดหนักหน่อย ให้เวลา 30 วิ
            await page.goto(url, timeout=30000, wait_until='domcontentloaded')
            
            # ใช้ JS แกะราคาจาก fin-streamer
            data = await page.evaluate("""() => {
                const getPrice = (field) => {
                    const el = document.querySelector(`fin-streamer[data-field="${field}"]`);
                    return el ? el.innerText.replace(/,/g, '') : null;
                };
                
                // ลองหาหลายๆ field
                let price = getPrice('regularMarketPrice') || getPrice('regularMarketOpen') || getPrice('navPrice');
                
                // ถ้ายังไม่เจอ ลองหาจาก class ทั่วไป (เผื่อ Yahoo เปลี่ยน layout)
                if (!price) {
                    const el = document.querySelector('fin-streamer[data-test="qsp-price"]');
                    if (el) price = el.innerText.replace(/,/g, '');
                }

                return { price };
            }""")

            if data['price'] and data['price'].replace('.', '', 1).isdigit():
                return {
                    "ticker": ticker,
                    "asset_type": "FUND",
                    "source": "Yahoo Finance",
                    "nav_price": float(data['price']),
                    "currency": "USD", # Default
                    "as_of_date": self.current_date,
                    "scrape_date": self.current_date
                }
        except Exception:
            pass
        finally:
            await page.close()
        return None

    async def run(self):
        tickers = self.get_failed_tickers()
        if not tickers:
            return

        logger.info(f"🚑 Starting Repair for {len(tickers)} items...")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            # ทำทีละ 5 ตัวพอ (Playwright กิน RAM เยอะ)
            BATCH_SIZE = 5
            success_count = 0
            
            for i in range(0, len(tickers), BATCH_SIZE):
                batch = tickers[i:i+BATCH_SIZE]
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                
                tasks = [self.process_ticker(context, t) for t in batch]
                results = await asyncio.gather(*tasks)
                
                valid = [r for r in results if r]
                
                if valid:
                    df = pd.DataFrame(valid)
                    try: insert_dataframe(df, "stg_daily_nav")
                    except: pass
                    df.to_csv(self.repair_log, mode='a', header=not self.repair_log.exists(), index=False)
                    success_count += len(valid)
                
                await context.close()
                logger.info(f"Repair Batch {i//BATCH_SIZE + 1} | Fixed: {len(valid)}/{len(batch)}")

            await browser.close()
            logger.info(f"🏁 Repair Finished. Rescued: {success_count}/{len(tickers)}")

if __name__ == "__main__":
    repair = YFRepair()
    asyncio.run(repair.run())