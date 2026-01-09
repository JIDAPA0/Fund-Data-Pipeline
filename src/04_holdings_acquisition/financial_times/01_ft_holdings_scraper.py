import sys
import asyncio
import pandas as pd
import re
import random
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

from src.utils.logger import setup_logger
from src.utils.db_connector import get_active_tickers

# ==========================================
# CONFIGURATION
# ==========================================
logger = setup_logger("01_ft_full_scraper")
OUTPUT_DIR = project_root / "validation_output" / "Financial_Times" / "04_Holdings"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class FTFullScraper:
    def __init__(self):
        self.tickers_data = get_active_tickers("Financial Times")
        
        # Resume Logic: เช็คว่าทำไปแล้วหรือยัง
        self.processed_tickers = set()
        for f in OUTPUT_DIR.glob("*_holdings.csv"):
            safe_ticker = f.name.replace("_holdings.csv", "")
            self.processed_tickers.add(safe_ticker)

        logger.info(f"✅ Total Tickers: {len(self.tickers_data)} | Already Scraped: {len(self.processed_tickers)}")

    def _get_url(self, ticker, asset_type):
        base = 'etfs' if 'ETF' in str(asset_type).upper() else 'funds'
        return f"https://markets.ft.com/data/{base}/tearsheet/holdings?s={ticker}"

    def _clean_val(self, text):
        if not text: return None
        clean = text.strip().replace('%', '').replace(',', '')
        if clean in ['--', '-', '']: return None
        try: return float(clean)
        except: return None

    def _clean_name(self, text):
        # ลบ Ticker ที่ติดมา (เช่น "NVIDIA Corp\n\nNVDA:NSQ" -> "NVIDIA Corp")
        if not text: return None
        return text.split('\n')[0].strip()

    def _classify_category(self, header_category, first_item_name):
        cat = header_category
        item = first_item_name.lower()
        
        # คำบ่งชี้ว่าเป็น ภูมิภาค/ประเทศ
        geo_keywords = [
            'americas', 'europe', 'asia', 'africa', 'kingdom', 'states', 
            'china', 'japan', 'india', 'germany', 'france', 'euro', 'emerging', 'developed',
            'latin', 'middle east', 'pacific', 'canada', 'australia'
        ]
        
        # ถ้า Header คือ Sector แต่ไส้ในเจอชื่อประเทศ -> เปลี่ยนเป็น Geography
        if "sector" in cat.lower():
            if any(k in item for k in geo_keywords):
                return "Geography"
        
        if "type" in cat.lower():
            return "Asset Class"
            
        return cat

    async def scrape_ticker(self, page, item):
        ticker, atype = item['ticker'], item['asset_type']
        url = self._get_url(ticker, atype)
        
        safe_ticker = ticker.replace(':', '_').replace('/', '_')
        
        file_holdings = OUTPUT_DIR / f"{safe_ticker}_holdings.csv"
        file_alloc = OUTPUT_DIR / f"{safe_ticker}_alloc.csv"

        if safe_ticker in self.processed_tickers or file_holdings.exists():
            return None

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            try: await page.wait_for_selector("table", timeout=5000)
            except: pass 

            # หาวันที่รวม (Backup Date)
            body_text = await page.inner_text("body")
            global_date = None
            m = re.search(r'As of\s+([A-Za-z]{3}\s+\d{1,2}\s+\d{4})', body_text, re.IGNORECASE)
            if m:
                try:
                    dt = datetime.strptime(m.group(1), "%b %d %Y")
                    global_date = dt.strftime("%Y-%m-%d")
                except: pass

            tables = await page.locator("table").all()
            
            holdings_data = []
            allocations_data = []

            for tbl in tables:
                headers = await tbl.locator("th").all_inner_texts()
                clean_headers = [h.lower().strip() for h in headers]
                
                # --- Identification (Loose Matching) ---
                is_holdings = any(x in h for h in clean_headers for x in ['company', 'security', 'constituent'])
                has_val = any(x in h for h in clean_headers for x in ['% net assets', 'net assets', 'weight', 'value', '%'])
                is_allocation = has_val and not is_holdings
                is_summary = any('top 10' in h for h in clean_headers)

                if is_summary: continue

                rows = await tbl.locator("tr").all()
                
                # Find Value Index
                idx_val = -1
                for j, h in enumerate(clean_headers):
                    if any(x in h for x in ['% net assets', 'net assets', 'weight', 'value', '%']): 
                        idx_val = j
                        break
                if idx_val == -1: continue

                # --- PROCESS HOLDINGS ---
                if is_holdings:
                    idx_name = -1
                    for j, h in enumerate(clean_headers):
                        if any(x in h for x in ['company', 'security', 'constituent']): 
                            idx_name = j
                            break
                    
                    for row in rows:
                        cols = await row.locator("td").all()
                        if len(cols) <= max(idx_name, idx_val): continue
                        
                        raw_name = await cols[idx_name].inner_text()
                        raw_val = await cols[idx_val].inner_text()
                        
                        name = self._clean_name(raw_name)
                        val = self._clean_val(raw_val)
                        
                        if name and val is not None and "total" not in name.lower():
                            holdings_data.append({
                                'ticker': ticker,
                                'asset_type': atype,
                                'source': 'Financial Times',
                                'as_of_date': global_date,
                                'holding_name': name,
                                'holding_weight': val
                            })

                # --- PROCESS ALLOCATION ---
                elif is_allocation:
                    raw_cat = headers[0].strip().title()
                    
                    # Smart Classification: Check first row item
                    first_item = ""
                    for r_check in rows[:3]:
                        c = await r_check.locator("td").all()
                        if c:
                            txt = await c[0].inner_text()
                            if txt.strip():
                                first_item = txt.strip()
                                break

                    smart_cat = self._classify_category(raw_cat, first_item)
                    
                    for row in rows:
                        cols = await row.locator("td").all()
                        if len(cols) <= idx_val: continue
                        
                        name = await cols[0].inner_text()
                        val = await cols[idx_val].inner_text()
                        
                        name = name.strip()
                        val = self._clean_val(val)
                        
                        if name and val is not None and "total" not in name.lower() and "--" not in name:
                            allocations_data.append({
                                'ticker': ticker,
                                'asset_type': atype,
                                'source': 'Financial Times',
                                'as_of_date': global_date,
                                'category': smart_cat,
                                'item_name': name,
                                'weight': val
                            })

            # Save Files
            res_code = 0
            if holdings_data:
                pd.DataFrame(holdings_data).to_csv(file_holdings, index=False)
                res_code = 1
            
            if allocations_data:
                pd.DataFrame(allocations_data).to_csv(file_alloc, index=False)
                res_code = 1

            if res_code == 1:
                logger.info(f"✅ Saved {ticker}: Holdings={len(holdings_data)}, Alloc={len(allocations_data)}")
                return 1
            else:
                return 0

        except Exception as e:
            logger.error(f"❌ Error {ticker}: {e}")
            return None

    async def run(self):
        queue = []
        for t in self.tickers_data:
            safe_ticker = t['ticker'].replace(':', '_').replace('/', '_')
            if safe_ticker not in self.processed_tickers:
                queue.append(t)

        logger.info(f"🚀 Full Scraper Started. Remaining: {len(queue)}")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            success_count = 0
            for i, item in enumerate(queue, 1):
                res = await self.scrape_ticker(page, item)
                if res: success_count += 1
                
                if i % 50 == 0:
                    await context.close()
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                    page = await context.new_page()
                
                await asyncio.sleep(random.uniform(1.5, 3))

            await browser.close()
        logger.info(f"🎉 Finished! Saved data for {success_count} funds.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(FTFullScraper().run())