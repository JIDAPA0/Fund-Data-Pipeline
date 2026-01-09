import sys
import os
import asyncio
import aiohttp
import pandas as pd
import re
import time
import math
import json
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path

# ==========================================
# SYSTEM PATH SETUP
# ==========================================
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[2]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.logger import setup_logger
from src.utils.browser_utils import get_random_headers
from src.utils.db_connector import get_active_tickers

# ==========================================
# CONFIGURATION
# ==========================================
logger = setup_logger("04_ft_policy_scraper")
CONCURRENCY = 10
BATCH_SIZE = 50    

class FTPolicyScraper:
    def __init__(self):
        self.start_time = time.time()
        self.output_dir = project_root / "validation_output" / "Financial_Times" / "03_Detail_Static"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "ft_fund_policy.csv"
        
        logger.info("📡 Fetching Active Tickers...")
        all_tickers = get_active_tickers("Financial Times") 
        logger.info(f"📋 Total Active Tickers: {len(all_tickers)}")
        
        processed_tickers = set()
        if self.output_file.exists():
            try:
                df_existing = pd.read_csv(self.output_file, usecols=['ticker'])
                processed_tickers = set(df_existing['ticker'].astype(str).tolist())
                logger.info(f"⏭️ Found existing file. Skipping {len(processed_tickers)} rows.")
            except: pass

        self.tickers = [t for t in all_tickers if t['ticker'] not in processed_tickers]
        logger.info(f"✅ Remaining to Scrape: {len(self.tickers)}")
        
        self.total_processed = 0
        self.total_success = 0

    def _clean_text(self, text):
        if not text: return None
        return re.sub(r'\s+', ' ', text).strip()

    def _extract_val(self, text):
        """Clean value: remove HTML tags, commas, spaces"""
        if not text: return None
        clean = re.sub(r'<[^>]+>', '', str(text)).strip()
        if clean in ['--', '-', '', 'NA']: return None
        try:
            return clean.replace(',', '').replace('%', '') 
        except: return None

    def _parse_date(self, text):
        if not text: return None
        try:
            clean_text = re.sub(r'\.$', '', text.strip())
            match = re.search(r'([A-Za-z]{3})\s+(\d{1,2})\s+(\d{4})', clean_text)
            if match:
                date_str = f"{match.group(1)} {match.group(2)} {match.group(3)}"
                dt = datetime.strptime(date_str, "%b %d %Y")
                return dt.strftime("%Y-%m-%d")
        except: pass
        return None

    def _get_urls(self, ticker, asset_type):
        is_etf = 'ETF' in str(asset_type).upper()
        base_type = 'etfs' if is_etf else 'funds'
        
        # 1. Summary Page (Name, Dividend Yield)
        url_summary = f"https://markets.ft.com/data/{base_type}/tearsheet/summary?s={ticker}"
        
        # 2. Performance HTML Page (Trailing 1y, 3y, 5y) -> ใช้ HTML Parser แทน API ที่พัง
        url_perf_html = f"https://markets.ft.com/data/{base_type}/tearsheet/performance?s={ticker}"

        # 3. API: Annual (YTD) -> ใช้ API เพราะดึง YTD ง่ายกว่าแกะกราฟ
        url_annual_api = f"https://markets.ft.com/data/funds/ajax/trailing-total-returns?chartType=annual&symbol={ticker}"
        
        return url_summary, url_perf_html, url_annual_api

    async def fetch_page(self, session, url):
        headers = get_random_headers()
        for attempt in range(3):
            try:
                async with session.get(url, headers=headers, timeout=15) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 429:
                        await asyncio.sleep(5 * (attempt + 1))
                    else:
                        await asyncio.sleep(1)
            except:
                await asyncio.sleep(1)
        return None

    def _extract_from_json(self, json_text, keywords_list):
        """Extract value from JSON API (ใช้สำหรับ YTD)"""
        if not json_text: return None
        try:
            data_raw = json.loads(json_text)
            if 'data' in data_raw and 'chartData' in data_raw['data']:
                chart_data = json.loads(data_raw['data']['chartData'])
                headers = [h.lower() for h in chart_data.get('headers', [])] 
                values = chart_data.get('data', [])
                
                for i, h in enumerate(headers):
                    if any(kw in h for kw in keywords_list):
                        if i < len(values):
                            raw_val = values[i].get('fundPerformance')
                            return self._extract_val(raw_val)
        except: pass
        return None

    def _extract_trailing_from_html(self, html):
        """แกะตาราง HTML เพื่อเอา 1y, 3y, 5y (แม่นยำกว่า API ที่ Error 400)"""
        results = {'1y': None, '3y': None, '5y': None}
        if not html: return results

        try:
            soup = BeautifulSoup(html, 'html.parser')
            tables = soup.find_all('table')
            target_table = None
            
            # หาตารางที่มี Header '1 year'
            for tbl in tables:
                if tbl.find('th') and '1 year' in tbl.text.lower():
                    target_table = tbl
                    break
            
            if target_table:
                headers = [th.get_text(strip=True).lower() for th in target_table.find_all('th')]
                
                # หา Row ที่เป็นข้อมูลกองทุน (ไม่ใช่ Category Average)
                rows = target_table.find_all('tr')
                data_row = None
                for row in rows:
                    if row.find('td') and 'category' not in row.text.lower():
                        data_row = row
                        break
                
                if data_row:
                    cols = data_row.find_all('td')
                    
                    def get_col_val(keywords):
                        for i, h in enumerate(headers):
                            if any(kw in h for kw in keywords):
                                if i < len(cols):
                                    return self._extract_val(cols[i].text)
                        return None

                    results['1y'] = get_col_val(['1 year', '1 yr'])
                    results['3y'] = get_col_val(['3 year', '3 yr'])
                    results['5y'] = get_col_val(['5 year', '5 yr'])

        except: pass
        return results

    async def process_ticker(self, session, item, semaphore):
        ticker = item['ticker']
        asset_type = item['asset_type']
        
        url_summary, url_perf, url_annual = self._get_urls(ticker, asset_type)
        
        async with semaphore:
            # ยิง 3 Requests (Summary, HTML Perf, API Annual)
            task_sum = self.fetch_page(session, url_summary)
            task_perf = self.fetch_page(session, url_perf)
            task_annual = self.fetch_page(session, url_annual)
            
            res_sum, res_perf, res_annual = await asyncio.gather(task_sum, task_perf, task_annual)
            
            data = {
                "ticker": ticker,
                "asset_type": asset_type,
                "source": "Financial Times",
                "name": None,
                "updated_at": None,
                "dividend_yield": None,
                "total_return_ytd": None,
                "total_return_1y": None,
                "total_return_3y": None,
                "total_return_5y": None
            }

            # 1. HTML Summary (Name, Yield)
            if res_sum:
                soup = BeautifulSoup(res_sum, 'html.parser')
                header = soup.select_one('h1.mod-tearsheet-overview__header__name')
                if header: data['name'] = self._clean_text(header.text)
                
                footer = soup.find(string=re.compile(r'As of\s+[A-Za-z]{3}'))
                if footer: data['updated_at'] = self._parse_date(footer)
                
                div_yield = soup.find(string=re.compile(r'Yield', re.IGNORECASE))
                if div_yield:
                    parent = div_yield.find_parent(['tr', 'li'])
                    if parent:
                        val_node = parent.find(class_=re.compile(r'value|data'))
                        if val_node: data['dividend_yield'] = self._extract_val(val_node.text)
                    if not data['dividend_yield']:
                         val_node = div_yield.find_next(class_='mod-ui-data-list__value')
                         if val_node: data['dividend_yield'] = self._extract_val(val_node.text)

            # 2. HTML Performance (1y, 3y, 5y)
            if res_perf:
                trailing = self._extract_trailing_from_html(res_perf)
                data['total_return_1y'] = trailing['1y']
                data['total_return_3y'] = trailing['3y']
                data['total_return_5y'] = trailing['5y']

            # 3. API Annual (YTD)
            if res_annual:
                data['total_return_ytd'] = self._extract_from_json(res_annual, ['ytd'])

            return data

    async def scrape_batch(self, batch_tickers):
        headers = get_random_headers()
        connector = aiohttp.TCPConnector(limit=CONCURRENCY)
        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = [self.process_ticker(session, t, sem) for t in batch_tickers]
            results = await asyncio.gather(*tasks)
            return results

    def save_incremental(self, results):
        if not results: return
        df = pd.DataFrame(results)
        
        valid_cols = [
            'ticker', 'asset_type', 'source', 'name', 
            'dividend_yield',
            'total_return_ytd', 
            'total_return_1y',
            'updated_at',
            # เก็บ 3y, 5y ลง CSV ด้วย (เผื่อ DB ยังไม่มีก็ไม่เป็นไร CSV จะมีครบ)
            'total_return_3y', 
            'total_return_5y'
        ]
        
        for col in valid_cols:
            if col not in df.columns: df[col] = None
            
        df = df[valid_cols]
        use_header = not self.output_file.exists()
        df.to_csv(self.output_file, mode='a', header=use_header, index=False)

    async def run(self):
        if not self.tickers: return
        logger.info(f"🚀 Starting FT Policy Scraper (Hybrid HTML+API)")
        
        total = len(self.tickers)
        batches = math.ceil(total / BATCH_SIZE)
        
        for i in range(batches):
            start = time.time()
            batch = self.tickers[i*BATCH_SIZE : (i+1)*BATCH_SIZE]
            
            results = await self.scrape_batch(batch)
            self.save_incremental(results)
            self.total_success += len(results)
            self.total_processed += len(batch)
            
            dur = time.time() - start
            logger.info(f"Batch {i+1}/{batches} | Saved: {len(results)} | Progress: {self.total_processed}/{total} | Time: {dur:.2f}s")
            
            await asyncio.sleep(1)

async def main():
    scraper = FTPolicyScraper()
    await scraper.run()
    logger.info(f"✅ Finished! Total Saved: {scraper.total_success}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())