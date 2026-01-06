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
logger = setup_logger("04_ft_policy_api_master")
CONCURRENCY = 5    # 🐢 Safe Speed
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
        
        clean = re.sub(r'<[^>]+>', '', str(text))
        clean = clean.strip()
        if clean == '--' or clean == '-': return None
        return clean.replace(',', '') 

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
        
        
        main_url = f"https://markets.ft.com/data/{base_type}/tearsheet/performance?s={ticker}"
        
        
        
        api_url = f"https://markets.ft.com/data/funds/ajax/trailing-total-returns?chartType=annual&symbol={ticker}"
        
        return main_url, api_url

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

    def _extract_1y_from_html(self, html):
        if not html: return None
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table')
        
        for table in tables:
            headers = [th.text.strip().lower() for th in table.find_all('th')]
            
            target_idx = -1
            for i, h in enumerate(headers):
                if '1 year' in h and '3 year' not in h:
                    target_idx = i
                    break
            
            if target_idx != -1:
                
                rows = table.find_all('tr')
                for r in rows:
                    if r.find('th'): continue
                    cols = r.find_all('td')
                    if len(cols) > target_idx:
                        
                        first_text = cols[0].text.strip().lower()
                        if "quartile" not in first_text and "category" not in first_text:
                            return self._extract_val(cols[target_idx].text)
        return None

    def _extract_ytd_from_json(self, json_text):
        if not json_text: return None
        try:
            data_raw = json.loads(json_text)
            if 'data' in data_raw and 'chartData' in data_raw['data']:
                
                chart_data = json.loads(data_raw['data']['chartData'])
                
                headers = chart_data.get('headers', [])
                values = chart_data.get('data', [])
                
                
                ytd_idx = -1
                for i, h in enumerate(headers):
                    if h == 'YTD':
                        ytd_idx = i
                        break
                
                if ytd_idx != -1 and ytd_idx < len(values):
                    
                    raw_val = values[ytd_idx].get('fundPerformance')
                    return self._extract_val(raw_val) # Clean HTML tags if any
        except:
            pass
        return None

    async def process_ticker(self, session, item, semaphore):
        ticker = item['ticker']
        asset_type = item['asset_type']
        
        main_url, api_url = self._get_urls(ticker, asset_type)
        
        async with semaphore:
            # Task 1: Main Page (1Y, Name, Date)
            task_main = self.fetch_page(session, main_url)
            # Task 2: API (YTD)
            task_api = self.fetch_page(session, api_url)
            
            res_main, res_api = await asyncio.gather(task_main, task_api)
            
            data = {
                "ticker": ticker,
                "asset_type": asset_type,
                "source": "Financial Times",
                "name": None,
                "updated_at": None,
                "total_return_1y": None,
                "total_return_ytd": None
            }

            # Process HTML
            if res_main:
                soup = BeautifulSoup(res_main, 'lxml')
                header = soup.select_one('h1.mod-tearsheet-overview__header__name')
                if header: data['name'] = self._clean_text(header.text)
                
                footer = soup.find(string=re.compile(r'As of\s+[A-Za-z]{3}'))
                if footer: data['updated_at'] = self._parse_date(footer)
                
                data['total_return_1y'] = self._extract_1y_from_html(res_main)

            # Process JSON API
            if res_api:
                data['total_return_ytd'] = self._extract_ytd_from_json(res_api)

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
            'total_return_ytd', 
            'total_return_1y',
            'updated_at'
        ]
        
        for col in valid_cols:
            if col not in df.columns: df[col] = None
            
        df = df[valid_cols]
        use_header = not self.output_file.exists()
        df.to_csv(self.output_file, mode='a', header=use_header, index=False)

    async def run(self):
        if not self.tickers: return
        logger.info(f"🚀 Starting FT Policy Scraper (API Master)")
        
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