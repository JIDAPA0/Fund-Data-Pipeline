import sys
import os
import asyncio
import aiohttp
import pandas as pd
import re
import time
import math
from bs4 import BeautifulSoup
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
logger = setup_logger("01_ft_info_turbo")

CONCURRENCY = 50  
BATCH_SIZE = 100  

class FTInfoScraper:
    def __init__(self):
        self.start_time = time.time()
        
        # Output Path
        self.output_dir = project_root / "validation_output" / "Financial_Times" / "03_Detail_Static"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "ft_fund_info.csv"
        
        # Load Tickers
        logger.info("📡 Fetching Active Tickers...")
        self.tickers = get_active_tickers("Financial Times") 
        logger.info(f"✅ Loaded {len(self.tickers)} active tickers.")
        
        self.total_processed = 0
        self.total_success = 0

    def _clean_text(self, text):
        if not text: return None
        return re.sub(r'\s+', ' ', text).strip()

    def _extract_table_value(self, soup, label_pattern):
        """Helper to extract value by label"""
        target = soup.find(['th', 'span', 'div', 'td'], string=re.compile(label_pattern, re.IGNORECASE))
        if target:
            if target.name in ['th', 'td']:
                sibling = target.find_next_sibling('td')
                if sibling: return self._clean_text(sibling.text)
            
            parent = target.find_parent(['tr', 'div', 'li'])
            if parent:
                value = parent.find(class_=re.compile(r'value|data'))
                if value: return self._clean_text(value.text)
                val_td = parent.find_all('td')
                if len(val_td) > 1: return self._clean_text(val_td[1].text)
        return None

    # =========================================================================
    # 1. SUMMARY PAGE
    # =========================================================================
    async def get_summary_data(self, session, ticker):
        url = f"https://markets.ft.com/data/funds/tearsheet/summary?s={ticker}"
        try:
            async with session.get(url, timeout=8) as response:
                if response.status != 200: return None
                html = await response.text()
                soup = BeautifulSoup(html, 'lxml')
                
                data = {}
                header = soup.select_one('h1.mod-tearsheet-overview__header__name')
                data['name'] = self._clean_text(header.text) if header else None
                
                symbol_tag = soup.select_one('.mod-tearsheet-overview__header__symbol')
                if symbol_tag:
                    parts = symbol_tag.text.split(':')
                    data['exchange'] = parts[-1].strip() if len(parts) > 1 else None
                else:
                    data['exchange'] = None

                data['isin_number'] = self._extract_table_value(soup, r'ISIN')
                data['category'] = self._extract_table_value(soup, r'Morningstar Category|Category|Asset class')
                data['inception_date'] = self._extract_table_value(soup, r'Launch date|Inception')
                data['country'] = self._extract_table_value(soup, r'Domicile')
                
                # --- Market Cap & Style Extraction (Fixed Regex) ---
                inv_style_raw = self._extract_table_value(soup, r'Investment style')
                data['market_cap_size'] = None
                data['investment_style'] = None
                
                if inv_style_raw:
                    mc_match = re.search(r'Market Cap:\s*(.*?)(?=\s*Investment Style|$)', inv_style_raw, re.IGNORECASE)
                    st_match = re.search(r'Investment Style:\s*(.*)', inv_style_raw, re.IGNORECASE)
                    
                    if mc_match: data['market_cap_size'] = self._clean_text(mc_match.group(1))
                    if st_match: data['investment_style'] = self._clean_text(st_match.group(1))
                
                return data
        except:
            return None

    # =========================================================================
    # 2. HOLDINGS PAGE
    # =========================================================================
    async def get_region_data(self, session, ticker):
        url = f"https://markets.ft.com/data/funds/tearsheet/holdings?s={ticker}"
        try:
            async with session.get(url, timeout=8) as response:
                if response.status != 200: return None
                html = await response.text()
                soup = BeautifulSoup(html, 'lxml')
                
                header = soup.find(string=re.compile(r'Geographical breakdown|Asset allocation', re.IGNORECASE))
                if header:
                    table = header.find_parent('div').find_next('table')
                    if table:
                        rows = table.find('tbody').find_all('tr')
                        if len(rows) > 0:
                            cols = rows[0].find_all('td')
                            if len(cols) >= 1:
                                txt = self._clean_text(cols[0].text)
                                if txt and "Cash" not in txt and "Other" not in txt:
                                    return txt
        except:
            pass
        return None

    async def process_ticker(self, session, item, semaphore):
        ticker = item['ticker']
        asset_type = item['asset_type']
        
        async with semaphore:
            summary_task = self.get_summary_data(session, ticker)
            region_task = self.get_region_data(session, ticker)
            
            summary_data, region = await asyncio.gather(summary_task, region_task)

            if not summary_data: return None 
            
            return {
                "ticker": ticker,
                "asset_type": asset_type,
                "source": "Financial Times",
                "name": summary_data.get('name'),
                "isin_number": summary_data.get('isin_number'),
                "category": summary_data.get('category'),
                "inception_date": summary_data.get('inception_date'),
                "exchange": summary_data.get('exchange'),
                "region": region, 
                "country": summary_data.get('country'),
                "market_cap_size": summary_data.get('market_cap_size'),
                "investment_style": summary_data.get('investment_style')
            }

    async def scrape_batch(self, batch_tickers):
        headers = get_random_headers()
        
        connector = aiohttp.TCPConnector(limit=CONCURRENCY)
        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = [self.process_ticker(session, t, sem) for t in batch_tickers]
            results = await asyncio.gather(*tasks)
            return [r for r in results if r]

    def save_incremental(self, results):
        if not results: return
        df = pd.DataFrame(results)
        
        desired_order = [
            'ticker', 'asset_type', 'source', 'name', 'isin_number', 
            'category', 'inception_date', 'exchange', 
            'region', 'country', 'market_cap_size', 'investment_style'
        ]
        
        final_cols = [c for c in desired_order if c in df.columns]
        df = df[final_cols]
        
        use_header = not self.output_file.exists()
        df.to_csv(self.output_file, mode='a', header=use_header, index=False)

    async def run(self):
        if not self.tickers: return

        logger.info(f"🚀 Starting Turbo Scraper (Concurrency: {CONCURRENCY})")
        logger.info(f"💾 Saving to {self.output_file}")
        
        total_items = len(self.tickers)
        total_batches = math.ceil(total_items / BATCH_SIZE)
        
        for i in range(total_batches):
            batch_start = time.time()
            start_idx = i * BATCH_SIZE
            end_idx = start_idx + BATCH_SIZE
            batch_tickers = self.tickers[start_idx:end_idx]
            
            results = await self.scrape_batch(batch_tickers)
            
            if results:
                self.save_incremental(results)
                self.total_success += len(results)
            
            self.total_processed += len(batch_tickers)
            
            duration = time.time() - batch_start
            logger.info(f"Batch {i+1}/{total_batches} | Saved: {len(results)} | Total: {self.total_success}/{self.total_processed} | Time: {duration:.2f}s")

async def main():
    scraper = FTInfoScraper()
    await scraper.run()
    
    total_duration = time.time() - scraper.start_time
    logger.info("="*50)
    logger.info(f"🎉 JOB COMPLETED")
    logger.info(f"⏱️  Total Time: {total_duration/60:.2f} min")
    logger.info(f"✅ Active Found: {scraper.total_success}")
    logger.info("="*50)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())