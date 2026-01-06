import sys
import os
import asyncio
import aiohttp
import pandas as pd
import re
import time
import json
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
logger = setup_logger("02_ft_fees_fixed_cols")
CONCURRENCY = 50
BATCH_SIZE = 100

class FTFeesScraper:
    def __init__(self):
        self.start_time = time.time()
        
        # Output Path
        self.output_dir = project_root / "validation_output" / "Financial_Times" / "03_Detail_Static"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "ft_fund_fees.csv"
        
        # 1. Load All Tickers
        logger.info("📡 Fetching Active Tickers...")
        all_tickers = get_active_tickers("Financial Times") 
        logger.info(f"📋 Total Active Tickers: {len(all_tickers)}")
        
        # 2. Resume Logic
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
        cleaned = re.sub(r'\s+', ' ', text).strip()
        for stopper in ["As of", "As at", "Data as of"]:
            if stopper in cleaned:
                cleaned = cleaned.split(stopper)[0].strip()
        return cleaned

    def _extract_table_value(self, soup, label_pattern):
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
    # 1. SUMMARY PAGE (Get Name + Fees + AUM)
    # =========================================================================
    async def get_summary_data(self, session, ticker):
        url = f"https://markets.ft.com/data/funds/tearsheet/summary?s={ticker}"
        data = {
            'name': None,
            'expense_ratio': None,
            'initial_charge': None,
            'exit_charge': None,
            'assets_aum': None
        }
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200: return data
                html = await response.text()
                soup = BeautifulSoup(html, 'lxml')
                
                # --- Get Name ---
                header = soup.select_one('h1.mod-tearsheet-overview__header__name')
                if header: data['name'] = self._clean_text(header.text)

                # --- Get Fees & AUM ---
                data['expense_ratio'] = self._extract_table_value(soup, r'Ongoing charge|Net Expense Ratio|Expense Ratio')
                data['initial_charge'] = self._extract_table_value(soup, r'Initial charge|Entry charge')
                data['exit_charge'] = self._extract_table_value(soup, r'Exit charge|Redemption charge')
                
                aum = self._extract_table_value(soup, r'Fund size')
                if not aum: aum = self._extract_table_value(soup, r'Share class size')
                if not aum: aum = self._extract_table_value(soup, r'Total Net Assets|Net Assets')
                data['assets_aum'] = aum
        except: pass
        return data

    # =========================================================================
    # 2. HOLDINGS PAGE
    # =========================================================================
    async def get_holdings_data(self, session, ticker):
        url = f"https://markets.ft.com/data/funds/tearsheet/holdings?s={ticker}"
        data = {'top_10_hold_pct': None, 'holdings_count': None}
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200: return data
                html = await response.text()
                soup = BeautifulSoup(html, 'lxml')
                
                module = soup.find('div', attrs={'data-module-name': 'TopHoldingsApp'})
                if module:
                    # Count Rows
                    tables = module.find_all('table')
                    target_table = None
                    for tbl in tables:
                        headers = [th.get_text(strip=True).lower() for th in tbl.find_all('th')]
                        if any(x in headers for x in ['company', 'security', 'portfolio weight', 'weight']):
                            target_table = tbl
                            break
                    if target_table:
                        rows = target_table.find_all('tr')
                        data['holdings_count'] = max(0, len(rows) - 1)

                    # Get Percent from JSON
                    config_str = module.get('data-mod-config')
                    if config_str:
                        try:
                            config = json.loads(config_str)
                            if isinstance(config, list) and len(config) > 0:
                                items = config[0]
                                for item in items:
                                    label = item.get('Label', '')
                                    pct = item.get('Percent')
                                    if pct and float(pct) > 0:
                                        if "Net Assets" in label or "Top" in label:
                                            data['top_10_hold_pct'] = f"{float(pct):.2f}%"
                                            break
                                if not data['top_10_hold_pct'] and items and items[0].get('Percent'):
                                     data['top_10_hold_pct'] = f"{float(items[0].get('Percent')):.2f}%"
                        except: pass

                    # Get Percent from Footer (Backup)
                    if not data['top_10_hold_pct']:
                        footer_td = module.find('td', string=re.compile(r'Per cent', re.IGNORECASE))
                        if not footer_td:
                            for td in module.find_all('td'):
                                if "Per cent" in td.get_text():
                                    footer_td = td; break
                        if footer_td:
                            match = re.search(r'(\d{1,3}\.\d{2}%)', footer_td.get_text())
                            if match: data['top_10_hold_pct'] = match.group(1)
        except: pass
        return data

    async def process_ticker(self, session, item, semaphore):
        ticker = item['ticker']
        asset_type = item['asset_type'] 
        
        async with semaphore:
            summary_res, holdings_res = await asyncio.gather(
                self.get_summary_data(session, ticker),
                self.get_holdings_data(session, ticker)
            )
            
            
            result = {
                "ticker": ticker,
                "asset_type": asset_type,
                "source": "Financial Times",
                "name": summary_res.get('name'),  
                **summary_res,  
                **holdings_res  
            }
            
            
            return result

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
        
        
        desired_order = [
            'ticker', 'asset_type', 'source', 'name',
            'expense_ratio', 'initial_charge', 'exit_charge', 
            'assets_aum', 'top_10_hold_pct', 'holdings_count'
        ]
        
        for col in desired_order:
            if col not in df.columns: df[col] = None
            
        final_cols = [c for c in desired_order if c in df.columns]
        df = df[final_cols]
        
        use_header = not self.output_file.exists()
        df.to_csv(self.output_file, mode='a', header=use_header, index=False)

    async def run(self):
        if not self.tickers:
            logger.info("🎉 No new tickers to scrape.")
            return

        logger.info(f"🚀 Starting FT Fees Scraper (Fixed Cols + Force Save)")
        logger.info(f"💾 Saving to {self.output_file}")
        
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

async def main():
    scraper = FTFeesScraper()
    await scraper.run()
    logger.info(f"✅ Finished! Total Records: {scraper.total_success}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())