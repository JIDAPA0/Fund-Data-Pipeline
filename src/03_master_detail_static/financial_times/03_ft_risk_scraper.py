import sys
import os
import asyncio
import aiohttp
import pandas as pd
import re
import time
import math
import argparse
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
logger = setup_logger("03_ft_risk_scraper")
CONCURRENCY = 10     # ปรับความเร็วพอประมาณ
BATCH_SIZE = 50    


def _get_max_tickers(value):
    try:
        limit = int(value)
        return limit if limit > 0 else None
    except (TypeError, ValueError):
        return None


class FTRiskScraper:
    def __init__(self, max_tickers=None):
        self.start_time = time.time()
        self.output_dir = project_root / "validation_output" / "Financial_Times" / "03_Detail_Static"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "ft_fund_risk.csv"
        
        logger.info("📡 Fetching Active Tickers...")
        all_tickers = get_active_tickers("Financial Times") 
        logger.info(f"📋 Total Active Tickers: {len(all_tickers)}")
        
        # Resume Logic
        processed_tickers = set()
        if self.output_file.exists():
            try:
                df_existing = pd.read_csv(self.output_file, usecols=['ticker'])
                processed_tickers = set(df_existing['ticker'].astype(str).tolist())
                logger.info(f"⏭️ Found existing file. Skipping {len(processed_tickers)} rows.")
            except: pass

        self.tickers = [t for t in all_tickers if t['ticker'] not in processed_tickers]
        env_limit = _get_max_tickers(os.getenv("FT_MAX_TICKERS"))
        self.max_tickers = max_tickers or env_limit
        if self.max_tickers:
            self.tickers = self.tickers[: self.max_tickers]
            logger.info("⏱️ Max tickers limit: %s", self.max_tickers)
        logger.info(f"✅ Remaining to Scrape: {len(self.tickers)}")
        
        self.total_processed = 0
        self.total_success = 0

    def _clean_text(self, text):
        if not text: return None
        return re.sub(r'\s+', ' ', text).strip()

    def _extract_val(self, text):
        if not text: return None
        clean = text.strip()
        if clean in ['--', '-', '', 'NA']: return None
        try:
            return clean.replace(',', '').replace('%', '')
        except: return None

    def _parse_date(self, text):
        if not text: return None
        try:
            clean_text = re.sub(r'\.$', '', text.strip())
            # Format: Jan 08 2026
            match = re.search(r'([A-Za-z]{3})\s+(\d{1,2})\s+(\d{4})', clean_text)
            if match:
                date_str = f"{match.group(1)} {match.group(2)} {match.group(3)}"
                dt = datetime.strptime(date_str, "%b %d %Y")
                return dt.strftime("%Y-%m-%d")
        except: pass
        return None

    def _get_base_url(self, asset_type):
        if 'ETF' in str(asset_type).upper():
            return "https://markets.ft.com/data/etfs/tearsheet"
        return "https://markets.ft.com/data/funds/tearsheet"

    async def fetch_page(self, session, url):
        for attempt in range(3):
            try:
                async with session.get(url, headers=get_random_headers(), timeout=20) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 429:
                        await asyncio.sleep(5 * (attempt + 1))
                    else:
                        await asyncio.sleep(1)
            except:
                await asyncio.sleep(1)
        return None

    # =========================================================================
    # 1. RISK PAGE (1y, 3y, 5y, 10y)
    # =========================================================================
    async def get_risk_data(self, session, ticker, asset_type):
        base_url = self._get_base_url(asset_type)
        url = f"{base_url}/risk?s={ticker}"
        
        data = {
            'name': None, 'updated_at': None,
            'sharpe_ratio_1y': None, 'sharpe_ratio_3y': None, 'sharpe_ratio_5y': None, 'sharpe_ratio_10y': None,
            'beta_1y': None, 'beta_3y': None, 'beta_5y': None, 'beta_10y': None,
            'alpha_1y': None, 'alpha_3y': None, 'alpha_5y': None, 'alpha_10y': None,
            'standard_dev_1y': None, 'standard_dev_3y': None, 'standard_dev_5y': None, 'standard_dev_10y': None,
            'r_squared_1y': None, 'r_squared_3y': None, 'r_squared_5y': None, 'r_squared_10y': None
        }
        
        html = await self.fetch_page(session, url)
        if not html: return data

        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            header = soup.select_one('h1.mod-tearsheet-overview__header__name')
            if header: data['name'] = self._clean_text(header.text)

            footer = soup.find(string=re.compile(r'As of\s+[A-Za-z]{3}'))
            if footer: data['updated_at'] = self._parse_date(footer)
            
            panels = soup.find_all('div', class_='mod-ui-tab-content')
            for panel in panels:
                panel_id = panel.get('id', '')
                suffix = None
                if '1y' in panel_id: suffix = '1y'
                elif '3y' in panel_id: suffix = '3y'
                elif '5y' in panel_id: suffix = '5y'
                elif '10y' in panel_id: suffix = '10y' # เพิ่ม 10y ให้ครบตาม DB
                
                if suffix:
                    rows = panel.find_all('tr')
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 2:
                            label = self._clean_text(cols[0].text).lower()
                            val = self._extract_val(cols[1].text)
                            
                            if 'alpha' in label: data[f'alpha_{suffix}'] = val
                            elif 'beta' in label: data[f'beta_{suffix}'] = val
                            elif 'sharpe' in label: data[f'sharpe_ratio_{suffix}'] = val
                            elif 'standard deviation' in label: data[f'standard_dev_{suffix}'] = val
                            elif 'r squared' in label: data[f'r_squared_{suffix}'] = val
        except: pass
        return data

    # =========================================================================
    # 2. RATINGS PAGE
    # =========================================================================
    async def get_ratings_data(self, session, ticker, asset_type):
        base_url = self._get_base_url(asset_type)
        url = f"{base_url}/ratings?s={ticker}"
        
        data = {
            'morningstar_rating': None,
            'lipper_total_return_3y': None, 'lipper_total_return_5y': None, 'lipper_total_return_10y': None, 'lipper_total_return_overall': None,
            'lipper_consistent_return_3y': None, 'lipper_consistent_return_5y': None, 'lipper_consistent_return_10y': None, 'lipper_consistent_return_overall': None,
            'lipper_preservation_3y': None, 'lipper_preservation_5y': None, 'lipper_preservation_10y': None, 'lipper_preservation_overall': None,
            'lipper_expense_3y': None, 'lipper_expense_5y': None, 'lipper_expense_10y': None, 'lipper_expense_overall': None,
            'rating_updated_at': None
        }
        
        html = await self.fetch_page(session, url)
        if not html: return data

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # --- Morningstar ---
            ms_container = soup.find(class_=re.compile(r'morningstar-rating'))
            if ms_container:
                highlighted = ms_container.find('span', attrs={'data-mod-stars-highlighted': 'true'})
                if highlighted:
                    stars = len(highlighted.find_all(class_=re.compile(r'icon--star')))
                else:
                    stars = len(ms_container.find_all(class_=re.compile(r'icon--star')))
                
                if 1 <= stars <= 5:
                    data['morningstar_rating'] = stars
            
            # --- Lipper Leaders ---
            lipper_app = soup.find('div', attrs={'data-module-name': 'LipperRatingApp'})
            if lipper_app:
                footer = lipper_app.find('div', class_=re.compile(r'mod-disclaimer'))
                if footer: data['rating_updated_at'] = self._parse_date(footer.text)

                table = lipper_app.find('table')
                if table:
                    rows = table.find_all('tr')
                    if len(rows) > 0:
                        headers = [th.text.strip().lower() for th in rows[0].find_all('th')]
                        def get_col_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
                        
                        idx_total = get_col_idx('total return')
                        idx_consist = get_col_idx('consistent return')
                        idx_preserv = get_col_idx('preservation')
                        idx_expense = get_col_idx('expense')

                        def get_score(cols, idx):
                            if idx == -1 or idx >= len(cols): return None
                            icon = cols[idx].find('i', class_=re.compile(r'mod-sprite-lipper-'))
                            if icon:
                                for cls in icon.get('class', []):
                                    if 'mod-sprite-lipper-' in cls: 
                                        val = cls.split('-')[-1]
                                        if val.isdigit() and 1 <= int(val) <= 5:
                                            return int(val)
                            return None

                        for row in rows[1:]:
                            cols = row.find_all('td')
                            if not cols: continue
                            label = cols[0].text.strip().lower()
                            suffix = None
                            if 'overall' in label: suffix = 'overall'
                            elif '3 year' in label: suffix = '3y'
                            elif '5 year' in label: suffix = '5y'
                            elif '10 year' in label: suffix = '10y'

                            if suffix:
                                data[f'lipper_total_return_{suffix}'] = get_score(cols, idx_total)
                                data[f'lipper_consistent_return_{suffix}'] = get_score(cols, idx_consist)
                                data[f'lipper_preservation_{suffix}'] = get_score(cols, idx_preserv)
                                data[f'lipper_expense_{suffix}'] = get_score(cols, idx_expense)
        except: pass
        return data

    async def process_ticker(self, session, item, semaphore):
        ticker = item['ticker']
        asset_type = item['asset_type']
        
        async with semaphore:
            risk_res, ratings_res = await asyncio.gather(
                self.get_risk_data(session, ticker, asset_type),
                self.get_ratings_data(session, ticker, asset_type)
            )
            
            final_updated_at = risk_res.get('updated_at')
            if not final_updated_at: final_updated_at = ratings_res.get('rating_updated_at')
            ratings_res.pop('rating_updated_at', None)

            return {
                "ticker": ticker,
                "asset_type": asset_type,
                "source": "Financial Times",
                **risk_res,
                **ratings_res,
                "updated_at": final_updated_at
            }

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
            'ticker', 'asset_type', 'source', 'name', 'updated_at',
            'sharpe_ratio_1y', 'sharpe_ratio_3y', 'sharpe_ratio_5y', 'sharpe_ratio_10y',
            'beta_1y', 'beta_3y', 'beta_5y', 'beta_10y',
            'alpha_1y', 'alpha_3y', 'alpha_5y', 'alpha_10y',
            'standard_dev_1y', 'standard_dev_3y', 'standard_dev_5y', 'standard_dev_10y',
            'r_squared_1y', 'r_squared_3y', 'r_squared_5y', 'r_squared_10y',
            'morningstar_rating',
            'lipper_total_return_3y', 'lipper_total_return_5y', 'lipper_total_return_10y', 'lipper_total_return_overall',
            'lipper_consistent_return_3y', 'lipper_consistent_return_5y', 'lipper_consistent_return_10y', 'lipper_consistent_return_overall',
            'lipper_preservation_3y', 'lipper_preservation_5y', 'lipper_preservation_10y', 'lipper_preservation_overall',
            'lipper_expense_3y', 'lipper_expense_5y', 'lipper_expense_10y', 'lipper_expense_overall'
        ]
        
        for col in valid_cols:
            if col not in df.columns: df[col] = None
            
        df = df[valid_cols]
        
        if 'morningstar_rating' in df.columns:
            df['morningstar_rating'] = pd.to_numeric(df['morningstar_rating'], errors='coerce')
            df['morningstar_rating'] = df['morningstar_rating'].astype('Int64') 

        use_header = not self.output_file.exists()
        df.to_csv(self.output_file, mode='a', header=use_header, index=False)

    async def run(self):
        if not self.tickers: return
        logger.info(f"🚀 Starting FT Risk Scraper (Final + 10Y Support)")
        
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
            
            await asyncio.sleep(2)

async def main(max_tickers=None):
    scraper = FTRiskScraper(max_tickers=max_tickers)
    await scraper.run()
    logger.info(f"✅ Finished! Total Saved: {scraper.total_success}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = argparse.ArgumentParser(description="FT Risk Scraper")
    parser.add_argument("--max-tickers", type=int, help="Limit number of tickers to process")
    args = parser.parse_args()
    asyncio.run(main(max_tickers=args.max_tickers))
