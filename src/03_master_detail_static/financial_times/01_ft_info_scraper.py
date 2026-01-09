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

logger = setup_logger("01_ft_info_slow_sure_fix")
CONCURRENCY = 5  
BATCH_SIZE = 50 


def _get_max_tickers(value):
    try:
        limit = int(value)
        return limit if limit > 0 else None
    except (TypeError, ValueError):
        return None


class FTInfoScraper:
    def __init__(self, max_tickers=None):
        self.start_time = time.time()
        self.output_dir = project_root / "validation_output" / "Financial_Times" / "03_Detail_Static"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / "ft_fund_info.csv"
        
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

    def _parse_date(self, text):
        """
        แปลงวันที่จากรูปแบบต่างๆ เช่น:
        - "As of Jan 07 2026"
        - "Data delayed at least 15 minutes, as of Jan 07 2026."
        ให้เป็น "2026-01-07"
        """
        if not text: return None
        try:
            # ทำให้เป็นตัวเล็กเพื่อหาง่าย แล้วตัดเอาเฉพาะหลังคำว่า "as of"
            lower_text = text.lower()
            if "as of" in lower_text:
                clean = lower_text.split("as of")[-1].strip()
                clean = clean.replace('.', '') # ลบจุดท้ายประโยค
                
                # แปลงกลับเป็น Title case (jan -> Jan) เพื่อให้ strptime อ่านออก
                dt = datetime.strptime(clean.title(), "%b %d %Y")
                return dt.strftime("%Y-%m-%d")
            
            # กรณีไม่มี as of ลองแปลงตรงๆ (เผื่อไว้)
            clean = re.sub(r'As of\s+', '', text).strip().split('.')[0]
            dt = datetime.strptime(clean, "%b %d %Y")
            return dt.strftime("%Y-%m-%d")
        except: 
            return text # ถ้าแปลงไม่ได้ให้คืนค่าเดิม

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

    async def fetch_page(self, session, url):
        for attempt in range(3):
            try:
                async with session.get(url, headers=get_random_headers(), timeout=15) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 404:
                        return None
                    elif response.status == 429:
                        await asyncio.sleep(5 * (attempt + 1))
            except:
                await asyncio.sleep(2)
        return None

    async def get_summary_data(self, session, ticker, url_type):
        url = f"https://markets.ft.com/data/{url_type}/tearsheet/summary?s={ticker}"
        html = await self.fetch_page(session, url)
        
        if not html: return None

        soup = BeautifulSoup(html, 'html.parser')
        data = {}
        
        header = soup.select_one('h1.mod-tearsheet-overview__header__name')
        if not header: return None
        
        data['name'] = self._clean_text(header.text)
        
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
        
        inv_style_raw = self._extract_table_value(soup, r'Investment style')
        data['market_cap_size'] = None
        data['investment_style'] = None
        
        if inv_style_raw:
            mc_match = re.search(r'Market Cap:\s*(.*?)(?=\s*Investment Style|$)', inv_style_raw, re.IGNORECASE)
            st_match = re.search(r'Investment Style:\s*(.*)', inv_style_raw, re.IGNORECASE)
            if mc_match: data['market_cap_size'] = self._clean_text(mc_match.group(1))
            if st_match: data['investment_style'] = self._clean_text(st_match.group(1))

        footer = soup.find('div', class_=re.compile(r'mod-disclaimer'))
        if footer:
            data['updated_at'] = self._parse_date(self._clean_text(footer.text))
        else:
            data['updated_at'] = None

        return data

    async def get_region_data(self, session, ticker, url_type):
        url = f"https://markets.ft.com/data/{url_type}/tearsheet/holdings?s={ticker}"
        html = await self.fetch_page(session, url)
        if not html: return None
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # ค้นหา Header Geographical breakdown หรือ Asset allocation
            header = soup.find(string=re.compile(r'Geographical breakdown|Asset allocation', re.IGNORECASE))
            
            if header:
                parent = header.find_parent('div')
                if parent:
                    table = parent.find_next('table')
                    if table:
                        rows = table.find('tbody').find_all('tr')
                        data_parts = []
                        
                        # วนลูปเก็บข้อมูลทุกแถว
                        for row in rows:
                            cols = row.find_all('td')
                            # ต้องมีอย่างน้อย 2 คอลัมน์: ชื่อ Region และ %
                            if len(cols) >= 2:
                                region_name = self._clean_text(cols[0].text)
                                percentage = self._clean_text(cols[1].text)
                                
                                if region_name and percentage:
                                    # เก็บรูปแบบ "ชื่อ: %"
                                    data_parts.append(f"{region_name}: {percentage}")
                        
                        # ถ้ามีข้อมูล ให้รวมกันด้วย " | " ส่งกลับเป็น String เดียว
                        if data_parts:
                            return " | ".join(data_parts)
                            
        except Exception: 
            pass 
            
        return None

    async def process_ticker(self, session, item, semaphore):
        ticker = item['ticker']
        db_asset_type = item['asset_type']
        
        primary_type = 'etfs' if 'ETF' in str(db_asset_type).upper() else 'funds'
        secondary_type = 'funds' if primary_type == 'etfs' else 'etfs'
        
        async with semaphore:
            summary_data = await self.get_summary_data(session, ticker, primary_type)
            final_type = primary_type
            
            if not summary_data:
                summary_data = await self.get_summary_data(session, ticker, secondary_type)
                final_type = secondary_type
            
            region = await self.get_region_data(session, ticker, final_type)

            if summary_data is None: summary_data = {}

            return {
                "ticker": ticker,
                "asset_type": db_asset_type,
                "source": "Financial Times",
                "name": summary_data.get('name'),
                "isin_number": summary_data.get('isin_number'),
                "category": summary_data.get('category'),
                "inception_date": summary_data.get('inception_date'),
                "exchange": summary_data.get('exchange'),
                "region": region, 
                "country": summary_data.get('country'),
                "market_cap_size": summary_data.get('market_cap_size'),
                "investment_style": summary_data.get('investment_style'),
                "updated_at": summary_data.get('updated_at')
            }

    async def scrape_batch(self, batch_tickers):
        async with aiohttp.ClientSession() as session:
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = [self.process_ticker(session, t, sem) for t in batch_tickers]
            results = await asyncio.gather(*tasks)
            return results

    def save_incremental(self, results):
        if not results: return
        df = pd.DataFrame(results)
        
        desired_order = [
            'ticker', 'asset_type', 'source', 'name', 'isin_number', 
            'category', 'inception_date', 'exchange', 
            'region', 'country', 'market_cap_size', 'investment_style',
            'updated_at'
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

        logger.info(f"🚀 Starting Slow & Sure Scraper (Fixed)")
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
            
            await asyncio.sleep(2)

async def main(max_tickers=None):
    scraper = FTInfoScraper(max_tickers=max_tickers)
    await scraper.run()
    logger.info(f"✅ Finished! Total Saved: {scraper.total_success}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = argparse.ArgumentParser(description="FT Info Scraper")
    parser.add_argument("--max-tickers", type=int, help="Limit number of tickers to process")
    args = parser.parse_args()
    asyncio.run(main(max_tickers=args.max_tickers))
