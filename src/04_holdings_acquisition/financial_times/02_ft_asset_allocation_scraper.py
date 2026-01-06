import sys
import os
import asyncio
import aiohttp
import pandas as pd
import re
import time
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path

current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[2]
if str(project_root) not in sys.path: sys.path.append(str(project_root))

from src.utils.logger import setup_logger
from src.utils.browser_utils import get_random_headers
from src.utils.db_connector import get_active_tickers

logger = setup_logger("02_ft_asset_alloc")
CONCURRENCY = 5
OUTPUT_DIR = project_root / "validation_output" / "Financial_Times" / "04_Holdings" / "Asset_Allocation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class FTAssetAllocScraper:
    def __init__(self):
        self.tickers = get_active_tickers("Financial Times")
        logger.info(f"✅ Total Tickers: {len(self.tickers)}")
        self.total_count = len(self.tickers)
        self.processed_count = 0

    def _get_url(self, ticker, asset_type):
        base = 'etfs' if 'ETF' in str(asset_type).upper() else 'funds'
        return f"https://markets.ft.com/data/{base}/tearsheet/holdings?s={ticker}"

    async def fetch(self, session, url):
        try:
            async with session.get(url, headers=get_random_headers(), timeout=15) as response:
                if response.status == 200: return await response.text()
        except: pass
        return None

    def parse(self, html):
        if not html: return [], None
        soup = BeautifulSoup(html, 'lxml')
        data = []
        as_of_date = None
        footer = soup.find(string=re.compile(r'As of\s+[A-Za-z]{3}'))
        if footer: 
            try:
                dt = datetime.strptime(re.sub(r'\.$', '', footer.strip().split('As of ')[1]), "%b %d %Y")
                as_of_date = dt.strftime("%Y-%m-%d")
            except: pass

        for table in soup.find_all('table'):
            headers = [th.text.strip().lower() for th in table.find_all('th')]
            has_type = 'type' in headers
            has_long = any('long' in h for h in headers)
            
            if has_type and has_long:
                idx_net, idx_short, idx_long = -1, -1, -1
                for i, h in enumerate(headers):
                    if 'net assets' in h or 'weight' in h: idx_net = i
                    if 'short' in h: idx_short = i
                    if 'long' in h: idx_long = i
                
                if idx_net != -1:
                    for row in table.find_all('tr')[1:]:
                        cols = row.find_all('td')
                        if len(cols) >= 4:
                            val_net = cols[idx_net].text.strip().replace('%','').replace(',','')
                            val_short = cols[idx_short].text.strip().replace('%','').replace(',','') if idx_short != -1 else None
                            val_long = cols[idx_long].text.strip().replace('%','').replace(',','') if idx_long != -1 else None
                            if val_net and val_net != '--':
                                data.append({
                                    'allocation_type': 'asset_class',
                                    'item_name': cols[0].text.strip(),
                                    'value_net': val_net,
                                    'value_short': val_short,
                                    'value_long': val_long
                                })
                    if data: break
        return data, as_of_date

    async def process(self, session, item, sem):
        ticker, atype = item['ticker'], item['asset_type']
        self.processed_count += 1
        print(f"[{self.processed_count}/{self.total_count}] Checking: {ticker} ...", end='\r')

        
        safe_ticker = ticker.replace(':', '_').replace('/', '_')
        fname = OUTPUT_DIR / f"{safe_ticker}_{atype}_asset_alloc.csv"
        
        if fname.exists(): return None

        async with sem:
            html = await self.fetch(session, self._get_url(ticker, atype))
            rows, date = self.parse(html)
            if not rows: return None
            
            final = []
            for r in rows:
                final.append({'ticker': ticker, 'asset_type': atype, 'source': 'Financial Times', 'as_of_date': date, **r})
            
            df = pd.DataFrame(final)
            df.to_csv(fname, index=False)
            logger.info(f"💾 Saved: {ticker}")
            return 1

    async def run(self):
        connector = aiohttp.TCPConnector(limit=CONCURRENCY)
        async with aiohttp.ClientSession(connector=connector) as session:
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = [self.process(session, t, sem) for t in self.tickers]
            results = await asyncio.gather(*tasks)
            saved = sum(1 for r in results if r)
            logger.info(f"\n🎉 Finished! Saved {saved} files.")

if __name__ == "__main__":
    if sys.platform == 'win32': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(FTAssetAllocScraper().run())