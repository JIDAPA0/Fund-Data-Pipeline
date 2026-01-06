import asyncio
import aiohttp
import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import re
import string
import itertools
from datetime import datetime
from pathlib import Path
import sys
import os

# Setup System Path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[1]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.path_manager import get_validation_path
from src.utils.logger import setup_logger, log_execution_summary
from src.utils.browser_utils import get_random_headers

# Setup Logging
logger = setup_logger("01_master_FT") 

# Configuration
MAX_RETRIES = 5               
BACKOFF_FACTOR = 2            
CONCURRENT_LIMIT = 50         
ITEMS_PER_PAGE_FUNDS = 10   
API_URL_FUNDS = "https://markets.ft.com/data/funds/ajax/update-screener-results"

# ----------------------------------------------------------------------
# PART 1: MUTUAL FUNDS
# ----------------------------------------------------------------------

async def fetch_fund_page(session, page_num, payload_params, sem):
    async with sem:
        scrape_date = datetime.now().strftime("%Y-%m-%d")
        payload = {
            "page": page_num,
            "itemsPerPage": ITEMS_PER_PAGE_FUNDS,
            "params": payload_params
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                headers = get_random_headers()
                headers.update({
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
                })
                
                async with session.post(API_URL_FUNDS, data=payload, headers=headers, timeout=20) as response:
                    if response.status == 200:
                        json_data = await response.json()
                        if 'html' in json_data and json_data['html']:
                            soup = BeautifulSoup(json_data['html'], 'html.parser')
                            rows = soup.find_all('tr')
                            results = []
                            for row in rows:
                                cols = row.find_all('td')
                                if not cols: continue
                                try:
                                    name_tag = cols[0].find('a')
                                    if name_tag:
                                        name = name_tag.text.strip()
                                        fund_link = "https://markets.ft.com" + name_tag['href']
                                        
                                        raw_ticker = fund_link.split('s=')[-1] if 's=' in fund_link else ""
                                        ticker = raw_ticker.split(':')[0]
                                        
                                        if ticker and name:
                                            results.append([
                                                ticker, 
                                                "FUND",
                                                name, 
                                                "new",
                                                "Financial Times",
                                                scrape_date
                                            ])
                                        else:
                                            logger.warning(f"⚠️ Missing data on page {page_num}: Name='{name}', Ticker='{ticker}'")
                                except Exception as e:
                                    logger.error(f"❌ Error parsing row on page {page_num}: {e}")
                                    continue
                            return results
                    
                    wait_time = BACKOFF_FACTOR ** (attempt + 1) + random.random()
                    await asyncio.sleep(wait_time)
                    
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"❌ Page {page_num}: Failed after {MAX_RETRIES} attempts. Error: {e}")
                    return []
                wait_time = BACKOFF_FACTOR ** (attempt + 1) + random.random()
                await asyncio.sleep(wait_time)
        return []

def get_fund_total_count(url):
    try:
        response = requests.get(url, headers=get_random_headers(), timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        res = soup.find(['h2', 'div', 'span'], string=lambda t: t and 'total results' in t.lower())
        if res:
            nums = re.findall(r'[\d,]+', res.text)
            if nums: return int(nums[-1].replace(',', ''))
    except Exception as e:
        logger.error(f"❌ Failed to get total count: {e}")
        return 0
    return 0 

async def run_fund_scraper():
    logger.info("🚀 STARTING PART 1: MUTUAL FUNDS")
    main_url = "https://markets.ft.com/data/funds/uk/results"
    payload_params = "r:f|c:GBR"
    
    website_total = get_fund_total_count(main_url)
    if website_total == 0:
        logger.error("❌ Could not get total count. Aborting Part 1.")
        return 0
        
    total_pages = (website_total + (ITEMS_PER_PAGE_FUNDS - 1)) // ITEMS_PER_PAGE_FUNDS
    logger.info(f"[Funds] Total Items: {website_total:,} | Total Pages: {total_pages:,}")
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    csv_path = get_validation_path("Financial_Times", "01_List_Master", f"{today_str}/ft_funds_master.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    sem = asyncio.Semaphore(CONCURRENT_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_fund_page(session, p, payload_params, sem) for p in range(1, total_pages + 1)]
        
        all_results = []
        for i, task in enumerate(asyncio.as_completed(tasks)):
            res = await task
            all_results.extend(res)
            if (i + 1) % 100 == 0:
                logger.info(f"[Funds] Progress: {i+1}/{total_pages} pages processed...")

    unique_data = {item[0]: item for item in all_results}
    final_list = sorted(unique_data.values(), key=lambda x: x[0])

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ticker', 'asset_type', 'name', 'status', 'source', 'date_added'])
        writer.writerows(final_list)

    logger.info(f"✅ Finished Funds | Saved {len(final_list):,} unique items")
    return len(final_list)

# ----------------------------------------------------------------------
# PART 2: ETFs
# ----------------------------------------------------------------------

async def fetch_etf_search(session, query, seen_symbols, sem):
    url = f"https://markets.ft.com/data/search?query={query}&assetClass=ETF"
    async with sem:
        for attempt in range(3): 
            try:
                async with session.get(url, headers=get_random_headers(), timeout=15) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        rows = soup.select('table[class*="mod-ui-table"] tbody tr')
                        
                        results = []
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 3:
                                name = cols[0].get_text(strip=True)
                                full_ticker = cols[1].get_text(strip=True)
                                parts = full_ticker.split(':')
                                currency = parts[2] if len(parts) > 2 else "N/A"
                                exchange = parts[1] if len(parts) > 1 else "N/A"

                                if full_ticker not in seen_symbols:
                                    results.append([
                                        full_ticker, 
                                        "ETF",
                                        name, 
                                        currency, 
                                        exchange, 
                                        "new",
                                        "Financial Times"
                                    ])
                                    seen_symbols.add(full_ticker)
                        return results
            except:
                await asyncio.sleep(1)
        return []

async def run_etf_fast_search():
    logger.info("🚀 STARTING PART 2: GLOBAL ETFs")
    today_str = datetime.now().strftime("%Y-%m-%d")
    csv_path = get_validation_path("Financial_Times", "01_List_Master", f"{today_str}/ft_etfs_master.csv")
    
    queries = list(string.ascii_lowercase) + [''.join(p) for p in itertools.product(string.ascii_lowercase, repeat=2)] + [str(i) for i in range(100)]
    random.shuffle(queries)

    seen_symbols = set()
    all_results = []
    sem = asyncio.Semaphore(50)
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_etf_search(session, q, seen_symbols, sem) for q in queries]
        res_lists = await asyncio.gather(*tasks)
        for r in res_lists: all_results.extend(r)

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['full_ticker', 'asset_type', 'name', 'currency', 'exchange', 'status', 'source', 'date_added'])
        for row in all_results:
            writer.writerow(row + [today_str])

    logger.info(f"✅ Finished ETFs | Saved {len(all_results):,} unique items")
    return len(all_results)

# ----------------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------------

async def main():
    start_time = time.time()
    
    funds_count = await run_fund_scraper()
    etfs_count = await run_etf_fast_search()
    
    log_execution_summary(
        logger,
        start_time=start_time,
        total_items=funds_count + etfs_count,
        status="Completed",
        extra_info={"Funds": funds_count, "ETFs": etfs_count}
    )

if __name__ == "__main__":
    asyncio.run(main())