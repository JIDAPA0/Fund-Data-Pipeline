import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from bs4 import FeatureNotFound
import time
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import re
import random 
import os
import json 
import sys
from tqdm.asyncio import tqdm 

# ==========================================
# SYSTEM PATH SETUP
# ==========================================
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parents[1]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# ==========================================
# IMPORTS
# ==========================================
from src.utils.path_manager import get_validation_path
from src.utils.logger import setup_logger, log_execution_summary
from src.utils.browser_utils import (
    get_random_user_agent, 
    human_mouse_move, 
    human_sleep, 
    mimic_reading
)

# ==========================================
# CONFIGURATION
# ==========================================
logger = setup_logger("01_master_YF")

LONG_TIMEOUT = 45000            
ITEMS_PER_PAGE = 100            
MAX_PAGES_TO_CHECK = 100 
CONCURRENT_LIMIT = 4            
CSV_HEADERS = ['ticker', 'asset_type', 'name', 'status', 'source', 'date_added']

# ==========================================
# TARGET URLS
# ==========================================
ETF_MARKETS_URLS = {
    "Most_Active": "https://finance.yahoo.com/markets/etfs/most-active/",
    "Top_Gainers": "https://finance.yahoo.com/markets/etfs/gainers/",
    "Top_Losers": "https://finance.yahoo.com/markets/etfs/losers/",
    "Top_Performing": "https://finance.yahoo.com/markets/etfs/top-performing/",
    "Trending": "https://finance.yahoo.com/markets/etfs/trending/",
    "Best_Historical_Performance": "https://finance.yahoo.com/markets/etfs/best-historical-performance/",
}

MUTUAL_FUND_MARKETS_URLS = {
    "Top_Mutual_Funds": "https://finance.yahoo.com/markets/mutualfunds/most-active/",
    "Top_Gainers": "https://finance.yahoo.com/markets/mutualfunds/gainers/",
    "Top_Losers": "https://finance.yahoo.com/markets/mutualfunds/losers/",
    "Top_Performing": "https://finance.yahoo.com/markets/mutualfunds/top-performing/",
    "Trending": "https://finance.yahoo.com/markets/mutualfunds/trending/",
    "Best_Historical_Performance": "https://finance.yahoo.com/markets/mutualfunds/best-historical-performance/",
    "High_Yield": "https://finance.yahoo.com/markets/mutualfunds/high-yield/",
}

# ==========================================
# UTILITIES
# ==========================================
async def dismiss_popups(page):
    try: await page.keyboard.press("Escape"); await asyncio.sleep(0.2)
    except: pass
    try:
        close_selector = 'button[aria-label="Close"], button.close, div.ox-close, button:has-text("Maybe later"), button:has-text("No thanks"), button[name="agree"]'
        if await page.locator(close_selector).count() > 0:
            await page.locator(close_selector).first.click(force=True)
    except: pass

async def get_website_total_count(page) -> int:
    try:
        content = await page.content()
        soup = BeautifulSoup(content, 'lxml')
        matches = soup.find_all(string=re.compile(r"of\s+[\d,]+\s+results"))
        for text in matches:
            numbers = re.findall(r'of\s+([\d,]+)\s+results', text)
            if numbers:
                return int(numbers[0].replace(',', ''))
        return 0
    except Exception: return 0

def build_soup(content: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(content, 'lxml')
    except FeatureNotFound:
        return BeautifulSoup(content, 'html.parser')

async def repair_missing_name(browser, ticker):
    try:
        context = await browser.new_context(user_agent=get_random_user_agent())
        page = await context.new_page()
        url = f"https://finance.yahoo.com/quote/{ticker}"
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        
        name = "N/A"
        try:
            h1 = await page.locator("h1").first.text_content()
            if h1:
                name = h1.replace(f"{ticker} - ", "").strip()
        except:
            pass
            
        await context.close()
        return name
    except:
        return "N/A"

# ==========================================
# DATA PARSING
# ==========================================
def extract_full_table_data(soup: BeautifulSoup, asset_type_label: str) -> List[Dict[str, str]]:
    extracted_data = []
    rows = soup.select('table tbody tr')
    if not rows: rows = soup.select('div[data-testid="list-item"]')
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    for row in rows:
        ticker_text = ""
        name_text = ""
        
        link = row.find('a', href=lambda x: x and '/quote/' in x)
        if link:
            candidate_ticker = link.get_text(strip=True).split(' ')[0]
            if not candidate_ticker:
                href_parts = link['href'].split('/quote/')
                if len(href_parts) > 1:
                    candidate_ticker = href_parts[1].split('?')[0].split('/')[0]
            ticker_text = candidate_ticker
            
            try:
                if link.get('title'):
                    name_text = link.get('title').strip()
                elif row.find('span', title=True):
                    name_text = row.find('span', title=True).get_text(strip=True)
                else:
                    cols = row.find_all(['td', 'div'], recursive=False)
                    if len(cols) > 1: 
                        name_text = cols[1].get_text(strip=True)
            except: 
                name_text = "N/A"

        if (ticker_text and not ticker_text.isdigit() and 1 <= len(ticker_text) < 15 and re.match(r'^[A-Z0-9.\-]+$', ticker_text, re.IGNORECASE)):
            
            if not name_text: name_text = "N/A"

            extracted_data.append({
                'ticker': ticker_text, 
                'asset_type': asset_type_label, 
                'name': name_text, 
                'status': 'new',
                'source': 'Yahoo Finance', 
                'date_added': current_date
            })
    return extracted_data

async def extract_dom_rows(page, asset_type_label: str) -> List[Dict[str, str]]:
    current_date = datetime.now().strftime('%Y-%m-%d')
    rows = await page.eval_on_selector_all(
        'table tbody tr',
        """rows => rows.map(row => {
            const link = row.querySelector('a[href*="/quote/"]');
            let ticker = "";
            let name = "";
            if (link) {
                ticker = (link.textContent || "").trim().split(" ")[0];
                name = (link.getAttribute("title") || "").trim();
            }
            if (!name) {
                const titleSpan = row.querySelector('span[title]');
                if (titleSpan) name = (titleSpan.textContent || "").trim();
            }
            if (!name) {
                const cells = row.querySelectorAll('td, div');
                if (cells.length > 1) name = (cells[1].textContent || "").trim();
            }
            return { ticker, name };
        })"""
    )
    items = []
    for row in rows or []:
        ticker = (row.get("ticker") or "").strip()
        name = (row.get("name") or "").strip() or "N/A"
        if ticker and not ticker.isdigit() and 1 <= len(ticker) < 15 and re.match(r'^[A-Z0-9.\-]+$', ticker, re.IGNORECASE):
            items.append({
                'ticker': ticker,
                'asset_type': asset_type_label,
                'name': name,
                'status': 'new',
                'source': 'Yahoo Finance',
                'date_added': current_date
            })
    return items

# ==========================================
# SCRAPING LOGIC
# ==========================================
async def scrape_single_category(sem, browser, asset_key: str, category_name: str, url_template: str):
    async with sem:
        await human_sleep(0.5, 1.5) 
        asset_label = "ETF" if asset_key == "ETF" else "FUND"
        items = []
        seen_tickers = set() 
        website_total = 0
        
        context = await browser.new_context(
            user_agent=get_random_user_agent(),
            viewport={'width': 1280, 'height': 800}, 
            locale="en-US"
        )
        
        async def route_handler(route):
            if route.request.resource_type in ["image", "media", "font"]:
                await route.abort()
            else:
                await route.continue_()
        await context.route("**/*", route_handler)

        page = await context.new_page()
        
        start_index = 0
        max_limit = MAX_PAGES_TO_CHECK * ITEMS_PER_PAGE
        
        while start_index < max_limit:
            separator = "&" if "?" in url_template else "?"
            url = f"{url_template}{separator}count={ITEMS_PER_PAGE}&start={start_index}"

            retry_count = 0
            success = False
            new_items_in_page = []
            
            while retry_count < 2 and not success:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=LONG_TIMEOUT)
                    try: await page.wait_for_load_state("networkidle", timeout=5000)
                    except: pass

                    try: await page.wait_for_selector('table tbody tr', timeout=8000)
                    except: pass
                    
                    if start_index == 0: 
                        await dismiss_popups(page)
                        website_total = await get_website_total_count(page)
                        if website_total > 0: 
                            logger.info(f"[{category_name} ({asset_label})] Total according to Web: {website_total:,}")
                    
                    await mimic_reading(page, min_sec=0.5, max_sec=1.5)
                    
                    await dismiss_popups(page)
                    content = await page.content()
                    new_items_in_page = extract_full_table_data(build_soup(content), asset_label)
                    if not new_items_in_page:
                        row_count = await page.locator('table tbody tr').count()
                        if row_count > 0:
                            new_items_in_page = await extract_dom_rows(page, asset_label)
                    success = True
                except Exception as e:
                    retry_count += 1
                    logger.debug(f"Retry {retry_count} for {url}: {e}")
                    await asyncio.sleep(1)
            
            if not new_items_in_page: break
            
            added_this_round = 0
            for item in new_items_in_page:
                if item['ticker'] not in seen_tickers:
                    if item['name'] == "N/A" or item['name'] == "":
                        logger.info(f"🔍 Deep scraping name for {item['ticker']}...")
                        real_name = await repair_missing_name(browser, item['ticker'])
                        item['name'] = real_name
                        if real_name != "N/A":
                             logger.info(f"✅ Recovered name: {item['ticker']} -> {real_name}")

                    seen_tickers.add(item['ticker'])
                    items.append(item)
                    added_this_round += 1
            
            if added_this_round == 0: break
                
            start_index += len(new_items_in_page)
            if len(new_items_in_page) < 25: break
            
            await human_sleep(0.5, 1.0)
            
        await context.close()
        return asset_key, category_name, items, website_total

# ==========================================
# MAIN EXECUTION
# ==========================================
async def main():
    logger.info("🚀 STARTING YAHOO FINANCE MASTER SCRAPER")
    start_time = time.time()
    sem = asyncio.Semaphore(CONCURRENT_LIMIT)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        tasks = []
        for cat, url in ETF_MARKETS_URLS.items(): 
            tasks.append(scrape_single_category(sem, browser, "ETF", cat, url))
        for cat, url in MUTUAL_FUND_MARKETS_URLS.items(): 
            tasks.append(scrape_single_category(sem, browser, "Fund", cat, url)) 
        
        results = []
        audit_report = []
        for f in tqdm.as_completed(tasks, desc="Scraping Categories", total=len(tasks)):
            asset_key, cat_name, items, web_total = await f
            results.append((asset_key, cat_name, items))
            
            status = "✅ Complete"
            if web_total > 0 and len(items) < (web_total * 0.9) and len(items) < 2500:
                status = "⚠️ Partial"
            audit_report.append({
                "Category": f"{cat_name} ({asset_key})", 
                "Web": web_total, 
                "Got": len(items), 
                "Status": status
            })
            
        today_str = datetime.now().strftime("%Y-%m-%d")
        total_saved_count = 0
        
        for asset_type in ["ETF", "Fund"]:
            type_items = [item for ak, cn, data in results if ak == asset_type for item in data]
            unique_dict = {item['ticker']: item for item in type_items}
            unique_list = sorted(list(unique_dict.values()), key=lambda x: x['ticker'])
            
            file_path = get_validation_path("Yahoo_Finance", "01_List_Master", f"{today_str}/yf_{asset_type.lower()}_master.csv")
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                writer.writeheader()
                writer.writerows(unique_list)
            
            count = len(unique_list)
            total_saved_count += count
            logger.info(f"🏆 {asset_type} Saved: {count:,} unique tickers to {file_path}")

        print("\n" + "="*70)
        print(f"{'Category':<40} | {'Web':>8} | {'Got':>8} | {'Status'}")
        print("-" * 70)
        for r in sorted(audit_report, key=lambda x: x['Category']):
            print(f"{r['Category']:<40} | {r['Web']:>8,d} | {r['Got']:>8,d} | {r['Status']}")
        print("="*70)

        await browser.close()
        
        log_execution_summary(
            logger,
            start_time=start_time,
            total_items=total_saved_count,
            status="Completed",
            extra_info={"Categories": len(audit_report)}
        )

if __name__ == '__main__':
    asyncio.run(main())
