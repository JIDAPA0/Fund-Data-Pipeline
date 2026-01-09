import sys
import os
import asyncio
import csv
import pandas as pd
import re
import time
import math
import random
import argparse
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
logger = setup_logger("01_yf_holdings_master")
CONCURRENCY = 3
BATCH_SIZE = 20

# Base Output Directory
BASE_OUTPUT_DIR = project_root / "validation_output" / "Yahoo_Finance" / "04_Holdings"
DIR_HOLDINGS = BASE_OUTPUT_DIR / "Holdings"
DIR_SECTORS = BASE_OUTPUT_DIR / "Sectors"
DIR_ALLOCATION = BASE_OUTPUT_DIR / "Allocation"
DIR_BOND_RATINGS = BASE_OUTPUT_DIR / "Bond_Ratings"
DIR_EQUITY_HOLDINGS = BASE_OUTPUT_DIR / "Equity_Holdings"
DIR_BOND_HOLDINGS = BASE_OUTPUT_DIR / "Bond_Holdings"


MISSING_REPORT_FILE = BASE_OUTPUT_DIR / "yf_holdings_missing_report.csv"

# Create Directories
for d in [
    DIR_HOLDINGS,
    DIR_SECTORS,
    DIR_ALLOCATION,
    DIR_BOND_RATINGS,
    DIR_EQUITY_HOLDINGS,
    DIR_BOND_HOLDINGS,
]:
    d.mkdir(parents=True, exist_ok=True)

class YFHoldingsScraper:
    def __init__(self, tickers=None, default_asset_type="Fund"):
        self.start_time = time.time()
        
        if tickers:
            normalized = []
            for item in tickers:
                if isinstance(item, dict):
                    ticker = item.get("ticker")
                    asset_type = item.get("asset_type") or default_asset_type
                else:
                    ticker = str(item)
                    asset_type = default_asset_type
                if ticker:
                    normalized.append({"ticker": ticker, "asset_type": asset_type})
            self.tickers = normalized
            logger.info(f"✅ Using provided tickers: {len(self.tickers)}")
        else:
            logger.info("📡 Fetching Active Tickers from DB...")
            self.tickers = get_active_tickers("Yahoo Finance") 
            logger.info(f"✅ Total Tickers to Process: {len(self.tickers)}")
        
        self.total_processed = 0
        self.total_success = 0
        
        # User Agents
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
        ]

        
        if not MISSING_REPORT_FILE.exists():
            pd.DataFrame(columns=["ticker", "asset_type", "reason", "timestamp"]).to_csv(MISSING_REPORT_FILE, index=False)

    def get_random_ua(self):
        return random.choice(self.user_agents)

    def _clean_text(self, text):
        if text is None:
            return ""
        return re.sub(r"\s+", " ", str(text).replace("\xa0", " ")).strip()

    def _looks_like_symbol(self, text):
        return bool(re.match(r"^[A-Z0-9.-]{1,12}$", str(text).strip()))

    async def _extract_table(self, section):
        try:
            if await section.count() == 0:
                return None
            table = section.locator("table").first
            if await table.count() == 0:
                return None
            return await table.evaluate(
                """
                table => {
                    const headers = Array.from(table.querySelectorAll('thead th'))
                        .map(th => th.innerText.trim())
                        .filter(h => h.length > 0);
                    const rows = Array.from(table.querySelectorAll('tbody tr')).map(tr =>
                        Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim())
                    );
                    return { headers, rows };
                }
                """
            )
        except Exception:
            return None

    async def _extract_content_blocks(self, section):
        rows = []
        try:
            if await section.count() == 0:
                return rows
            blocks = section.locator('div[class*="content"]')
            cnt = await blocks.count()
            for i in range(cnt):
                raw = await blocks.nth(i).inner_text()
                parts = [self._clean_text(p) for p in raw.split("\n")]
                parts = [p for p in parts if p]
                if parts:
                    rows.append(parts)
        except Exception:
            return rows
        return rows

    def _expand_metric_table(self, table_data):
        out = []
        if not table_data:
            return out
        headers = [self._clean_text(h) for h in table_data.get("headers", []) if self._clean_text(h)]
        rows = table_data.get("rows", [])
        for row in rows:
            row = [self._clean_text(c) for c in row if c is not None]
            if len(row) < 2:
                continue
            metric = row[0]
            values = row[1:]
            if headers:
                if len(headers) == len(row):
                    value_headers = headers[1:]
                elif len(headers) == len(values):
                    value_headers = headers
                else:
                    value_headers = [f"value_{i+1}" for i in range(len(values))]
            else:
                value_headers = [f"value_{i+1}" for i in range(len(values))]

            for col_name, val in zip(value_headers, values):
                out.append({"metric": metric, "column_name": col_name, "value": val})
        return out

    async def log_missing(self, ticker, asset_type, reason):
        try:
            df = pd.DataFrame([{
                "ticker": ticker,
                "asset_type": asset_type,
                "reason": reason,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }])
            df.to_csv(MISSING_REPORT_FILE, mode='a', header=False, index=False)
        except: pass

    async def dismiss_popups(self, page):
        try:
            await page.keyboard.press("Escape")
            selectors = [
                'button[name="reject"]', 'button[name="agree"]', 'button[value="agree"]',
                'button[aria-label="Close"]', 'button.close', 'div.ox-close', 
                '#consent-page button.reject', 'button:has-text("Maybe later")', 
                'button:has-text("Not now")'
            ]
            for sel in selectors:
                if await page.locator(sel).count() > 0:
                    try: await page.locator(sel).first.click(force=True, timeout=500)
                    except: pass
        except: pass

    async def search_fallback(self, page, ticker):
        try:
            search_box = page.locator('#ybar-sbq')
            if await search_box.count() > 0:
                await search_box.fill(ticker)
                await page.keyboard.press("Enter")
                try: await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except: pass
                
                if "/quote/" in page.url and "lookup" not in page.url:
                    match = re.search(r'/quote/([^/?]+)', page.url)
                    if match: return match.group(1)
                    return ticker
        except: pass
        return None

    async def process_ticker(self, context, item):
        ticker = item['ticker']
        raw_asset_type = item.get('asset_type', 'Fund')
        if not raw_asset_type: raw_asset_type = 'Fund'
        asset_type = str(raw_asset_type).upper().replace('/', '').replace(' ', '')
        
        safe_ticker = ticker.replace('/', '_').replace(':', '_')
        
        f_hold = DIR_HOLDINGS / f"{safe_ticker}_{asset_type}_holdings.csv"
        f_sect = DIR_SECTORS / f"{safe_ticker}_{asset_type}_sectors.csv"
        f_alloc = DIR_ALLOCATION / f"{safe_ticker}_{asset_type}_allocation.csv"
        f_bond_ratings = DIR_BOND_RATINGS / f"{safe_ticker}_{asset_type}_bond_ratings.csv"
        f_equity_holdings = DIR_EQUITY_HOLDINGS / f"{safe_ticker}_{asset_type}_equity_holdings.csv"
        f_bond_holdings = DIR_BOND_HOLDINGS / f"{safe_ticker}_{asset_type}_bond_holdings.csv"
        
        expected_files = [
            f_hold,
            f_sect,
            f_alloc,
            f_bond_ratings,
            f_equity_holdings,
            f_bond_holdings,
        ]
        if all(f.exists() for f in expected_files):
            return "SKIPPED"

        page = await context.new_page()
        target_ticker = ticker
        url = f"https://finance.yahoo.com/quote/{target_ticker}/holdings/"
        
        data_found = False
        fail_reason = "UNKNOWN"
        page_ok = False
        
        try:
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            
            # --- 1. HANDLE REDIRECT / SEARCH ---
            if "lookup" in page.url:
                new_ticker = await self.search_fallback(page, ticker)
                if new_ticker:
                    target_ticker = new_ticker
                    await page.goto(f"https://finance.yahoo.com/quote/{target_ticker}/holdings/", timeout=60000)
                else:
                    await page.close()
                    await self.log_missing(ticker, asset_type, "INVALID_TICKER (Search Failed)")
                    return "INVALID_TICKER"

            if "lookup" in page.url:
                await page.close()
                await self.log_missing(ticker, asset_type, "INVALID_TICKER (Still Lookup)")
                return "INVALID_TICKER"

            page_ok = True
            await asyncio.sleep(2) 
            await self.dismiss_popups(page)
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            
            # --- 2. SCRAPE DATA ---
            # Top Holdings
            holdings_data = []
            section = page.locator('section[data-testid="top-holdings"]')
            if await section.count() > 0:
                rows = await self._extract_content_blocks(section)
                for parts in rows:
                    if len(parts) < 2:
                        continue
                    value = parts[-1]
                    symbol = "-"
                    name = parts[0]
                    if len(parts) >= 3:
                        first, second = parts[0], parts[1]
                        if self._looks_like_symbol(first) and not self._looks_like_symbol(second):
                            symbol, name = first, second
                        elif self._looks_like_symbol(second) and not self._looks_like_symbol(first):
                            symbol, name = second, first
                        else:
                            symbol, name = first, second
                    elif len(parts) == 2 and self._looks_like_symbol(parts[0]) and "%" in parts[1]:
                        symbol = parts[0]
                    holdings_data.append({'symbol': symbol, 'name': name, 'value': value})

            if not holdings_data:
                tables = page.locator('table')
                cnt_tbl = await tables.count()
                for i in range(cnt_tbl):
                    rows = tables.nth(i).locator('tbody tr')
                    if await rows.count() == 0: continue
                    first_row = await rows.nth(0).inner_text()
                    if "Symbol" in first_row or "% Assets" in first_row:
                        for r in range(await rows.count()):
                            cols = rows.nth(r).locator('td')
                            if await cols.count() >= 3:
                                sym = await cols.nth(0).inner_text()
                                name = await cols.nth(1).inner_text()
                                val = await cols.nth(2).inner_text()
                                holdings_data.append({'symbol': sym, 'name': name, 'value': val})
                        if holdings_data: break

            if holdings_data:
                df = pd.DataFrame(holdings_data)
                df['ticker'] = ticker
                df['yahoo_ticker'] = target_ticker
                df['asset_type'] = asset_type
                df['updated_at'] = datetime.now().strftime('%Y-%m-%d')
                df.to_csv(f_hold, index=False)
                data_found = True

            # Sector Weightings
            sector_data = []
            sec_section = page.locator('section[data-testid*="sector-weightings"]')
            if await sec_section.count() > 0:
                rows = await self._extract_content_blocks(sec_section)
                for parts in rows:
                    if len(parts) >= 2:
                        sector_data.append({'sector': parts[0], 'value': parts[-1]})
            
            if sector_data:
                df = pd.DataFrame(sector_data)
                df['ticker'] = ticker
                df['asset_type'] = asset_type
                df['updated_at'] = datetime.now().strftime('%Y-%m-%d')
                df.to_csv(f_sect, index=False)
                data_found = True

            # Overall Portfolio Composition (%)
            alloc_data = []
            alloc_section = page.locator('section[data-testid="portfolio-composition"]')
            alloc_table = await self._extract_table(alloc_section)
            if alloc_table:
                for row in alloc_table.get("rows", []):
                    if len(row) >= 2:
                        alloc_data.append({'category': self._clean_text(row[0]), 'value': self._clean_text(row[1])})

            if alloc_data:
                df = pd.DataFrame(alloc_data)
                df['ticker'] = ticker
                df['asset_type'] = asset_type
                df['updated_at'] = datetime.now().strftime('%Y-%m-%d')
                df.to_csv(f_alloc, index=False)
                data_found = True

            # Bond Ratings
            bond_rating_data = []
            rating_section = page.locator('section[data-testid="bond-ratings"]')
            rating_table = await self._extract_table(rating_section)
            if rating_table:
                for row in rating_table.get("rows", []):
                    if len(row) >= 2:
                        bond_rating_data.append({'rating': self._clean_text(row[0]), 'value': self._clean_text(row[1])})

            if bond_rating_data:
                df = pd.DataFrame(bond_rating_data)
                df['ticker'] = ticker
                df['asset_type'] = asset_type
                df['updated_at'] = datetime.now().strftime('%Y-%m-%d')
                df.to_csv(f_bond_ratings, index=False)
                data_found = True

            # Equity Holdings (metrics)
            equity_holdings_data = []
            equity_section = page.locator('section[data-testid="equity-holdings"]')
            equity_table = await self._extract_table(equity_section)
            if equity_table:
                equity_holdings_data = self._expand_metric_table(equity_table)

            if equity_holdings_data:
                df = pd.DataFrame(equity_holdings_data)
                df['ticker'] = ticker
                df['asset_type'] = asset_type
                df['updated_at'] = datetime.now().strftime('%Y-%m-%d')
                df.to_csv(f_equity_holdings, index=False)
                data_found = True

            # Bond Holdings (metrics)
            bond_holdings_data = []
            bond_section = page.locator('section[data-testid="bond-holdings"]')
            bond_table = await self._extract_table(bond_section)
            if bond_table:
                bond_holdings_data = self._expand_metric_table(bond_table)

            if bond_holdings_data:
                df = pd.DataFrame(bond_holdings_data)
                df['ticker'] = ticker
                df['asset_type'] = asset_type
                df['updated_at'] = datetime.now().strftime('%Y-%m-%d')
                df.to_csv(f_bond_holdings, index=False)
                data_found = True

            # Optional sections: write empty files so we can skip next run
            if page_ok:
                if not f_bond_ratings.exists():
                    pd.DataFrame(columns=["ticker", "asset_type", "updated_at", "rating", "value"]).to_csv(
                        f_bond_ratings, index=False
                    )
                if not f_equity_holdings.exists():
                    pd.DataFrame(columns=["ticker", "asset_type", "updated_at", "metric", "column_name", "value"]).to_csv(
                        f_equity_holdings, index=False
                    )
                if not f_bond_holdings.exists():
                    pd.DataFrame(columns=["ticker", "asset_type", "updated_at", "metric", "column_name", "value"]).to_csv(
                        f_bond_holdings, index=False
                    )

            if not data_found:
                fail_reason = "NO_HOLDINGS_DATA (Page loaded but empty)"
                await self.log_missing(ticker, asset_type, fail_reason)

        except Exception as e:
            fail_reason = f"ERROR: {str(e)[:50]}"
            await self.log_missing(ticker, asset_type, fail_reason)
        finally:
            await page.close()
        
        return "SUCCESS" if data_found else "NO_DATA"

    async def run(self):
        if not self.tickers: return
        logger.info(f"🚀 Starting Yahoo Holdings Scraper (With Missing Report)")
        
        total = len(self.tickers)
        batches = math.ceil(total / BATCH_SIZE)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent=self.get_random_ua()
            )
            
            for i in range(batches):
                start_b = time.time()
                batch = self.tickers[i*BATCH_SIZE : (i+1)*BATCH_SIZE]
                
                tasks = [self.process_ticker(context, t) for t in batch]
                results = await asyncio.gather(*tasks)
                
                success_cnt = results.count("SUCCESS")
                skip_cnt = results.count("SKIPPED")
                self.total_success += success_cnt
                self.total_processed += len(batch)
                
                dur = time.time() - start_b
                logger.info(f"Batch {i+1}/{batches} | Saved: {success_cnt} | Skips: {skip_cnt} | Progress: {self.total_processed}/{total} | Time: {dur:.2f}s")
                
                if (i+1) % 10 == 0:
                    await context.close()
                    context = await browser.new_context(
                        viewport={'width': 1280, 'height': 800},
                        user_agent=self.get_random_ua()
                    )

            await browser.close()
        
        logger.info(f"🎉 Finished! Total Saved: {self.total_success} tickers")
        logger.info(f"📄 Check missing tickers at: {MISSING_REPORT_FILE}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = argparse.ArgumentParser(description="Yahoo Finance Holdings Scraper")
    parser.add_argument(
        "--tickers",
        help="Comma/space separated tickers, optionally TICKER:ASSET_TYPE",
    )
    parser.add_argument(
        "--asset-type",
        default="Fund",
        help="Default asset type for tickers without explicit type",
    )
    args = parser.parse_args()

    def parse_ticker_list(raw, default_asset_type):
        if not raw:
            return None
        items = re.split(r"[,\s]+", raw.strip())
        tickers = []
        for item in items:
            if not item:
                continue
            if ":" in item:
                ticker, asset_type = item.split(":", 1)
                ticker = ticker.strip()
                asset_type = asset_type.strip() or default_asset_type
            else:
                ticker = item.strip()
                asset_type = default_asset_type
            if ticker:
                tickers.append({"ticker": ticker, "asset_type": asset_type})
        return tickers or None

    tickers = parse_ticker_list(args.tickers, args.asset_type)
    asyncio.run(YFHoldingsScraper(tickers=tickers, default_asset_type=args.asset_type).run())
