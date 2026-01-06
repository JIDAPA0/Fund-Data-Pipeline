import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime

# กองทุนเป้าหมาย
TEST_TICKER = "LU0385659072:EUR"

async def scrape_regions_smart():
    print(f"🚀 STARTING PLAYWRIGHT (SMART WAIT)...")
    print(f"🎯 Target: {TEST_TICKER}")
    
    url = f"https://markets.ft.com/data/funds/tearsheet/holdings?s={TEST_TICKER}"
    
    async with async_playwright() as p:
        # เปิด Browser (Headless = True คือไม่โชว์หน้าจอ)
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            # 1. ไปที่หน้าเว็บ
            print("1️⃣ Loading Page...")
            await page.goto(url, timeout=60000)
            
            # 2. คลิกปุ่ม Regions (ใช้ Selector ที่แม่นยำที่สุด)
            print("2️⃣ Clicking 'Regions' Tab...")
            # ใช้ force=True เพื่อบังคับกดแม้จะมีอะไรบัง
            await page.click('li[aria-controls="regions-panel"]', force=True)
            
            # 3. 🔥 SMART WAIT: รอจนกว่าจะเจอคำว่า Region/Market/Country ในตาราง
            print("3️⃣ Waiting for 'Regions' Data to appear...")
            
            # Selector นี้หมายความว่า: หา <th> ที่มีคำว่า Region หรือ Market หรือ Country อยู่ข้างใน
            # บอทจะรอตรงนี้สูงสุด 15 วินาที ถ้าไม่มาจะ Error (ดีกว่าได้ข้อมูลผิด)
            try:
                await page.wait_for_selector(
                    '#regions-panel th:has-text("Region"), #regions-panel th:has-text("Market"), #regions-panel th:has-text("Country")', 
                    state='visible', 
                    timeout=15000
                )
            except Exception:
                print("⚠️ Warning: Wait timed out. Data might not be available or slow.")
            
            # 4. ดึง HTML
            # ดึงเฉพาะกล่อง Regions มาเลย
            content_html = await page.inner_html('#regions-panel')
            print("✅ Data Loaded! Parsing...")
            
            # --- PARSING ---
            soup = BeautifulSoup(content_html, 'lxml')
            data = []
            
            tables = soup.find_all('table')
            for table in tables:
                headers = [th.text.strip().lower() for th in table.find_all('th')]
                print(f"   🔎 Found Headers: {headers}")
                
                # เช็คอีกรอบเพื่อความชัวร์
                if any(k in headers for k in ['region', 'market', 'country']):
                    print("   🎉 JACKPOT! Found Regions Table.")
                    
                    idx_net = -1
                    idx_cat = -1
                    for i, h in enumerate(headers):
                        if 'net assets' in h: idx_net = i
                        if 'category' in h: idx_cat = i
                    
                    if idx_net != -1:
                        rows = table.find_all('tr')
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) > idx_net:
                                name = cols[0].text.strip()
                                val = cols[idx_net].text.strip()
                                print(f"      - {name}: {val}")
                                data.append({'name': name, 'value': val})
                    break 
            
            if not data:
                print("❌ No Region data extracted (Table might be empty or structure changed).")

        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            await browser.close()
            print("🏁 Browser Closed.")

if __name__ == "__main__":
    asyncio.run(scrape_regions_smart())