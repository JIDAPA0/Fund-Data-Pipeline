import asyncio
from playwright.async_api import async_playwright

async def quick_test():
    async with async_playwright() as p:
        
        browser = await p.chromium.launch(headless=False) 
        context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        page = await context.new_page()
        
        ticker = "SPY"
        print(f"🚀 กำลังทดสอบดึงข้อมูล: {ticker}")
        
        try:
            
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/risk", wait_until="domcontentloaded", timeout=30000)
            
            print("⏳ รอให้ตาราง Risk ปรากฏบนหน้าจอ...")
            
            target_selector = 'section[data-testid="risk-statistics-table"]'
            await page.wait_for_selector(target_selector, timeout=15000)
            
            
            rows = page.locator(f'{target_selector} tbody tr')
            count = await rows.count()
            
            print(f"📊 พบข้อมูลในตารางทั้งหมด {count} แถว")
            
            for i in range(count):
                row_text = await rows.nth(i).inner_text()
                
                clean_text = row_text.replace('\n', ' | ')
                print(f"Row {i+1}: {clean_text}")

        except Exception as e:
            print(f"❌ เกิดข้อผิดพลาด: {str(e)}")
        
        finally:
            print("🏁 จบการทดสอบ")
            await asyncio.sleep(5) 
            await browser.close()

if __name__ == "__main__":
    asyncio.run(quick_test())