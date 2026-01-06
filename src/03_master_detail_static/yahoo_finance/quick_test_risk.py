import asyncio
from playwright.async_api import async_playwright

async def quick_test():
    async with async_playwright() as p:
        # แนะนำให้เปิด headless=False เพื่อดูว่าหน้าเว็บโหลดถึงไหน
        browser = await p.chromium.launch(headless=False) 
        context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        page = await context.new_page()
        
        ticker = "SPY"
        print(f"🚀 กำลังทดสอบดึงข้อมูล: {ticker}")
        
        try:
            # เปลี่ยนจาก networkidle เป็น domcontentloaded เพื่อความเร็ว
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/risk", wait_until="domcontentloaded", timeout=30000)
            
            print("⏳ รอให้ตาราง Risk ปรากฏบนหน้าจอ...")
            # รอเฉพาะ Selector ที่เราต้องการจริงๆ
            target_selector = 'section[data-testid="risk-statistics-table"]'
            await page.wait_for_selector(target_selector, timeout=15000)
            
            # ดึงข้อมูล Alpha แถวแรกมาเช็ค
            rows = page.locator(f'{target_selector} tbody tr')
            count = await rows.count()
            
            print(f"📊 พบข้อมูลในตารางทั้งหมด {count} แถว")
            
            for i in range(count):
                row_text = await rows.nth(i).inner_text()
                # คลีนข้อความเพื่อดูผลลัพธ์
                clean_text = row_text.replace('\n', ' | ')
                print(f"Row {i+1}: {clean_text}")

        except Exception as e:
            print(f"❌ เกิดข้อผิดพลาด: {str(e)}")
        
        finally:
            print("🏁 จบการทดสอบ")
            await asyncio.sleep(5) # ให้เวลาเราดูหน้าจอก่อนปิด
            await browser.close()

if __name__ == "__main__":
    asyncio.run(quick_test())