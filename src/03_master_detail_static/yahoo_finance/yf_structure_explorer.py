import asyncio
from playwright.async_api import async_playwright

async def explore_risk_page(ticker="OSOL"):
    async with async_playwright() as p:
        print(f"\n🔍 เริ่มสำรวจโครงสร้างหน้า Risk: {ticker}")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0...")
        page = await context.new_page()
        
        try:
            await page.goto(f"https://finance.yahoo.com/quote/{ticker}/risk", wait_until="domcontentloaded")
            await asyncio.sleep(7) # รอให้ตาราง Render นานขึ้นหน่อย

            # 1. เช็คว่ามี Table ไหม และ Table อยู่ภายใต้ ID/Class อะไร
            tables = page.locator("table")
            t_count = await tables.count()
            print(f"📊 พบตาราง {t_count} ตาราง")

            for i in range(t_count):
                table = tables.nth(i)
                # ดึง 2 แถวแรกมาดูโครงสร้าง Tag
                rows = await table.locator("tr").all()
                if rows:
                    print(f"\n--- ตารางที่ {i+1} ---")
                    for j in range(min(len(rows), 3)):
                        row_html = await rows[j].inner_html()
                        row_text = await rows[j].inner_text()
                        print(f"Row {j+1} Text: {row_text.replace('\n', ' | ')}")
                        # พี่อยากดูว่ามันใช้ <td> หรือ <div> ข้างใน
                        print(f"Row {j+1} HTML Snippet: {row_html[:150]}...")

            # 2. เช็คว่ามันซ่อนอยู่ใน Section ชื่ออะไร
            sections = page.locator('section')
            s_count = await sections.count()
            print(f"\n📂 พบ Section ทั้งหมด {s_count} ส่วน")
            for i in range(s_count):
                s_id = await sections.nth(i).get_attribute("data-testid")
                if s_id: print(f" - Section Test-ID: {s_id}")

        except Exception as e:
            print(f"❌ พลาด: {str(e)}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(explore_risk_page("SPY")) # ลอง SPY เพราะข้อมูลน่าจะเยอะกว่า