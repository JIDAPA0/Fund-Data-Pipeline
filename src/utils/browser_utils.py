import random
import asyncio
from typing import Dict, Any

# ==============================================================================
# 1. MASSIVE USER-AGENT POOL (ครบชุด 50+ เหมือนเดิม)
# ==============================================================================
USER_AGENTS = [
    # --- Windows 10/11 (Chrome, Edge, Firefox) ---
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",

    # --- macOS (Intel & M1/M2/M3) ---
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; ARM Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; ARM Mac OS X 14_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; ARM Mac OS X 13_6_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",

    # --- Linux ---
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
]

def get_random_user_agent():
    """สุ่ม User-Agent 1 ค่า"""
    return random.choice(USER_AGENTS)

def get_random_headers():
    """สร้าง Headers แบบสุ่มสำหรับ requests/aiohttp"""
    return {
        "User-Agent": get_random_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }

# ==============================================================================
# 2. PLAYWRIGHT CONFIGURATION (ตั้งค่า Browser แบบ Stealth)
# ==============================================================================
def get_launch_args(headless: bool = False) -> Dict[str, Any]:
    return {
        "headless": headless,
        "args": [
            '--disable-blink-features=AutomationControlled', 
            '--window-size=1920,1080',
            '--start-maximized',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-infobars',
            '--disable-dev-shm-usage',
            '--disable-extensions',
        ]
    }

def get_context_options() -> Dict[str, Any]:
    return {
        "user_agent": get_random_user_agent(),
        "viewport": {'width': 1920, 'height': 1080},
        "locale": "en-US",
        "timezone_id": "America/New_York", 
        "has_touch": False,
        "is_mobile": False,
        "java_script_enabled": True,
    }

# ==============================================================================
# 3. HUMAN SIMULATION
# ==============================================================================
async def human_sleep(min_sec=1.0, max_sec=3.0):
    await asyncio.sleep(random.uniform(min_sec, max_sec))

async def human_mouse_move(page):
    try:
        viewport = page.viewport_size or {'width': 1366, 'height': 768}
        width, height = viewport['width'], viewport['height']
        for _ in range(random.randint(1, 3)):
            x = random.randint(50, width - 50)
            y = random.randint(50, height - 50)
            await page.mouse.move(x, y, steps=random.randint(10, 25))
            await asyncio.sleep(random.uniform(0.1, 0.4))
    except Exception:
        pass 

async def mimic_reading(page, min_sec=2, max_sec=5):
    await human_mouse_move(page)
    await page.mouse.wheel(0, random.randint(100, 500))
    await asyncio.sleep(random.uniform(0.5, 1.5))
    if random.random() > 0.7:
        await page.mouse.wheel(0, -random.randint(50, 200))
    await human_sleep(min_sec, max_sec)

# ==============================================================================
# 4. COOKIE KILLER (⚡️ IFRAME PIERCING MODE ⚡️)
# ==============================================================================
async def dismiss_cookie_banner(page):
    """
    🍪 ท่าไม้ตายใหม่: หาปุ่มทั้งในหน้าหลัก AND ในทุก IFRAME
    """
    # รวม Selector ทุกรูปแบบที่เป็นไปได้
    # เพิ่ม class 'sp_choice_type_11' ที่คุณเจอมาแล้ว
    selectors = [
        'button[title="Accept Cookies"]',
        'button[aria-label="Accept Cookies"]',
        'button.sp_choice_type_11', 
        'button#onetrust-accept-btn-handler',
        'button:has-text("Accept Cookies")',
        'button:has-text("Accept All")',
        'button:has-text("I Agree")',
        'button:has-text("Allow all")'
    ]

    # ฟังก์ชันย่อย: ลองกดปุ่มใน Context ที่ส่งมา (Page หรือ Frame)
    async def try_click_in_context(context):
        for selector in selectors:
            try:
                # หาปุ่มแรกที่เจอ
                btn = context.locator(selector).first
                if await btn.is_visible():
                    # เจอปุ๊บ กดปั๊บ ไม่รอ
                    await btn.click(timeout=1000)
                    return True
            except:
                continue
        return False

    try:
        # 1. ลองหาในหน้าหลักก่อน
        if await try_click_in_context(page):
            return True

        # 2. 🔥 ทีเด็ด: วนลูปหาในทุก IFRAME (เพราะ FT ชอบซ่อนในนี้)
        for frame in page.frames:
            if await try_click_in_context(frame):
                return True
                
    except Exception:
        pass

    return False