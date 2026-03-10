import asyncio
from playwright.async_api import async_playwright
import json

async def test_sedar():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,  # visible browser so we can see what happens
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
            ]
        )
        
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 800},
            locale='en-CA',
        )
        
        page = await context.new_page()
        
        # Remove automation detection
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        print("Navigating to SEDAR+...")
        await page.goto('https://www.sedarplus.ca/landingpage/', wait_until='networkidle')
        
        title = await page.title()
        print(f"Page title: {title}")
        
        # Wait and see what loads
        await asyncio.sleep(5)
        
        content = await page.content()
        
        if 'Captcha' in title or 'captcha' in content.lower():
            print("❌ Radware captcha detected")
        else:
            print("✅ Page loaded successfully")
            print(f"Content length: {len(content)}")
        
        await browser.close()

asyncio.run(test_sedar())