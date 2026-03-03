import asyncio
import aiohttp
import feedparser
import pandas as pd
import json
import os

SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRYXAkf_syLltQDImMYKPJb5XRrOceJiLIzUSnwKJr58QvfcQeVZRaFJaDovLJD8kEiyXId85HS7xcP/pub?gid=893052359&single=true&output=csv"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
CACHE_FILE = "seen_links.json"

async def fetch_feed(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=15) as response:
            if response.status == 200:
                text = await response.text()
                return feedparser.parse(text)
    except: return None

async def main():
    # --- 1. Load Memory (Seen Links) with safe encoding ---
    seen_links = set()
    if os.path.exists(CACHE_FILE):
        try:
            # We use utf-8-sig to handle Windows "BOM" markers automatically
            with open(CACHE_FILE, "r", encoding='utf-8-sig') as f:
                data = json.load(f)
                seen_links = set(data)
        except (json.JSONDecodeError, UnicodeDecodeError):
            print("Memory file corrupted, starting fresh.")
            seen_links = set()

    try:
        # --- 2. Load Feeds from Sheet ---
        df = pd.read_csv(SHEET_URL)
        # Use column index 3 (RSS URL)
        urls = df.iloc[:, 3].dropna().tolist()

        # --- 3. Fetch All Feeds ---
        print(f"Checking {len(urls)} sources for updates...")
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_feed(session, url) for url in urls]
            results = await asyncio.gather(*tasks)

        # --- 4. Filter only NEW articles ---
        new_articles = []
        for feed in results:
            if feed and hasattr(feed, 'entries'):
                for entry in feed.entries[:5]:
                    link = entry.get("link")
                    if link and link not in seen_links:
                        new_articles.append({
                            "title": entry.get("title", "No Title"),
                            "link": link,
                            "source": feed.feed.get("title", "Unknown Source")
                        })
                        seen_links.add(link)

        # --- 5. Save ONLY new articles ---
        with open("raw_news.json", "w", encoding='utf-8') as f:
            json.dump(new_articles, f, indent=4, ensure_ascii=False)
        
        # --- 6. Update Memory (Limit to last 5000) ---
        with open(CACHE_FILE, "w", encoding='utf-8') as f:
            json.dump(list(seen_links)[-5000:], f, indent=4, ensure_ascii=False)
        
        print(f"Success: Found {len(new_articles)} NEW articles.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
if __name__ == "__main__":
    asyncio.run(main())