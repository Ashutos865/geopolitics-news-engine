import asyncio
import aiohttp
import feedparser
import pandas as pd
import json
import os
import trafilatura  # New requirement

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

async def fetch_content(session, url):
    """New function to extract full article text"""
    try:
        async with session.get(url, headers=HEADERS, timeout=15) as response:
            if response.status == 200:
                html = await response.text()
                # Trafilatura extracts the main text content automatically
                content = trafilatura.extract(html)
                return content if content else "Full text unavailable."
    except:
        return "Error fetching content."

async def main():
    # --- 1. Load Memory ---
    seen_links = set()
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding='utf-8-sig') as f:
                seen_links = set(json.load(f))
        except: seen_links = set()

    try:
        # --- 2. Load Feeds from Sheet ---
        df = pd.read_csv(SHEET_URL)
        urls = df.iloc[:, 3].dropna().tolist()

        # --- 3. Fetch All Feeds ---
        print(f"Checking {len(urls)} sources...")
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_feed(session, url) for url in urls]
            results = await asyncio.gather(*tasks)

            # --- 4. Identify NEW articles ---
            to_scrape = []
            for feed in results:
                if feed and hasattr(feed, 'entries'):
                    for entry in feed.entries[:3]: # Limit to top 3 for speed
                        link = entry.get("link")
                        if link and link not in seen_links:
                            to_scrape.append({
                                "title": entry.get("title", "No Title"),
                                "link": link,
                                "source": feed.feed.get("title", "Unknown Source")
                            })

            # --- 5. Scrape full content for new articles ---
            print(f"Extracting content for {len(to_scrape)} new articles...")
            new_articles = []
            for item in to_scrape:
                full_text = await fetch_content(session, item["link"])
                item["content"] = full_text
                new_articles.append(item)
                seen_links.add(item["link"])

        # --- 6. Save updates ---
        with open("raw_news.json", "w", encoding='utf-8') as f:
            json.dump(new_articles, f, indent=4, ensure_ascii=False)
        
        with open(CACHE_FILE, "w", encoding='utf-8') as f:
            json.dump(list(seen_links)[-5000:], f, indent=4, ensure_ascii=False)
        
        print(f"Success: Processed {len(new_articles)} articles.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(main())