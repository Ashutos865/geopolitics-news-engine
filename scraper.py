import asyncio
import aiohttp
import feedparser
import pandas as pd
import json
import os
import trafilatura
from datetime import datetime
import re

# --- CONFIGURATION ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRYXAkf_syLltQDImMYKPJb5XRrOceJiLIzUSnwKJr58QvfcQeVZRaFJaDovLJD8kEiyXId85HS7xcP/pub?gid=893052359&single=true&output=csv"
CACHE_FILE = "seen_links.json"
OUTPUT_FILE = "raw_news.json"
MAX_ENTRIES_PER_FEED = 5

# Detailed Geocoding for Conflict Mapping
GEO_COORDINATES = {
    "Taiwan": [23.69, 120.96], "Ukraine": [48.37, 31.16], "Israel": [31.04, 34.85],
    "Gaza": [31.35, 34.30], "Russia": [61.52, 105.31], "Iran": [32.42, 53.68],
    "South China Sea": [12.0, 113.0], "Red Sea": [20.0, 38.0], "India": [20.59, 78.96],
    "USA": [37.09, -95.71], "China": [35.86, 104.19], "North Korea": [40.33, 127.51],
    "South Korea": [35.90, 127.76], "Lebanon": [33.85, 35.86], "Syria": [34.80, 38.99]
}

async def fetch_feed(session, url):
    """Fetches and parses RSS feeds with error handling."""
    try:
        async with session.get(url, timeout=15) as response:
            if response.status == 200:
                text = await response.text()
                return feedparser.parse(text)
    except Exception: return None

async def extract_tactical_data(session, item):
    """
    Extracts full article content, generates a fallback summary, 
    and assigns geospatial coordinates.
    """
    try:
        async with session.get(item['link'], timeout=15) as response:
            if response.status == 200:
                html = await response.text()
                # Use trafilatura for high-quality main text extraction
                content = trafilatura.extract(html, include_comments=False, include_tables=False)
                
                if content:
                    item['content'] = content
                    # Fallback summary (first 200 chars) for the UI cards
                    item['summary'] = content[:200].strip() + "..."
                else:
                    item['content'] = "Tactical data encrypted or blocked by source."
                    item['summary'] = "Full report available via source link."

                # --- GEOSPATIAL INTELLIGENCE ---
                item['lat'], item['lng'] = [None, None]
                item['location_tag'] = "Global"
                
                # Combine title and content for better keyword matching
                intel_blob = (item['title'] + " " + (content or "")).lower()
                
                for location, coords in GEO_COORDINATES.items():
                    if re.search(rf"\b{location.lower()}\b", intel_blob):
                        item['lat'], item['lng'] = coords
                        item['location_tag'] = location
                        break
                return item
    except Exception:
        item['content'] = "Source node unreachable."
        item['summary'] = "Extraction failed."
        return item

async def main():
    print(f"📡 [SYSTEM] Operational Start: {datetime.now().strftime('%H:%M:%S')}")
    
    # 1. Load Persistence (Seen Links)
    seen_links = set()
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding='utf-8-sig') as f:
                seen_links = set(json.load(f))
        except: pass

    try:
        # 2. Sync Intelligence Sources
        df = pd.read_csv(SHEET_URL)
        urls = df.iloc[:, 3].dropna().tolist()

        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) GeoIntel/1.0'}) as session:
            # 3. Parallel Feed Check (Speed Layer)
            print(f"🔍 [SYSTEM] Checking {len(urls)} intelligence nodes...")
            feed_tasks = [fetch_feed(session, url) for url in urls]
            feeds = await asyncio.gather(*feed_tasks)

            to_process = []
            for feed in feeds:
                if feed and hasattr(feed, 'entries'):
                    # Only take the freshest updates to keep the map relevant
                    for entry in feed.entries[:MAX_ENTRIES_PER_FEED]: 
                        link = entry.get("link")
                        if link and link not in seen_links:
                            to_process.append({
                                "title": entry.get("title", "UNTITLED REPORT"),
                                "link": link,
                                "source": feed.feed.get("title", "Independent Source"),
                                "timestamp": datetime.now().isoformat()
                            })

            if not to_process:
                print("✅ [SYSTEM] No new intelligence detected. Standing by.")
                return

            # 4. Parallel Tactical Extraction (The Power Layer)
            print(f"⚡ [SYSTEM] Processing {len(to_process)} new intelligence packets...")
            extraction_tasks = [extract_tactical_data(session, item) for item in to_process]
            new_articles = await asyncio.gather(*extraction_tasks)

            # 5. Commit to Memory and Disk
            for art in new_articles:
                seen_links.add(art['link'])

            with open(OUTPUT_FILE, "w", encoding='utf-8') as f:
                json.dump(new_articles, f, indent=4, ensure_ascii=False)

            # Maintain memory limit for GitHub performance
            with open(CACHE_FILE, "w", encoding='utf-8') as f:
                json.dump(list(seen_links)[-3000:], f, indent=4, ensure_ascii=False)

            print(f"🎯 [SYSTEM] Success: {len(new_articles)} tactical reports pushed to Discovery.")

    except Exception as e:
        print(f"⚠️ [CRITICAL] Operation Aborted: {e}")

if __name__ == "__main__":
    asyncio.run(main())