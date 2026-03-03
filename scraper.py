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
MAX_STORAGE_LIMIT = 1200  # Now supports 1000+ articles
MAX_ENTRIES_PER_FEED = 5

# --- INTELLIGENCE DICTIONARIES ---
GEO_COORDINATES = {
    "Taiwan": [23.69, 120.96], "Ukraine": [48.37, 31.16], "Israel": [31.04, 34.85],
    "Gaza": [31.35, 34.30], "Russia": [61.52, 105.31], "Iran": [32.42, 53.68],
    "South China Sea": [12.0, 113.0], "Red Sea": [20.0, 38.0], "India": [20.59, 78.96],
    "USA": [37.09, -95.71], "China": [35.86, 104.19], "North Korea": [40.33, 127.51],
    "Lebanon": [33.85, 35.86], "Syria": [34.80, 38.99], "Sudan": [12.86, 30.21]
}

# New: Categorization Engine
INTEL_TAGS = {
    "Tech": r"ai|semiconductor|chip|cyber|quantum|space|satellite|startup|digital",
    "Agri": r"grain|wheat|food security|crop|harvest|agriculture|fertilizer|famine",
    "Defense": r"military|nato|missile|pentagon|warship|soldier|deployment|arms",
    "Finance": r"sanction|gdp|inflation|economy|trade|tariff|market|oil|energy",
    "Conflict": r"explosion|strike|casualty|clash|invasion|protest|riot|ceasefire"
}

async def fetch_feed(session, url):
    try:
        async with session.get(url, timeout=15) as response:
            if response.status == 200:
                return feedparser.parse(await response.text())
    except Exception: return None

async def extract_tactical_data(session, item):
    """Enhanced extraction with auto-categorization and geospatial tagging."""
    try:
        async with session.get(item['link'], timeout=15) as response:
            if response.status == 200:
                html = await response.text()
                content = trafilatura.extract(html, include_comments=False, include_tables=False)
                
                # Content Prep
                full_intel = (item['title'] + " " + (content or "")).lower()
                item['content'] = content if content else "Full tactical data extraction failed."
                item['summary'] = (content[:250].strip() + "...") if content else "Brief intelligence report available."

                # 1. Geospatial Tagging
                item['lat'], item['lng'] = [None, None]
                item['location_tag'] = "Global"
                for loc, coords in GEO_COORDINATES.items():
                    if re.search(rf"\b{loc.lower()}\b", full_intel):
                        item['lat'], item['lng'], item['location_tag'] = coords[0], coords[1], loc
                        break

                # 2. Automated Categorization (Tech, Agri, etc.)
                item['tags'] = []
                for tag, pattern in INTEL_TAGS.items():
                    if re.search(pattern, full_intel):
                        item['tags'].append(tag)
                
                # 3. Urgency Detection
                item['urgency'] = "Standard"
                if re.search(r"urgent|breaking|immediate|critical|alert", full_intel):
                    item['urgency'] = "Critical"

                return item
    except Exception:
        item['content'] = "Node unreachable during extraction."
        return item

async def main():
    print(f"📡 [SYSTEM] Operational Start: {datetime.now().strftime('%H:%M:%S')}")
    
    # 1. Load History (Memory Persistence)
    seen_links = set()
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding='utf-8-sig') as f:
                seen_links = set(json.load(f))
        except: pass

    # 2. Load Existing Articles (The 1000+ Growth Strategy)
    existing_articles = []
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding='utf-8') as f:
                existing_articles = json.load(f)
        except: pass

    try:
        df = pd.read_csv(SHEET_URL)
        urls = df.iloc[:, 3].dropna().tolist()

        async with aiohttp.ClientSession(headers={'User-Agent': 'GeoIntel/2.0'}) as session:
            # 3. Fetch Updates
            feed_tasks = [fetch_feed(session, url) for url in urls]
            feeds = await asyncio.gather(*feed_tasks)

            to_process = []
            for feed in feeds:
                if feed and hasattr(feed, 'entries'):
                    for entry in feed.entries[:MAX_ENTRIES_PER_FEED]: 
                        link = entry.get("link")
                        if link and link not in seen_links:
                            to_process.append({
                                "title": entry.get("title", "UNTITLED"),
                                "link": link,
                                "source": feed.feed.get("title", "Open Intel"),
                                "timestamp": datetime.now().isoformat()
                            })

            if not to_process:
                print("✅ [SYSTEM] No new intelligence. Monitoring...")
                return

            # 4. Deep Extraction
            new_articles = await asyncio.gather(*[extract_tactical_data(session, item) for item in to_process])

            # 5. Database Integration (Append & Prune)
            for art in new_articles:
                seen_links.add(art['link'])

            # Merge new with old, prioritize new, limit to MAX_STORAGE_LIMIT
            total_intelligence = (new_articles + existing_articles)[:MAX_STORAGE_LIMIT]

            with open(OUTPUT_FILE, "w", encoding='utf-8') as f:
                json.dump(total_intelligence, f, indent=4, ensure_ascii=False)

            with open(CACHE_FILE, "w", encoding='utf-8') as f:
                json.dump(list(seen_links)[-4000:], f, indent=4, ensure_ascii=False)

            print(f"🎯 [SYSTEM] Success: Database expanded to {len(total_intelligence)} reports.")

    except Exception as e:
        print(f"⚠️ [CRITICAL] System Fault: {e}")

if __name__ == "__main__":
    asyncio.run(main())