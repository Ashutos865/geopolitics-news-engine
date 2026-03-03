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
MAX_STORAGE_LIMIT = 1500  # Increased for deeper history
MAX_ENTRIES_PER_FEED = 10 # Get more data per run

# --- EXPANDED GEOSPATIAL INTELLIGENCE ---
GEO_COORDINATES = {
    "Taiwan": [23.69, 120.96], "Ukraine": [48.37, 31.16], "Israel": [31.04, 34.85],
    "Gaza": [31.35, 34.30], "Russia": [61.52, 105.31], "Iran": [32.42, 53.68],
    "South China Sea": [12.0, 113.0], "Red Sea": [20.0, 38.0], "India": [20.59, 78.96],
    "USA": [37.09, -95.71], "China": [35.86, 104.19], "North Korea": [40.33, 127.51],
    "Lebanon": [33.85, 35.86], "Syria": [34.80, 38.99], "Sudan": [12.86, 30.21],
    "Red Sea": [20.1, 38.5], "Bab al-Mandab": [12.6, 43.3], "Hormuz": [26.6, 56.2],
    "Arctic": [76.0, 100.0], "Baltic": [57.0, 19.0], "Poland": [51.9, 19.1]
}

INTEL_TAGS = {
    "Defense": r"military|nato|missile|warship|deployment|nuclear|pentagon|army|navy|air force",
    "Energy": r"oil|gas|pipeline|nord stream|opec|energy|blackout|grid|nuclear plant",
    "Cyber": r"hack|cyber|data breach|ransomware|satellite|quantum|starlink|encryption",
    "Agri": r"grain|wheat|food security|famine|crop|export",
    "Trade": r"sanction|tariff|gdp|inflation|economy|chips|semiconductor|supply chain"
}

async def fetch_feed(session, url):
    try:
        async with session.get(url, timeout=15) as response:
            if response.status == 200:
                return feedparser.parse(await response.text())
    except: return None

async def extract_tactical_data(session, item):
    """Enhanced extraction for World Monitor UI."""
    try:
        async with session.get(item['link'], timeout=12) as response:
            if response.status == 200:
                html = await response.text()
                content = trafilatura.extract(html, include_comments=False)
                
                text_blob = (item['title'] + " " + (content or "")).lower()
                item['content'] = content if content else "Tactical extraction failed."
                
                # Default Location
                item['lat'], item['lng'] = [20.0, 0.0] # Default center
                item['location_tag'] = "Global"
                
                # Match Coordinates
                for loc, coords in GEO_COORDINATES.items():
                    if re.search(rf"\b{loc.lower()}\b", text_blob):
                        item['lat'], item['lng'], item['location_tag'] = coords[0], coords[1], loc
                        break

                # Tagging & Urgency
                item['tags'] = [tag for tag, pat in INTEL_TAGS.items() if re.search(pat, text_blob)]
                item['urgency'] = "High" if re.search(r"breaking|critical|alert|urgent|attack|strike", text_blob) else "Low"
                
                return item
    except:
        return item

async def main():
    print(f"📡 [SYSTEM] Wake-up: {datetime.now().isoformat()}")
    
    # Load History
    seen_links = set()
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding='utf-8-sig') as f:
                seen_links = set(json.load(f))
        except: pass

    # Load Existing
    db = []
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding='utf-8') as f:
                db = json.load(f)
        except: pass

    try:
        # Fetch Feeds
        df = pd.read_csv(SHEET_URL)
        urls = df.iloc[:, 3].dropna().tolist()

        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as session:
            print(f"🔍 Checking {len(urls)} nodes...")
            results = await asyncio.gather(*[fetch_feed(session, url) for url in urls])

            to_process = []
            for feed in results:
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

            if to_process:
                print(f"⚡ Processing {len(to_process)} new intel packets...")
                new_data = await asyncio.gather(*[extract_tactical_data(session, item) for item in to_process])
                for art in new_data:
                    seen_links.add(art['link'])
                db = (new_data + db)[:MAX_STORAGE_LIMIT]
            else:
                print("✅ No new links. Refreshing existing database structures.")

            # ALWAYS SAVE (Ensures React always gets a fresh file)
            with open(OUTPUT_FILE, "w", encoding='utf-8') as f:
                json.dump(db, f, indent=4, ensure_ascii=False)

            with open(CACHE_FILE, "w", encoding='utf-8') as f:
                json.dump(list(seen_links)[-5000:], f, indent=4, ensure_ascii=False)

            print(f"🎯 Operational: Database contains {len(db)} reports.")

    except Exception as e:
        print(f"⚠️ FAULT: {e}")

if __name__ == "__main__":
    asyncio.run(main())