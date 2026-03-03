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
    """Extraction with error handling for Connection Resets."""
    try:
        # Added a tiny sleep to prevent 'Forcibly Closed' errors
        await asyncio.sleep(0.1) 
        async with session.get(item['link'], timeout=12) as response:
            if response.status == 200:
                html = await response.text()
                content = trafilatura.extract(html, include_comments=False)
                
                text_blob = (item['title'] + " " + (content or "")).lower()
                item['content'] = content if content else "Tactical extraction failed."
                item['lat'], item['lng'] = [20.0, 0.0] 
                item['location_tag'] = "Global"
                
                for loc, coords in GEO_COORDINATES.items():
                    if re.search(rf"\b{loc.lower()}\b", text_blob):
                        item['lat'], item['lng'], item['location_tag'] = coords[0], coords[1], loc
                        break

                item['tags'] = [tag for tag, pat in INTEL_TAGS.items() if re.search(pat, text_blob)]
                item['urgency'] = "High" if re.search(r"breaking|critical|alert|urgent|attack|strike", text_blob) else "Low"
                return item
    except Exception:
        # Return item even if scrape fails so the link is still 'seen'
        item['content'] = "Link analysis skipped (Connection Reset)."
        return item

async def main():
    # Removed emojis to prevent Windows UnicodeEncodeError
    print(f"[SYSTEM] Wake-up: {datetime.now().isoformat()}")
    
    seen_links = set()
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding='utf-8') as f: # Strict UTF-8
                data = json.load(f)
                seen_links = set(data)
        except Exception: pass

    db = []
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding='utf-8') as f:
                db = json.load(f)
        except Exception: pass

    try:
        df = pd.read_csv(SHEET_URL)
        urls = df.iloc[:, 3].dropna().tolist()

        connector = aiohttp.TCPConnector(limit=10) # Limit concurrent connections to stay safe
        async with aiohttp.ClientSession(connector=connector, headers={'User-Agent': 'Mozilla/5.0'}) as session:
            print(f"[SYSTEM] Checking {len(urls)} nodes...")
            results = await asyncio.gather(*[fetch_feed(session, url) for url in urls])

            to_process = []
            for feed in results:
                if feed and hasattr(feed, 'entries'):
                    for entry in feed.entries[:MAX_ENTRIES_PER_FEED]:
                        link = entry.get("link")
                        if link and link not in seen_links:
                            to_process.append({
                                "title": entry.get("title", "UNTITLED").strip(),
                                "link": link,
                                "source": feed.feed.get("title", "Open Intel"),
                                "timestamp": datetime.now().isoformat()
                            })

            if to_process:
                print(f"[SYSTEM] Processing {len(to_process)} packets...")
                new_data = await asyncio.gather(*[extract_tactical_data(session, item) for item in to_process])
                new_data = [d for d in new_data if d is not None]
                
                for art in new_data:
                    seen_links.add(art['link'])
                db = (new_data + db)[:MAX_STORAGE_LIMIT]
            else:
                print("[SYSTEM] No new links.")

            with open(OUTPUT_FILE, "w", encoding='utf-8') as f:
                json.dump(db, f, indent=4, ensure_ascii=False)

            with open(CACHE_FILE, "w", encoding='utf-8') as f:
                json.dump(list(seen_links)[-5000:], f, indent=4, ensure_ascii=False)

            print(f"[STATUS] Database Synchronized: {len(db)} reports.")

    except Exception as e:
        print(f"[FAULT] {e}")
if __name__ == "__main__":
    asyncio.run(main())