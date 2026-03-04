import asyncio
import aiohttp
import feedparser
import pandas as pd
import json
import os
import trafilatura
from datetime import datetime, timezone
import re

# --- CONFIGURATION ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRYXAkf_syLltQDImMYKPJb5XRrOceJiLIzUSnwKJr58QvfcQeVZRaFJaDovLJD8kEiyXId85HS7xcP/pub?gid=893052359&single=true&output=csv"
CACHE_FILE = "seen_links.json"
OUTPUT_FILE = "raw_news.json"
MAX_STORAGE_LIMIT = 2000 
MAX_ENTRIES_PER_FEED = 15 
MAX_CONCURRENT_REQUESTS = 15  # Fixed the Windows crash issue

# --- INTEL CONFIG ---
GEO_COORDINATES = {
    "Taiwan": [23.69, 120.96], "Ukraine": [48.37, 31.16], "Israel": [31.04, 34.85],
    "Gaza": [31.35, 34.30], "Russia": [61.52, 105.31], "Iran": [32.42, 53.68],
    "India": [20.59, 78.96], "USA": [37.09, -95.71], "China": [35.86, 104.19]
}

INTEL_TAGS = {
    "Defense": r"military|nato|missile|warship|deployment|nuclear|pentagon|army|navy",
    "Energy": r"oil|gas|pipeline|energy|blackout|grid",
    "Cyber": r"hack|cyber|data breach|satellite|starlink",
    "Trade": r"sanction|tariff|gdp|inflation|chips|semiconductor"
}

def get_relative_time(ts_iso):
    """Calculates human-readable 'time ago'."""
    try:
        past = datetime.fromisoformat(ts_iso).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        diff = now - past
        minutes = int(diff.total_seconds() / 60)
        if minutes < 60: return f"{minutes}m ago"
        hours = int(minutes / 60)
        if hours < 24: return f"{hours}h ago"
        return f"{int(hours/24)}d ago"
    except: return "Recent"

async def fetch_feed(session, url):
    try:
        async with session.get(url, timeout=15) as response:
            if response.status == 200:
                return feedparser.parse(await response.text())
    except: return None

async def extract_tactical_data(session, item, sem):
    """Uses Semaphore to prevent 'Forcibly Closed' errors on Windows."""
    async with sem:
        try:
            await asyncio.sleep(0.2) # Politeness delay
            async with session.get(item['link'], timeout=12) as response:
                if response.status == 200:
                    html = await response.text()
                    # IMAGE CAPTURE logic here
                    res = trafilatura.extract(html, output_format='json', with_metadata=True)
                    
                    if res:
                        res_data = json.loads(res)
                        content = res_data.get('text', "")
                        item['image'] = res_data.get('image') # The image you asked for
                    else:
                        content = trafilatura.extract(html) or ""
                        item['image'] = None

                    text_blob = (item['title'] + " " + content).lower()
                    item['content'] = content[:600] + "..." if content else "Summary unavailable."
                    
                    # Tags & Geo
                    item['tags'] = [tag for tag, pat in INTEL_TAGS.items() if re.search(pat, text_blob)]
                    item['lat'], item['lng'], item['location'] = [20.0, 0.0, "Global"]
                    for loc, coords in GEO_COORDINATES.items():
                        if loc.lower() in text_blob:
                            item['lat'], item['lng'], item['location'] = coords[0], coords[1], loc
                            break
                    return item
        except:
            item['image'] = None
            item['content'] = "Link analysis skipped (Timeout/Reset)."
            return item

async def main():
    print(f"[SYSTEM] Pulse Check: {datetime.now().isoformat()}")
    
    # Load Cache
    seen_links = set()
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding='utf-8') as f:
            seen_links = set(json.load(f))

    # Load DB
    db = []
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding='utf-8') as f:
            db = json.load(f)

    sem = asyncio.BoundedSemaphore(MAX_CONCURRENT_REQUESTS)

    try:
        df = pd.read_csv(SHEET_URL)
        urls = df.iloc[:, 3].dropna().tolist()

        async with aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0'}) as session:
            feeds = await asyncio.gather(*[fetch_feed(session, url) for url in urls])
            
            to_process = []
            for feed in feeds:
                if feed and hasattr(feed, 'entries'):
                    source_name = feed.feed.get("title", "Open Intel")
                    for entry in feed.entries[:MAX_ENTRIES_PER_FEED]:
                        link = entry.get("link")
                        if link and link not in seen_links:
                            to_process.append({
                                "title": entry.get("title", "UNTITLED").strip(),
                                "link": link,
                                "source": source_name,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "human_time": "Just now"
                            })

            if to_process:
                print(f"[SYSTEM] Extracting {len(to_process)} new packets safely...")
                new_data = await asyncio.gather(*[extract_tactical_data(session, item, sem) for item in to_process])
                valid_new = [d for d in new_data if d]
                db = valid_new + db
                for d in valid_new: seen_links.add(d['link'])

            # AUTO-UPDATE & ROTATION
            for article in db:
                article['human_time'] = get_relative_time(article['timestamp'])

            db.sort(key=lambda x: x['timestamp'], reverse=True)
            db = db[:MAX_STORAGE_LIMIT] # Removes olds automatically

            # Save
            with open(OUTPUT_FILE, "w", encoding='utf-8') as f:
                json.dump(db, f, indent=4, ensure_ascii=False)

            with open(CACHE_FILE, "w", encoding='utf-8') as f:
                json.dump(list(seen_links)[-7000:], f, indent=4, ensure_ascii=False)

            print(f"[STATUS] Sync Complete. Items: {len(db)}")

    except Exception as e:
        print(f"[FAULT] {e}")

if __name__ == "__main__":
    if os.name == 'nt': # Windows stability fix
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())