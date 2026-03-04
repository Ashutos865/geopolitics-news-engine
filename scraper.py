import asyncio
import aiohttp
import feedparser
import pandas as pd
import json
import os
import trafilatura
import google.generativeai as genai
from datetime import datetime, timezone
import re

# --- CONFIGURATION ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") 
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    ai_model = genai.GenerativeModel("gemini-2.5-flash")
else:
    print("[ERROR] GEMINI_API_KEY not found in environment!")

SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRYXAkf_syLltQDImMYKPJb5XRrOceJiLIzUSnwKJr58QvfcQeVZRaFJaDovLJD8kEiyXId85HS7xcP/pub?gid=893052359&single=true&output=csv"
CACHE_FILE = "seen_links.json"
OUTPUT_FILE = "raw_news.json"
MAX_STORAGE_LIMIT = 2000 
MAX_ENTRIES_PER_FEED = 15 
MAX_CONCURRENT_REQUESTS = 5 

async def ai_process_intelligence(title, content):
    if not GEMINI_API_KEY: return None
    prompt = f"""
    Analyze this news packet and respond ONLY with a valid JSON object.
    Categories: Defense, Energy, Cyber, Trade, Agri. (Trade includes startups, funding, and valuations).
    
    Article Title: {title}
    Article Content: {content[:1800]}

    Return JSON format:
    {{
      "category": "Chosen Category",
      "location": "Specific Country or Global",
      "narration": "A 2-sentence tactical summary in a professional intelligence style.",
      "analysis": "A brief 3-point bulleted breakdown of the impact."
    }}
    """
    try:
        response = await ai_model.generate_content_async(prompt)
        text = response.text
        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        return None
    except Exception as e:
        print(f"[AI ERROR] {e}")
        return None

def get_relative_time(ts_iso):
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
    async with sem:
        try:
            await asyncio.sleep(0.5) 
            async with session.get(item['link'], timeout=15) as response:
                if response.status == 200:
                    html = await response.text()
                    res = trafilatura.extract(html, output_format='json', with_metadata=True)
                    if res:
                        res_data = json.loads(res)
                        full_text = res_data.get('text', "")
                        item['image'] = res_data.get('image')
                        item['full_content'] = full_text 
                        item['content'] = full_text[:400] + "..."
                        ai_data = await ai_process_intelligence(item['title'], full_text)
                        if ai_data:
                            item['tags'] = [ai_data.get('category')]
                            item['location'] = ai_data.get('location')
                            item['narration'] = ai_data.get('narration')
                            item['analysis'] = ai_data.get('analysis')
                        else:
                            item['tags'] = ["Trade"] if "valuation" in item['title'].lower() else ["General"]
                            item['location'] = "Global"
                    return item
        except: return None

async def main():
    print(f"[SYSTEM] Pulse Check: {datetime.now().isoformat()}")
    seen_links = set()
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding='utf-8') as f:
                seen_links = set(json.load(f))
        except: pass

    db = []
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding='utf-8') as f:
                db = json.load(f)
        except: pass

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
                            })

            if to_process:
                print(f"[SYSTEM] Processing {len(to_process)} packets...")
                new_data = await asyncio.gather(*[extract_tactical_data(session, item, sem) for item in to_process])
                valid_new = [d for d in new_data if d and 'tags' in d]
                
                # --- STRICT FIFO ROTATION LOGIC ---
                num_new = len(valid_new)
                if len(db) + num_new > MAX_STORAGE_LIMIT:
                    # Remove exactly the number of items we are about to add
                    items_to_remove = (len(db) + num_new) - MAX_STORAGE_LIMIT
                    db = db[:-items_to_remove] # Remove from the end (oldest)
                
                # Prepend new items to top
                db = valid_new + db
                for d in valid_new: seen_links.add(d['link'])

            for article in db:
                article['human_time'] = get_relative_time(article['timestamp'])

            # Final sort to ensure time consistency
            db.sort(key=lambda x: x['timestamp'], reverse=True)

            with open(OUTPUT_FILE, "w", encoding='utf-8') as f:
                json.dump(db, f, indent=4, ensure_ascii=False)

            with open(CACHE_FILE, "w", encoding='utf-8') as f:
                json.dump(list(seen_links)[-7000:], f, indent=4, ensure_ascii=False)

            print(f"[STATUS] Database Rotated. Size: {len(db)}")

    except Exception as e:
        print(f"[FAULT] {e}")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
