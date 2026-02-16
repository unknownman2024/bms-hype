import json
import os
import random
import time
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import cloudscraper

# =====================================================
# CONFIG
# =====================================================
MAX_WORKERS = 4
API_TIMEOUT = 12
IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = datetime.now(IST).strftime("%Y%m%d")
DATE_LABEL = datetime.now(IST).strftime("%d%m%Y")

DATA_DIR = "Data"
TRASH_DIR = "Trash"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TRASH_DIR, exist_ok=True)

RANK_FILE = f"{DATA_DIR}/{DATE_LABEL}_Rankings.json"
FAIL_FILE = f"{TRASH_DIR}/failures_{DATE_LABEL}.json"

# =====================================================
# USER AGENTS
# =====================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118 Safari/537.36",
]

thread_local = threading.local()

class Identity:
    def __init__(self):
        self.ua = random.choice(USER_AGENTS)
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )

    def headers(self):
        return {
            "User-Agent": self.ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-IN,en;q=0.9",
            "Origin": "https://in.bookmyshow.com",
            "Referer": "https://in.bookmyshow.com/",
        }

def get_identity():
    if not hasattr(thread_local, "identity"):
        thread_local.identity = Identity()
    return thread_local.identity

def reset_identity():
    if hasattr(thread_local, "identity"):
        del thread_local.identity

# =====================================================
# FETCH VENUE DATA
# =====================================================
def fetch_venue(vcode):
    ident = get_identity()

    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={vcode}&dateCode={DATE_CODE}"
    )

    try:
        for attempt in range(3):
            r = ident.scraper.get(url, headers=ident.headers(), timeout=API_TIMEOUT)

            if r.status_code == 403:
                reset_identity()
                time.sleep(2 ** attempt)
                continue

            if not r.text.strip().startswith("{"):
                raise Exception("Blocked")

            return r.json()

        return None

    except:
        return None

# =====================================================
# PARSE & ACCUMULATE
# =====================================================
def process_venue(vcode, venue_meta):
    data = fetch_venue(vcode)
    if not data:
        return None, vcode

    movies = []

    sd = data.get("ShowDetails", [])
    if not sd:
        return [], None

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "").strip()

        for ch in ev.get("ChildEvents", []):
            for sh in ch.get("ShowTimes", []):
                if sh.get("ShowDateCode") != DATE_CODE:
                    continue

                total = sold = 0

                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    free = int(cat.get("SeatsAvail", 0))
                    total += seats
                    sold += seats - free

                movies.append((title, sold))

    return movies, None

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":

    with open("venues1.json", "r", encoding="utf-8") as f:
        venues = json.load(f)

    movie_points = defaultdict(int)
    movie_cities = defaultdict(set)
    failures = []

    print(f"🚀 Starting ranking build for {DATE_LABEL}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_venue, vcode, venues[vcode]): vcode
            for vcode in venues
        }

        for future in as_completed(futures):
            movies, failed = future.result()

            if failed:
                failures.append(failed)
                continue

            if not movies:
                continue

            for title, sold in movies:
                movie_points[title] += sold

    # =====================================================
    # SORT RANKINGS
    # =====================================================
    sorted_movies = sorted(movie_points.items(), key=lambda x: x[1], reverse=True)

    rankings = {}
    for i, (movie, score) in enumerate(sorted_movies[:50], 1):
        rankings[f"rank{i}"] = {
            "movie": movie,
            "score": score
        }

    output = {
        "date": DATE_LABEL,
        "total_movies": len(movie_points),
        "rankings": rankings
    }

    with open(RANK_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=4, ensure_ascii=False)

    if failures:
        with open(FAIL_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "date": DATE_LABEL,
                "total_failures": len(failures),
                "venues": failures
            }, f, indent=4)

    print(f"✅ Done. Rankings saved to {RANK_FILE}")
