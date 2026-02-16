import cloudscraper
import json
import random
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import threading
from datetime import datetime
import pytz
import logging

# -----------------------
# CONFIG
# -----------------------
MAX_WORKERS = 4        # 🔥 reduced
RETRY_LIMIT = 3
IST = pytz.timezone("Asia/Kolkata")

# -----------------------
# DATE SETUP
# -----------------------
today = datetime.now(IST)
date_str = today.strftime("%d%m%Y")

DATA_DIR = "Data"
TRASH_DIR = "Trash"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TRASH_DIR, exist_ok=True)

RANKING_FILE = f"{DATA_DIR}/{date_str}_Rankings.json"
LOG_FILE = f"{TRASH_DIR}/run_{date_str}.log"
FAILURE_FILE = f"{TRASH_DIR}/failures_{date_str}.json"

# -----------------------
# LOGGING
# -----------------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

print(f"🚀 Starting run for {date_str}")

# -----------------------
# USER AGENTS
# -----------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

thread_local = threading.local()
failures = []
lock = threading.Lock()

# -----------------------
# Identity per thread
# -----------------------
class Identity:
    def __init__(self):
        self.ua = random.choice(USER_AGENTS)
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )

        # 🔥 Warm homepage (important)
        try:
            self.scraper.get(
                "https://in.bookmyshow.com/",
                headers=self.headers(),
                timeout=10
            )
        except:
            pass

    def headers(self):
        return {
            "User-Agent": self.ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-IN,en;q=0.9",
            "Origin": "https://in.bookmyshow.com",
            "Referer": "https://in.bookmyshow.com/",
            "Connection": "keep-alive"
        }

def get_identity():
    if not hasattr(thread_local, "identity"):
        thread_local.identity = Identity()
    return thread_local.identity

def reset_identity():
    if hasattr(thread_local, "identity"):
        del thread_local.identity

# -----------------------
def load_cities(filename="allcities.json"):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)

def clean_title(title):
    return title.rsplit("(", 1)[0].strip()

# -----------------------
def fetch_movies_for_city(city):
    region_name = city.get("RegionName")
    region_code = city.get("RegionCode")
    url = f"https://in.bookmyshow.com/quickbook-search.bms?r={region_code}"

    ident = get_identity()

    for attempt in range(RETRY_LIMIT):
        try:
            time.sleep(random.uniform(0.6, 1.4))  # softer pacing

            response = ident.scraper.get(
                url,
                headers=ident.headers(),
                timeout=15
            )

            if response.status_code == 403:
                logging.warning(f"403 hit for {region_name}, resetting identity")
                reset_identity()
                ident = get_identity()
                time.sleep(2 ** attempt)
                continue

            if response.status_code == 429:
                time.sleep(2 ** attempt)
                continue

            response.raise_for_status()
            data = response.json()

            hits = data.get("hits", [])
            movies = [
                clean_title(hit["TITLE"])
                for hit in hits
                if hit.get("TYPE") == "MT" and "TITLE" in hit
            ]

            ranked = {f"rank{i+1}": title for i, title in enumerate(movies[:8])}
            return region_name, ranked

        except Exception as e:
            if attempt == RETRY_LIMIT - 1:
                with lock:
                    failures.append({
                        "region": region_name,
                        "code": region_code,
                        "error": str(e)
                    })
                logging.error(f"Failed {region_name}: {e}")
                return region_name, {}

    return region_name, {}

# -----------------------
def main():
    cities = load_cities()

    all_rankings = {}
    movie_points = defaultdict(int)
    movie_city_count = defaultdict(set)

    logging.info(f"Total cities: {len(cities)}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_movies_for_city, city) for city in cities]

        for future in as_completed(futures):
            region_name, ranked_movies = future.result()

            if ranked_movies:
                all_rankings[region_name] = ranked_movies

                for rank_key, movie_title in ranked_movies.items():
                    rank = int(rank_key.replace("rank", ""))
                    points = 9 - rank
                    movie_points[movie_title] += points
                    movie_city_count[movie_title].add(region_name)

    # Save rankings
    output = {
        "date": date_str,
        "total_cities": len(cities),
        "rankings": all_rankings
    }

    with open(RANKING_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=4, ensure_ascii=False)

    if failures:
        with open(FAILURE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "date": date_str,
                "total_failures": len(failures),
                "failures": failures
            }, f, indent=4, ensure_ascii=False)

    # Top 20
    top_movies = sorted(movie_points.items(), key=lambda x: x[1], reverse=True)[:20]

    print("\n🏆 Top 20 Trending Movies\n")
    for idx, (movie, points) in enumerate(top_movies, 1):
        print(f"{idx:2d}. {movie:<30} | Points: {points}")

    print(f"\n✅ Rankings saved to {RANKING_FILE}")

if __name__ == "__main__":
    main()
