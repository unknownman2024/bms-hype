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
MAX_ERRORS = 20
MAX_WORKERS = 15
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
# LOGGING SETUP
# -----------------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

print(f"🚀 Starting run for {date_str}")

# -----------------------
# GLOBALS
# -----------------------
scraper = cloudscraper.create_scraper()
lock = threading.Lock()
error_count = 0
failures = []

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://in.bookmyshow.com",
    "Referer": "https://in.bookmyshow.com/"
}

# -----------------------
def load_cities(filename="allcities.json"):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)

# -----------------------
def clean_title(title):
    return title.rsplit("(", 1)[0].strip()

# -----------------------
def fetch_movies_for_city(city):
    global error_count

    region_name = city.get("RegionName")
    region_code = city.get("RegionCode")
    url = f"https://in.bookmyshow.com/quickbook-search.bms?r={region_code}"

    try:
        time.sleep(random.uniform(0.5, 1.2))

        response = scraper.get(url, headers=headers, timeout=15)

        if response.status_code == 429:
            raise Exception("Rate limited")

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
        with lock:
            error_count += 1
            failures.append({
                "region": region_name,
                "code": region_code,
                "error": str(e)
            })

        logging.error(f"Error fetching {region_name}: {e}")
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

    # Save rankings JSON
    output = {
        "date": date_str,
        "total_cities": len(cities),
        "rankings": all_rankings
    }

    with open(RANKING_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=4, ensure_ascii=False)

    # Save failures
    if failures:
        with open(FAILURE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "date": date_str,
                "total_failures": len(failures),
                "failures": failures
            }, f, indent=4, ensure_ascii=False)

    # Print Top 20
    top_movies = sorted(movie_points.items(), key=lambda x: x[1], reverse=True)[:20]

    print("\n🏆 Top 20 Trending Movies\n")
    for idx, (movie, points) in enumerate(top_movies, 1):
        cities_count = len(movie_city_count[movie])
        print(f"{idx:2d}. {movie:<30} | Points: {points:<4} | Cities: {cities_count}")

    logging.info("Run completed successfully")
    print(f"\n✅ Rankings saved to {RANKING_FILE}")


if __name__ == "__main__":
    main()
