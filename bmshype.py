import cloudscraper
import json
import random
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import threading
import sys

# -----------------------
# GLOBALS
# -----------------------
scraper = cloudscraper.create_scraper()
lock = threading.Lock()
error_count = 0
MAX_ERRORS = 20
raw_district_venues = []
all_rankings = {}  # Make global so it can be accessed in the exception handler

headers = {
    "User-Agent": "1M1ozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://in.bookmyshow.com",
    "Referer": "https://in.bookmyshow.com/"
}

# -----------------------
# Load existing rankings
# -----------------------
def load_existing_rankings(filename="bms_movie_rankings.json"):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

# -----------------------
def clear_console():
    os.system("clear" if os.name != "nt" else "cls")
# -----------------------
def load_cities(filename="allcities.json"):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)

# -----------------------
# Clean movie title
# -----------------------
def clean_title(title):
    return title.rsplit("(", 1)[0].strip()

# -----------------------
# Fetch movie rankings for a city
# -----------------------
def fetch_movies_for_city(city):
    global error_count
    region_name = city.get("RegionName")
    region_code = city.get("RegionCode")
    url = f"https://in.bookmyshow.com/quickbook-search.bms?r={region_code}"

    try:
        print(f"[>>] Fetching: {region_name} ({region_code})")
        time.sleep(random.uniform(0.5, 1.5))  # Respectful delay

        response = scraper.get(url, headers=headers, timeout=10)

        if response.status_code == 429:
            print(f"⏳ 429 Rate limit for {region_name}")
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
            print(f"[!] Error fetching {region_name} ({region_code}): {e}")

            if error_count >= MAX_ERRORS:
                print("🛑 Too many errors. Saving progress and restarting...")

                # Load partial rankings if exist
                partial_data = load_existing_rankings()

                # Merge whatever is fetched till now (thread-safe)
                for region, data in partial_data.items():
                    all_rankings[region] = data  # all_rankings should be made global

                with open("bms_movie_rankings.json", "w", encoding="utf-8") as f:
                    json.dump(all_rankings, f, indent=4, ensure_ascii=False)

                time.sleep(3)
                clear_console()
                os.execv(sys.executable, ['python'] + sys.argv)

        return region_name, {}

# -----------------------
# Main logic
# -----------------------
def main():
    global all_rankings
    cities = load_cities()
    existing_rankings = load_existing_rankings()
    all_rankings = dict(existing_rankings)

    movie_points = defaultdict(int)
    movie_city_count = defaultdict(set)

    pending_cities = [
        city for city in cities
        if city.get("RegionName") not in existing_rankings
    ]

    print(f"📌 Total cities: {len(cities)}")
    print(f"⏭️ Skipping {len(cities) - len(pending_cities)} already fetched")

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(fetch_movies_for_city, city) for city in pending_cities]

        for future in as_completed(futures):
            region_name, ranked_movies = future.result()
            if ranked_movies:
                all_rankings[region_name] = ranked_movies

                for rank_key, movie_title in ranked_movies.items():
                    try:
                        rank = int(rank_key.replace("rank", ""))
                        points = 9 - rank
                        movie_points[movie_title] += points
                        movie_city_count[movie_title].add(region_name)
                    except:
                        continue

    for region_name, ranked_movies in existing_rankings.items():
        for rank_key, movie_title in ranked_movies.items():
            try:
                rank = int(rank_key.replace("rank", ""))
                points = 9 - rank
                movie_points[movie_title] += points
                movie_city_count[movie_title].add(region_name)
            except:
                continue

    with open("bms_movie_rankings.json", "w", encoding="utf-8") as f:
        json.dump(all_rankings, f, indent=4, ensure_ascii=False)

    top_movies = sorted(movie_points.items(), key=lambda x: x[1], reverse=True)[:20]

    print("\n🏆 Top 20 Trending Movies:\n")
    for idx, (movie, points) in enumerate(top_movies, 1):
        cities_count = len(movie_city_count[movie])
        print(f"{idx:2d}. {movie:<30} | 🎯 Points: {points:<4} | 🌍 Trending in: {cities_count} cities")

    print("\n✅ Done. Results saved to bms_movie_rankings.json")

if __name__ == "__main__":
    main()
