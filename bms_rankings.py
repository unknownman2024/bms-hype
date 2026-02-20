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
import re
import requests
from requests_oauthlib import OAuth1

# -----------------------
# CONFIG
# -----------------------
MAX_WORKERS = 4
RETRY_LIMIT = 3
REQUEST_DELAY = (0.6, 1.4)
IST = pytz.timezone("Asia/Kolkata")

POST_TO_X = True  # 🔥 Set False to disable tweeting

# -----------------------
# X API (Use ENV VARIABLES)
# -----------------------
X_API_KEY = os.getenv("X_API_KEY")
X_API_SECRET = os.getenv("X_API_SECRET")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
X_ACCESS_SECRET = os.getenv("X_ACCESS_SECRET")

# -----------------------
# DATE SETUP
# -----------------------
now = datetime.now(IST)
year_str = now.strftime("%Y")
date_file_str = now.strftime("%Y-%m-%d")
last_updated_str = now.strftime("%Y-%m-%d %H:%M IST")

DATA_DIR = f"Data/{year_str}"
TRASH_DIR = f"Trash/{year_str}"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TRASH_DIR, exist_ok=True)

RANKING_FILE = f"{DATA_DIR}/Rankings_{date_file_str}.json"
FAILURE_FILE = f"{TRASH_DIR}/failures_{date_file_str}.json"
LOG_FILE = f"{TRASH_DIR}/run_{date_file_str}.log"

# -----------------------
# LOGGING SETUP
# -----------------------
logger = logging.getLogger("BMS")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(message)s"))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.info(f"\n🚀 Starting run for {date_file_str}\n")

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
        self.warm_session()

    def headers(self):
        return {
            "User-Agent": self.ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-IN,en;q=0.9",
            "Origin": "https://in.bookmyshow.com",
            "Referer": "https://in.bookmyshow.com/",
            "Connection": "keep-alive"
        }

    def warm_session(self):
        try:
            self.scraper.get(
                "https://in.bookmyshow.com/",
                headers=self.headers(),
                timeout=10
            )
            logger.info("🌐 Session warmed")
        except Exception as e:
            logger.warning(f"Warmup failed: {e}")

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

def clean_hashtag(title):
    title = re.sub(r"\(.*?\)", "", title)
    title = re.sub(r"[^A-Za-z0-9 ]+", "", title)
    title = title.replace(" ", "")
    return f"#{title}"

# -----------------------
# X POST FUNCTION
# -----------------------
def post_to_x(tweet_text):
    if not POST_TO_X:
        logger.info("🐦 Tweeting disabled.")
        return

    if not all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET]):
        logger.error("❌ Missing X API credentials.")
        return

    try:
        url = "https://api.twitter.com/2/tweets"

        auth = OAuth1(
            X_API_KEY,
            X_API_SECRET,
            X_ACCESS_TOKEN,
            X_ACCESS_SECRET
        )

        payload = {"text": tweet_text}

        response = requests.post(url, auth=auth, json=payload)

        if response.status_code == 201:
            logger.info("🐦 Tweet posted successfully!")
        else:
            logger.error(f"Tweet failed: {response.text}")

    except Exception as e:
        logger.error(f"Tweet error: {e}")

# -----------------------
def fetch_movies_for_city(city, index, total):
    region_name = city.get("RegionName")
    region_code = city.get("RegionCode")
    url = f"https://in.bookmyshow.com/quickbook-search.bms?r={region_code}"

    ident = get_identity()
    logger.info(f"[{index}/{total}] Fetching {region_name} ({region_code})")

    for attempt in range(RETRY_LIMIT):
        try:
            time.sleep(random.uniform(*REQUEST_DELAY))
            response = ident.scraper.get(url, headers=ident.headers(), timeout=15)

            if response.status_code in (403, 429):
                reset_identity()
                ident = get_identity()
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
                return region_name, {}

    return region_name, {}

# -----------------------
def main():
    cities = load_cities()
    total = len(cities)

    all_rankings = {}
    movie_points = defaultdict(int)
    movie_city_count = defaultdict(set)

    logger.info(f"📌 Total cities: {total}\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(fetch_movies_for_city, city, i+1, total)
            for i, city in enumerate(cities)
        ]

        for future in as_completed(futures):
            region_name, ranked_movies = future.result()
            if ranked_movies:
                all_rankings[region_name] = ranked_movies
                for rank_key, movie_title in ranked_movies.items():
                    rank = int(rank_key.replace("rank", ""))
                    points = 9 - rank
                    movie_points[movie_title] += points
                    movie_city_count[movie_title].add(region_name)

    # Save Rankings
    output = {
        "date": date_file_str,
        "last_updated": last_updated_str,
        "total_cities": total,
        "success_cities": len(all_rankings),
        "failed_cities": len(failures),
        "rankings": all_rankings
    }

    with open(RANKING_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=4, ensure_ascii=False)

    # -----------------------
    # Build 280 Character Tweet
    # -----------------------
    logger.info("\n🏆 Building Tweet\n")

    top_movies = sorted(movie_points.items(), key=lambda x: x[1], reverse=True)[:20]

    tweet_date = now.strftime("%d %B %Y")
    header = f"Top 20 Trending Movies on BookMyShow - {tweet_date}\n\n"

    tweet_body = ""
    char_limit = 280

    for idx, (movie, points) in enumerate(top_movies, 1):
        cities_count = len(movie_city_count[movie])
        tag = clean_hashtag(movie)
        line = f"{idx}. {tag} | P:{points} | C:{cities_count}\n"

        if len(header + tweet_body + line) <= char_limit:
            tweet_body += line
        else:
            break

    final_tweet = header + tweet_body.strip()

    logger.info("\n🐦 Tweet Preview:\n")
    logger.info(final_tweet)

    post_to_x(final_tweet)

    logger.info(f"\n✅ Rankings saved to {RANKING_FILE}\n")

# -----------------------
if __name__ == "__main__":
    main()
