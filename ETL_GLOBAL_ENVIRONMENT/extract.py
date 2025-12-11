import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# -------------------------------
# ENV CONFIG
# -------------------------------
API_BASE = os.getenv("OPENAQ_API_BASE")
HOURLY_FIELDS = os.getenv("AQ_HOURLY_FIELDS")
CITIES = os.getenv("AQ_CITIES").split(",")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
TIMEOUT = int(os.getenv("TIMEOUT_SECONDS", 10))
SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN_CALLS", 0.5))

RAW_DIR = os.getenv("RAW_DIR", "data/raw")
os.makedirs(RAW_DIR, exist_ok=True)

# -------------------------------
# Coordinates for each city
# -------------------------------
CITY_COORDS = {
    "Delhi": (28.7041, 77.1025),
    "Mumbai": (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867),
    "Kolkata": (22.5726, 88.3639)
}


# -------------------------------
# REQUEST FUNCTION WITH RETRIES
# -------------------------------
def fetch_with_retry(url, city):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"⏳ Fetching {city} (Attempt {attempt}/{MAX_RETRIES})...")

            response = requests.get(url, timeout=TIMEOUT)

            if response.status_code == 200:
                return response.json()

            print(f"⚠️ API Error {response.status_code} for {city}")

        except Exception as e:
            print(f"⚠️ Request failed for {city}: {e}")

        time.sleep(1)  # backoff delay

    print(f"❌ Failed to fetch data for {city} after {MAX_RETRIES} attempts")
    return None


# -------------------------------
# MAIN EXTRACTION FUNCTION
# -------------------------------
def extract_air_quality():
    saved_files = []

    for city in CITIES:

        lat, lon = CITY_COORDS[city]

        # Build final URL dynamically
        url = (
            f"{API_BASE}"
            f"?latitude={lat}&longitude={lon}"
            f"&hourly={HOURLY_FIELDS}"
        )

        data = fetch_with_retry(url, city)

        # If failed, log and continue
        if data is None:
            continue

        # Save raw JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(RAW_DIR, f"{city}_raw_{timestamp}.json")

        with open(filepath, "w") as f:
            json.dump(data, f, indent=4)

        print(f"✅ Saved raw data: {filepath}")
        saved_files.append(filepath)

        time.sleep(SLEEP_BETWEEN)

    return saved_files


# -------------------------------
# RUN SCRIPT
# -------------------------------
if __name__ == "__main__":
    files = extract_air_quality()

    print("\n📌 Extraction complete!")
    print("Saved files:")
    for f in files:
        print(" -", f)

