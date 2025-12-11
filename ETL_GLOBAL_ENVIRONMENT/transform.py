import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

RAW_DIR = "data/raw"
STAGED_DIR = "data/staged"

os.makedirs(STAGED_DIR, exist_ok=True)

OUTPUT_FILE = os.path.join(STAGED_DIR, "air_quality_transformed.csv")

# ---------------------------
# AQI Category (PM2.5 Based)
# ---------------------------
def aqi_category(pm25):
    if pd.isna(pm25):
        return "Unknown"
    if pm25 <= 50:
        return "Good"
    elif pm25 <= 100:
        return "Moderate"
    elif pm25 <= 200:
        return "Unhealthy"
    elif pm25 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"


# ---------------------------
# Risk Classification
# ---------------------------
def classify_risk(severity):
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"


# ---------------------------
# Load and Transform
# ---------------------------
def transform_data():

    all_rows = []

    raw_files = list(Path(RAW_DIR).glob("*.json"))

    if not raw_files:
        print(" No raw files found in data/raw/. Run extract.py first!")
        return

    for file in raw_files:
        city = file.name.split("_")[0]

        with open(file, "r") as f:
            data = json.load(f)

        if "hourly" not in data:
            print(f" Skipping {file} — No 'hourly' field")
            continue

        hourly = data["hourly"]

        times = hourly.get("time", [])
        pm10 = hourly.get("pm10", [])
        pm25 = hourly.get("pm2_5", [])
        co = hourly.get("carbon_monoxide", [])
        no2 = hourly.get("nitrogen_dioxide", [])
        so2 = hourly.get("sulphur_dioxide", [])
        ozone = hourly.get("ozone", [])
        uv = hourly.get("uv_index", [])

        rows = zip(times, pm10, pm25, co, no2, so2, ozone, uv)

        for t, p10, p25, co_v, no2_v, so2_v, oz_v, uv_v in rows:
            row = {
                "city": city,
                "time": t,
                "pm10": p10,
                "pm2_5": p25,
                "carbon_monoxide": co_v,
                "nitrogen_dioxide": no2_v,
                "sulphur_dioxide": so2_v,
                "ozone": oz_v,
                "uv_index": uv_v,
            }
            all_rows.append(row)

    df = pd.DataFrame(all_rows)

    # Convert time → datetime
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Convert numeric columns
    num_cols = [
        "pm10", "pm2_5", "carbon_monoxide",
        "nitrogen_dioxide", "sulphur_dioxide",
        "ozone", "uv_index"
    ]

    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove rows where all pollutants are missing
    df.dropna(subset=num_cols, how="all", inplace=True)

    # Feature Engineering
    df["AQI_Category"] = df["pm2_5"].apply(aqi_category)

    df["severity"] = (
        (df["pm2_5"] * 5) +
        (df["pm10"] * 3) +
        (df["nitrogen_dioxide"] * 4) +
        (df["sulphur_dioxide"] * 4) +
        (df["carbon_monoxide"] * 2) +
        (df["ozone"] * 3)
    )

    df["Risk_Level"] = df["severity"].apply(classify_risk)

    df["hour"] = df["time"].dt.hour

    # Save final staged file
    df.to_csv(OUTPUT_FILE, index=False)

    print(f" Transform completed! File saved to:\n{OUTPUT_FILE}")


if __name__ == "__main__":
    transform_data()
