''' 4️⃣ Analysis (etl_analysis.py)
Read the loaded data from Supabase and perform:
A. KPI Metrics
City with highest average PM2.5
City with the highest severity score
Percentage of High/Moderate/Low risk hours
Hour of day with worst AQI
B. City Pollution Trend Report
For each city:
time → pm2_5, pm10, ozone
C. Export Outputs
Save the following CSVs into data/processed/:
summary_metrics.csv
city_risk_distribution.csv
pollution_trends.csv
D. Visualizations
Save the following PNG plots:
Histogram of PM2.5
Bar chart of risk flags per city
Line chart of hourly PM2.5 trends
Scatter: severity_score vs pm2_5
 
'''
# etl_analysis.py
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Please set SUPABASE_URL and SUPABASE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# helper: fetch table
def fetch_table_as_df(table_name="air_quality_data"):
    # Supabase client: use postgrest select
    try:
        res = supabase.table(table_name).select("*").execute()
    except Exception as e:
        raise RuntimeError(f"Supabase query failed: {e}")
    # res may be object with .data or dict
    data = None
    if hasattr(res, "data"):
        data = res.data
    elif isinstance(res, dict) and "data" in res:
        data = res["data"]
    else:
        # try to interpret res directly
        data = res
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    # Convert time to datetime
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    # ensure numeric types
    numeric_cols = ["pm2_5","pm10","ozone","carbon_monoxide","nitrogen_dioxide","sulphur_dioxide","uv_index","severity_score","hour"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def compute_kpis(df):
    # A. City with highest average PM2.5
    avg_pm25 = df.groupby("city", as_index=False)["pm2_5"].mean().rename(columns={"pm2_5":"avg_pm2_5"})
    top_pm25 = avg_pm25.sort_values("avg_pm2_5", ascending=False).head(1)

    # City with highest severity score (average)
    avg_sev = df.groupby("city", as_index=False)["severity_score"].mean().rename(columns={"severity_score":"avg_severity"})
    top_sev = avg_sev.sort_values("avg_severity", ascending=False).head(1)

    # Percentage of High/Moderate/Low risk hours
    risk_counts = df.groupby(["city","risk_flag"]).size().reset_index(name="count")
    total_counts = df.groupby("city").size().reset_index(name="total")
    risk_dist = risk_counts.merge(total_counts, on="city")
    risk_dist["percent"] = (risk_dist["count"] / risk_dist["total"] * 100).round(2)

    # Hour of day with worst AQI (highest average pm2_5)
    worst_hour = df.groupby("hour", as_index=False)["pm2_5"].mean().sort_values("pm2_5", ascending=False).head(1)

    # summary metrics table
    summary_metrics = {
        "city_highest_avg_pm2_5": top_pm25.iloc[0]["city"] if not top_pm25.empty else None,
        "highest_avg_pm2_5": float(top_pm25.iloc[0]["avg_pm2_5"]) if not top_pm25.empty else None,
        "city_highest_avg_severity": top_sev.iloc[0]["city"] if not top_sev.empty else None,
        "highest_avg_severity": float(top_sev.iloc[0]["avg_severity"]) if not top_sev.empty else None,
        "worst_hour_of_day": int(worst_hour.iloc[0]["hour"]) if not worst_hour.empty else None,
        "worst_hour_avg_pm2_5": float(worst_hour.iloc[0]["pm2_5"]) if not worst_hour.empty else None,
    }

    summary_df = pd.DataFrame([summary_metrics])
    return summary_df, risk_dist, avg_pm25, avg_sev

def export_trends(df):
    # For each city, keep time, pm2_5, pm10, ozone
    trends = df[["city","time","pm2_5","pm10","ozone"]].dropna(subset=["time"]).sort_values(["city","time"])
    return trends

def save_csvs(summary_df, risk_dist, trends):
    summary_df.to_csv(PROCESSED_DIR / "summary_metrics.csv", index=False)
    risk_dist.to_csv(PROCESSED_DIR / "city_risk_distribution.csv", index=False)
    trends.to_csv(PROCESSED_DIR / "pollution_trends.csv", index=False)
    print("CSV outputs saved to data/processed/")

def make_plots(df):
    # Histogram of PM2.5
    plt.figure(figsize=(8,6))
    df["pm2_5"].dropna().hist(bins=40)
    plt.title("Histogram of PM2.5")
    plt.xlabel("PM2.5 (µg/m³)")
    plt.ylabel("Frequency")

    plt.tight_layout()
    plt.savefig(PROCESSED_DIR / "hist_pm2_5.png")
    plt.close()

    # Bar chart of risk flags per city
    plt.figure(figsize=(10,6))
    risk_counts = df.groupby(["city","risk_flag"]).size().unstack(fill_value=0)
    risk_counts.plot(kind="bar", stacked=False, figsize=(10,6))
    plt.title("Risk Flags per City")
    plt.xlabel("City")
    plt.ylabel("Counts")
    plt.tight_layout()
    plt.savefig(PROCESSED_DIR / "bar_risk_per_city.png")
    plt.close()

    # Line chart of hourly PM2.5 trends (average per hour per city)
    plt.figure(figsize=(12,6))
    hourly = df.groupby(["city","time"]).agg({"pm2_5":"mean"}).reset_index()
    # downsample to hourly-of-day average for clearer line
    hourly["hour"] = hourly["time"].dt.hour
    for city, grp in hourly.groupby("city"):
        grp2 = grp.groupby("hour")["pm2_5"].mean().reset_index()
        plt.plot(grp2["hour"], grp2["pm2_5"], label=city)
    plt.legend()
    plt.title("Hourly Average PM2.5 by City (hour of day)")
    plt.xlabel("Hour of Day")
    plt.ylabel("PM2.5 (µg/m³)")
    plt.tight_layout()
    
    plt.savefig(PROCESSED_DIR / "line_hourly_pm2_5.png")
    plt.close()

    # Scatter: severity_score vs pm2_5
    plt.figure(figsize=(8,6))
    sample = df.dropna(subset=["severity_score","pm2_5"])
    plt.scatter(sample["pm2_5"], sample["severity_score"], alpha=0.6)
    plt.xlabel("PM2.5")
    plt.ylabel("Severity Score")
    plt.title("Severity Score vs PM2.5")
    plt.tight_layout()
    plt.savefig(PROCESSED_DIR / "scatter_severity_pm2_5.png")
    plt.close()
    print("Plots saved to data/processed/")

def main():
    df = fetch_table_as_df()
    if df.empty:
        print("No data found in Supabase table.")
        return

    # KPI metrics
    summary_df, risk_dist, avg_pm25, avg_sev = compute_kpis(df)

    # Trends
    trends = export_trends(df)

    # Save CSVs
    save_csvs(summary_df, risk_dist, trends)

    # Plots
    make_plots(df)

    print("Analysis complete. Files saved under data/processed/")

if __name__ == "__main__":
    main()
