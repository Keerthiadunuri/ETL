"""
Full ETL Pipeline Runner
Runs: Extract → Transform → Load → Analysis
"""
"""
Full ETL Pipeline Runner
Runs: Extract → Transform → Load → Analysis
"""

import time
import traceback

from extract import extract_air_quality       # correct function in your extract.py
from transform import transform_data          # transform_data() takes NO arguments
from load import load_data                    # load_data() takes NO arguments
from etl_analysis import main as run_analysis # Analysis main()
    

def run_step(step_name, step_function, *args):
    """
    Utility wrapper: Runs each ETL step and prints logs.
    """
    print(f"\n🔵 Starting: {step_name} ...")

    start = time.time()
    try:
        result = step_function(*args)
        end = time.time()
        print(f"🟢 Completed: {step_name} in {round(end - start, 2)} seconds")
        return result

    except Exception as e:
        print(f"❌ ERROR in {step_name}: {e}")
        print(traceback.format_exc())
        raise SystemExit(f"⛔ Pipeline stopped at: {step_name}")


def main():
    print("\n=================================")
    print(" 🚀 AIR QUALITY ETL PIPELINE ")
    print("=================================\n")

    # 1️⃣ EXTRACT
    run_step("Extract Step", extract_air_quality)

    # 2️⃣ TRANSFORM
    run_step("Transform Step", transform_data)

    # 3️⃣ LOAD
    run_step("Load Step (Supabase)", load_data)

    # 4️⃣ ANALYSIS
    run_step("Analysis Step", run_analysis)

    print("\n🎉 PIPELINE FINISHED SUCCESSFULLY!")
    print("📌 Executed: extract → transform → load → analysis\n")


if __name__ == "__main__":
    main()
