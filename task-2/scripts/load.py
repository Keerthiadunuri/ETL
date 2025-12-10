import os
import pandas as pd
import numpy as np
import time
from supabase import create_client, Client
from postgrest.exceptions import APIError
from dotenv import load_dotenv
load_dotenv()
SUPABASE_URL=os.getenv("SUPABASE_URL")
SUPABASE_KEY=os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY environment variables.")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
def create_table():
    try:
        # Check if table exists
        supabase.table("churn_data").select("*").limit(1).execute()
        print("Table already exists: churn_data")
        return
    except APIError:
        print("Table not found — creating it...")
        create_sql = """
        CREATE TABLE churn_data (
            id BIGSERIAL PRIMARY KEY,
            tenure INTEGER,
            monthlycharges FLOAT,
            totalcharges FLOAT,
            churn TEXT,
            internetservice TEXT,
            contract TEXT,
            paymentmethod TEXT,
            tenure_group TEXT,
            monthly_charge_segment TEXT,
            has_internet_service INTEGER,
            is_multi_line_user INTEGER,
            contract_type_code INTEGER
        );
        """
        try:
            supabase.postgrest.rpc("execute_sql", {"sql": create_sql}).execute()
            print("Table created successfully!")
        except Exception as e:
            print("Error creating table using RPC:", e)
            print("➡ Make sure you created the SQL function execute_sql in Supabase:")
            print("""Run inside Supabase SQL editor""")
            exit()
def load_data():
    base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_path=os.path.join(base_dir, "data","staged","churn_transformed.csv")
    if not os.path.exists(staged_path):
        raise FileNotFoundError(" Staged file not found. Run transform.py first.")
    df = pd.read_csv(staged_path)
    df.columns = df.columns.str.lower()
    allowed_cols = [
    "tenure", "monthlycharges", "totalcharges", "churn",
    "internetservice", "contract", "paymentmethod",
    "tenure_group", "monthly_charge_segment",
    "has_internet_service", "is_multi_line_user",
    "contract_type_code"
    ]
    df = df[allowed_cols]
    df = df.replace({np.nan: None})
    rows = df.to_dict(orient="records")
    total = len(rows)
    batch_size = 200

    print(f" Uploading {total} records to Supabase...")

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = rows[start:end]

        attempts = 0
        success = False

        while attempts < 3 and not success:
            try:
                supabase.table("churn_data").insert(batch).execute()
                print(f" Uploaded batch {start} → {end}")
                success = True

            except Exception as e:
                attempts += 1
                print(f" Retry {attempts}/3 for batch {start}-{end}")
                print("Error:", e)
                time.sleep(2)

        if not success:
            print(f" Could NOT upload batch {start}-{end} even after retries.")
            break

    print("🎉 Upload completed successfully!")


# ---------------------------------------------------------
# 3️⃣ MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    create_table()
    load_data()