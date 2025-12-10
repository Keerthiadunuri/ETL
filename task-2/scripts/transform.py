import os
import pandas as pd
def transform_data():
    # Get project root directory
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Paths
    raw_path=os.path.join(base_dir,"data","raw","churn_raw.csv")
    staged_path=os.path.join(base_dir,"data","staged","churn_transformed.csv")
    if not os.path.exists(raw_path):
        raise FileNotFoundError("Raw data not found. Run extract.py first.")
    # Load dataset
    df = pd.read_csv(raw_path)
    #cleaning
    df["TotalCharges"]=pd.to_numeric(df["TotalCharges"],errors="coerce")
    df["tenure"]=df["tenure"].fillna(df["tenure"].median())
    df["MonthlyCharges"]=df["MonthlyCharges"].fillna(df["MonthlyCharges"].median())
    df["TotalCharges"]=df["TotalCharges"].fillna(df["TotalCharges"].median())
    categorical_cols=df.select_dtypes(include="object").columns
    for col in categorical_cols:
        df[col]=df[col].fillna("Unknown")
    #feature engineering
    #tenure_group
    df["tenure_group"]=pd.cut(df["tenure"],bins=[0,12,36,60,100],labels=["New","Regular","Loyal","Champion"],
        include_lowest=True)
    #monthly_charge_segment
    df["monthly_charge_segment"]=pd.cut(df["MonthlyCharges"],bins=[0,30,70,500],labels=["Low","Medium","High"],
        include_lowest=True)
    #has_internet_service
    df["has_internet_service"]=df["InternetService"].map({"DSL": 1,"Fiber optic": 1,"No": 0})
    #is_multi_line_user
    df["is_multi_line_user"]=(df["MultipleLines"]=="Yes").astype(int)
    # 5. contract_type_code
    df["contract_type_code"] = df["Contract"].map({
        "Month-to-month": 0,
        "One year": 1,
        "Two year": 2
    })
    #drop columns
    df=df.drop(columns=["customerID","gender"],errors="ignore")
    os.makedirs(os.path.dirname(staged_path), exist_ok=True)
    df.to_csv(staged_path, index=False)
    print(f"Transformation complete! File saved at:\n{staged_path}")
    return staged_path
if __name__ == "__main__":
    transform_data()