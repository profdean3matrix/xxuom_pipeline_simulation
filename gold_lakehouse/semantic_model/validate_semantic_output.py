import pandas as pd
import os

file_path = "semantic_model/CRM_TASKS_COMPLAINTS_CLEANED.parquet"

# Validate existence
if not os.path.exists(file_path):
    raise FileNotFoundError(f"❌ Semantic model file not found at {file_path}")

df = pd.read_parquet(file_path)

# Basic checks
required_columns = [
    "CUSTOMER_PARTY_ID", "ACTUAL_TASK_ID", "CREATED_DATE"
]

missing_cols = [col for col in required_columns if col not in df.columns]
if missing_cols:
    raise ValueError(f"❌ Missing required columns: {missing_cols}")

# Check data types
if not pd.api.types.is_string_dtype(df["CUSTOMER_PARTY_ID"]):
    raise TypeError("❌ CUSTOMER_PARTY_ID is not of type string")
if not pd.api.types.is_string_dtype(df["ACTUAL_TASK_ID"]):
    raise TypeError("❌ ACTUAL_TASK_ID is not of type string")

# CREATED_DATE should be datetime.date or string
if not pd.api.types.is_object_dtype(df["CREATED_DATE"]):
    raise TypeError("❌ CREATED_DATE is not formatted as date")

print("✅ Semantic model validation passed.")