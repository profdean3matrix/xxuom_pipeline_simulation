import os
import pandas as pd

# Path to the Gold Lakehouse file
gold_file_path = "gold_lakehouse/CRM_TASKS_COMPLAINTS.parquet"

# Step 1: Check if file exists
if not os.path.exists(gold_file_path):
    raise FileNotFoundError(f"❌ File not found: {gold_file_path}")

# Step 2: Load file
df = pd.read_parquet(gold_file_path)

# Step 3: Basic validations
expected_columns = [
    "CUSTOMER_PARTY_ID", "SERVICE_AREA", "CREATED_DATE", "DETAILS", "TASK_ID",
    "OWNER_NAME", "CREATED_BY", "STATUS", "TYPE", "PLANNED_START_DATE",
    "PLANNED_END_DATE", "SCHEDULED_START_DATE", "SCHEDULED_END_DATE",
    "ACTUAL_START_DATE", "ACTUAL_END_DATE", "DURATION", "DURATION_UOM",
    "PLANNED_EFFORT", "PLANNED_EFFORT_UOM", "ACTUAL_EFFORT", "ACTUAL_EFFORT_UOM",
    "APPEAL_DECISION", "COMPLAINT", "MISCONDUCT_TYPE", "FTP_OUTCOME",
    "ACADEMIC_PROGRAMME_CODE", "ACADEMIC_PLAN_CODE"
]

missing_columns = [col for col in expected_columns if col not in df.columns]
if missing_columns:
    raise ValueError(f"❌ Missing expected columns: {missing_columns}")

# Step 4: Check non-null values in key columns
key_columns = ["TYPE", "TASK_ID"]
for col in key_columns:
    if df[col].isnull().any():
        raise ValueError(f"❌ Column '{col}' contains null values.")

# Step 5: Verify TYPE values
valid_types = {"Appeal", "Complaint", "Malpractice", "Fitness to Practise"}
actual_types = set(df["TYPE"].unique())
unexpected_types = actual_types - valid_types
if unexpected_types:
    raise ValueError(f"❌ Unexpected TYPE values found: {unexpected_types}")

# Step 6: Check record count
if len(df) == 0:
    raise ValueError("❌ No data found in the Gold output table.")

print("✅ SIT Passed: Gold output is valid and contains expected structure/data.")
input("\nPress Enter to close...")
