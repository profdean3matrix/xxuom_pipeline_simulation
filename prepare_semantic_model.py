import os
import pandas as pd

# Define paths
gold_input_path = "gold_lakehouse/CRM_TASKS_COMPLAINTS.parquet"
semantic_dir = os.path.join("gold_lakehouse", "semantic_model")
semantic_output_path = os.path.join(semantic_dir, "CRM_TASKS_COMPLAINTS_CLEANED.parquet")

# Step 1: Load the Gold layer data
df = pd.read_parquet(gold_input_path)

# Step 2: Format CREATED_DATE to exclude time
df["CREATED_DATE"] = pd.to_datetime(df["CREATED_DATE"]).dt.date  # Convert to date only

# Step 3: Convert IDs to string (Text)
df["CUSTOMER_PARTY_ID"] = df["CUSTOMER_PARTY_ID"].astype(str)
df["ACTUAL_TASK_ID"] = df["ACTUAL_TASK_ID"].astype(str)

# Step 4: Create semantic folder inside gold_lakehouse and save
os.makedirs(semantic_dir, exist_ok=True)
df.to_parquet(semantic_output_path, index=False)

print("✅ Semantic Model prepared and saved to:", semantic_output_path)