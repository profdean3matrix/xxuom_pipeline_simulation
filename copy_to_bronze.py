import os
import pandas as pd

# Create output directory if it doesn't exist
os.makedirs("bronze_lakehouse", exist_ok=True)

# Load CSV
df = pd.read_csv("UOM_GAPS_XXUOM_TASKS.csv")

# Optional: Preview first 5 rows
print(df.head())

# Save as Parquet in bronze lakehouse directory
df.to_parquet("bronze_lakehouse/CRM_UOM_TASKS.parquet", index=False)

print("✅ Data successfully written to bronze_lakehouse/CRM_UOM_TASKS.parquet")
