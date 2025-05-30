import pandas as pd
import os

# Load source (CSV from Oracle CRM system)
df_source = pd.read_csv("UOM_GAPS_XXUOM_TASKS.csv")

# Load destination (Parquet from Bronze Lakehouse)
df_bronze = pd.read_parquet("bronze_lakehouse/CRM_UOM_TASKS.parquet")

# Run basic System Integration Tests (SIT)
test_results = {
    "Row Count Match": len(df_source) == len(df_bronze),
    "Column Match": list(df_source.columns) == list(df_bronze.columns),
    "Sample Data Match (first 5 rows)": df_source.head().equals(df_bronze.head())
}

# Print results
print("\n✅ SYSTEM INTEGRATION TEST RESULTS ✅\n")
for test, result in test_results.items():
    print(f"{test}: {'✅ PASS' if result else '❌ FAIL'}")

# Optional: Output differences if any test fails
if not all(test_results.values()):
    print("\n🔍 Differences (if any):")
    if len(df_source) != len(df_bronze):
        print(f"- Source rows: {len(df_source)}, Bronze rows: {len(df_bronze)}")
    if list(df_source.columns) != list(df_bronze.columns):
        print(f"- Source columns: {df_source.columns.tolist()}")
        print(f"- Bronze columns: {df_bronze.columns.tolist()}")
    if not df_source.head().equals(df_bronze.head()):
        print("- Sample data mismatch in the first 5 rows.")