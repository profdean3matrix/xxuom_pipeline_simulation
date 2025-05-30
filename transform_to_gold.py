import os
import pandas as pd

# Step 1: Create Gold folder
os.makedirs("gold_lakehouse", exist_ok=True)

# Step 2: Load Bronze Data
bronze_df = pd.read_parquet("bronze_lakehouse/CRM_UOM_TASKS.parquet")

# Step 3: Filter rows
filtered_df = bronze_df[bronze_df["TYPE"].isin(
    ["Appeal", "Complaint", "Malpractice", "Fitness to Practise"]
)]

# Step 4: Transform fields
filtered_df["APPEAL_DECISION"] = filtered_df.apply(lambda row: row["ATTRIBUTE1"] if row["TYPE"] == "Appeal" else None, axis=1)
filtered_df["APPEAL_OUTCOME"] = filtered_df.apply(lambda row: row["ATTRIBUTE2"] if row["TYPE"] == "Appeal" else None, axis=1)
filtered_df["APPEAL_GROUNDS_CLAIMED"] = filtered_df.apply(lambda row: row["ATTRIBUTE3"] if row["TYPE"] == "Appeal" else None, axis=1)
filtered_df["APPEAL_GROUNDS_CLAIMED_2"] = filtered_df.apply(lambda row: row["ATTRIBUTE4"] if row["TYPE"] == "Appeal" else None, axis=1)
filtered_df["APPEAL_GROUNDS_CLAIMED_3"] = filtered_df.apply(lambda row: row["ATTRIBUTE5"] if row["TYPE"] == "Appeal" else None, axis=1)
filtered_df["APPEAL_GROUNDS_CLAIMED_4"] = filtered_df.apply(lambda row: row["ATTRIBUTE6"] if row["TYPE"] == "Appeal" else None, axis=1)

filtered_df["COMPLAINT"] = filtered_df.apply(lambda row: row["ATTRIBUTE1"] if row["TYPE"] == "Complaint" else None, axis=1)
filtered_df["COMPLAINT_OUTCOME"] = filtered_df.apply(lambda row: row["ATTRIBUTE2"] if row["TYPE"] == "Complaint" else None, axis=1)
filtered_df["COMPLAINT_RESOLUTION"] = filtered_df.apply(lambda row: row["ATTRIBUTE3"] if row["TYPE"] == "Complaint" else None, axis=1)
filtered_df["COMPLAINT_OUTCOME_REFERRED_TO"] = filtered_df.apply(lambda row: row["ATTRIBUTE4"] if row["TYPE"] == "Complaint" else None, axis=1)
filtered_df["COMPLAINT_RES_COMP_AMNT"] = filtered_df.apply(lambda row: row["ATTRIBUTE5"] if row["TYPE"] == "Complaint" else None, axis=1)
filtered_df["COMPLAINT_RES_REF_AMNT"] = filtered_df.apply(lambda row: row["ATTRIBUTE6"] if row["TYPE"] == "Complaint" else None, axis=1)

filtered_df["MISCONDUCT_TYPE"] = filtered_df.apply(lambda row: row["ATTRIBUTE1"] if row["TYPE"] == "Malpractice" else None, axis=1)
filtered_df["MALPRACTICE_OUTCOME"] = filtered_df.apply(lambda row: row["ATTRIBUTE2"] if row["TYPE"] == "Malpractice" else None, axis=1)
filtered_df["MALPRACTICE_PENALTY"] = filtered_df.apply(lambda row: row["ATTRIBUTE3"] if row["TYPE"] == "Malpractice" else None, axis=1)
filtered_df["MALPRACTICE_PEN_AMNT"] = filtered_df.apply(lambda row: row["ATTRIBUTE4"] if row["TYPE"] == "Malpractice" else None, axis=1)
filtered_df["MALPRACTICE_PENALTY_2"] = filtered_df.apply(lambda row: row["ATTRIBUTE5"] if row["TYPE"] == "Malpractice" else None, axis=1)
filtered_df["MALPRACTICE_PENALTY_3"] = filtered_df.apply(lambda row: row["ATTRIBUTE6"] if row["TYPE"] == "Malpractice" else None, axis=1)
filtered_df["MALPRACTICE_PENALTY_4"] = filtered_df.apply(lambda row: row["ATTRIBUTE7"] if row["TYPE"] == "Malpractice" else None, axis=1)
filtered_df["MALPRACTICE_PENALTY_5"] = filtered_df.apply(lambda row: row["ATTRIBUTE8"] if row["TYPE"] == "Malpractice" else None, axis=1)

filtered_df["FTP_OUTCOME"] = filtered_df.apply(lambda row: row["ATTRIBUTE1"] if row["TYPE"] == "Fitness to Practise" else None, axis=1)
filtered_df["FTP_PENALTY"] = filtered_df.apply(lambda row: row["ATTRIBUTE2"] if row["TYPE"] == "Fitness to Practise" else None, axis=1)

filtered_df["ACADEMIC_PROGRAMME_CODE"] = filtered_df["ATTRIBUTE15"].astype(str).str[0:5]
filtered_df["ACADEMIC_PLAN_CODE"] = filtered_df["ATTRIBUTE15"].astype(str).str[5:10]

# Step 5: Save to Gold (optional, add this line only if saving is required)
filtered_df.to_parquet("gold_lakehouse/CRM_TASKS_COMPLAINTS.parquet", index=False)