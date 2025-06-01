# XXUOM Pipeline Simulation

![CI](https://github.com/profdean3matrix/xxuom_pipeline_simulation/actions/workflows/python-sit-validation.yml/badge.svg)

This project simulates a data pipeline that ingests CRM task data from a CSV file (representing an Oracle CRM source table `UOM_GAPS.XXUOM_TASKS`) into a **Bronze Lakehouse layer**, performs transformations, and outputs a curated dataset to the **Gold Lakehouse layer**. The pipeline is built with Python and Pandas, and follows DevOps-aligned principles including CI/CD and automated System Integration Testing (SIT).

---

## 🚀 Features

- 📥 Copy Data activity simulation (Bronze Lakehouse)
- 🔄 Transform data with business logic (Gold Lakehouse)
- ✅ Automated validation and data profiling (SIT)
- 🔁 GitHub Actions CI/CD pipeline
- 🧪 Testable, modular Python codebase

---

## 🛠️ How to Run Locally

1. **Clone the repository**
   ```bash
   git clone https://github.com/profdean3matrix/xxuom_pipeline_simulation.git
   cd xxuom_pipeline_simulation
   ```

2. **Create and activate a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/Scripts/activate  # or .venv/bin/activate on Mac/Linux
   ```

3. **Install required packages**
   ```bash
   pip install pandas pyarrow
   ```

4. **Run simulation scripts**
   - Copy to Bronze:
     ```bash
     python copy_to_bronze.py
     ```
   - Transform to Gold:
     ```bash
     python transform_to_gold.py
     ```
   - Run SIT Validation:
     ```bash
     python sit_validate_gold_output.py
     ```

---

## 📂 Project Structure

```
├── bronze_lakehouse/
│   └── CRM_UOM_TASKS.parquet
├── gold_lakehouse/
│   └── CRM_TASKS_COMPLAINTS.parquet
├── UOM_GAPS_XXUOM_TASKS.csv          # Source data
├── copy_to_bronze.py                 # Ingest from CSV to Bronze
├── transform_to_gold.py             # Transform Bronze → Gold
├── sit_validate_gold_output.py      # Validate Gold layer (SIT)
├── .github/workflows/               # CI/CD pipeline
│   └── python-sit-validation.yml
└── README.md
```

---

## 🤖 DevOps & CI/CD

- **GitHub Actions** pipeline automatically runs `sit_validate_gold_output.py` on every push/PR to `main`
- Validation checks for schema compliance, duplicates, nulls, and record quality
- Status badge embedded at the top of this file

---

## 👤 Author

Dean @profdean3matrix  
Multiverse Data Academy – Module 2: DevOps Integration in Microsoft Fabric