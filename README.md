# XXUOM Pipeline Simulation

This project simulates a data pipeline that copies data from a CSV file (representing Oracle CRM table `UOM_GAPS.XXUOM_TASKS`) into a Bronze Lakehouse layer (`CRM_UOM_TASKS`) using Python and Pandas.

To simulate a Copy Data activity in Microsoft Fabric using local tools like VS Code, with a DevOps-aligned workflow including CI/CD and SIT.

1. Clone the repository
2. Create and activate a virtual environment
3. Install requirements:
   ```bash
   pip install -r requirements.txt

   python copy_to_bronze.py
