import os

# Re-define directory structure after code execution reset
workflow_dir = ".github/workflows"
os.makedirs(workflow_dir, exist_ok=True)

# Define the YAML workflow content
workflow_content = """name: Semantic Model Validation

on:
  push:
    branches:
      - main
      - feature/semantic-model
  pull_request:

jobs:
  validate-semantic-model:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout repository
      uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install pandas pyarrow

    - name: Run Semantic Validation
      run: |
        python semantic_model/validate_semantic_output.py
"""

# Save the workflow to a file
workflow_path = os.path.join(workflow_dir, "validate_semantic_model.yml")
with open(workflow_path, "w") as f:
    f.write(workflow_content)

workflow_path