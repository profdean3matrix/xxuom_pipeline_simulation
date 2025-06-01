import pandas as pd
import pytest
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from transform_to_gold import transform_data  # You will modularise this from main script

@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        "TYPE": ["Appeal", "Complaint", "Malpractice"],
        "ATTRIBUTE1": ["Accepted", "Rudeness", "Plagiarism"],
        "ATTRIBUTE2": ["Approved", None, None],
        "ATTRIBUTE3": ["Academic Misjudgement", None, None],
        "ATTRIBUTE4": ["Medical Evidence", None, None],
        "ATTRIBUTE5": ["Mitigating Circumstances", None, None],
        "ATTRIBUTE6": ["Exam Board Error", None, None],
        "ATTRIBUTE7": ["Technical Fault", None, None],
        "ATTRIBUTE8": ["Bias", None, None],
        "ATTRIBUTE9": ["Extenuating Factors", None, None],
        "ATTRIBUTE10": ["Procedure Error", None, None],
        "ATTRIBUTE11": ["Wrong Info", None, None],
        "ATTRIBUTE12": ["System Crash", None, None],
        "ATTRIBUTE13": ["Staff Misconduct", None, None],
        "ATTRIBUTE14": ["Wrong Module", None, None],
        "ATTRIBUTE15": ["ABC01XYZ01", "DEF02XYZ02", "GHI03XYZ03"],
        "TASK_ID": [101, 102, 103]
    })

def test_column_derivations(sample_dataframe):
    df_transformed = transform_data(sample_dataframe)

    assert "APPEAL_DECISION" in df_transformed.columns
    assert df_transformed["APPEAL_DECISION"][0] == "Accepted"

    assert "COMPLAINT" in df_transformed.columns
    assert df_transformed["COMPLAINT"][1] == "Rudeness"

    assert "MISCONDUCT_TYPE" in df_transformed.columns
    assert df_transformed["MISCONDUCT_TYPE"][2] == "Plagiarism"

def test_academic_code_extraction(sample_dataframe):
    df_transformed = transform_data(sample_dataframe)

    assert df_transformed["ACADEMIC_PROGRAMME_CODE"][0] == "ABC01"
    assert df_transformed["ACADEMIC_PLAN_CODE"][0] == "XYZ01"