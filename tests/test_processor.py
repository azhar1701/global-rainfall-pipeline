import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from src.pipeline.processor import process_rainfall_data, validate_row_count

@pytest.fixture
def sample_gee_data():
    return [
        {'date': '2023-01-01', 'precipitation': 5.5},
        {'date': '2023-01-02', 'precipitation': 0.0},
        {'date': '2023-01-03', 'precipitation': None}, # Missing data
        {'date': '2023-01-04', 'precipitation': 10.2}
    ]

def test_process_rainfall_data_structure(sample_gee_data):
    """Test if the output is a DataFrame with correct columns."""
    df = process_rainfall_data(sample_gee_data, timezone='UTC')
    assert isinstance(df, pd.DataFrame)
    assert 'date' in df.columns
    assert 'precipitation' in df.columns

def test_process_rainfall_data_timezone_conversion(sample_gee_data):
    """Test if the date column is converted to datetime and has correct timezone."""
    df = process_rainfall_data(sample_gee_data, timezone='Asia/Tokyo')
    # Check if dtype is datetime-like
    assert pd.api.types.is_datetime64_any_dtype(df['date'])
    # Check timezone (if your implementation localizes or just keeps it naive but handled)
    # The requirement says "Set timezone".
    # Assuming the input 'date' is just a date string, usually YYYY-MM-DD.
    # If we localize, we expect the dt accessor to have tz info.
    assert df['date'].dt.tz.zone == 'Asia/Tokyo'

def test_process_rainfall_data_nan_handling(sample_gee_data):
    """Test that missing precipitation values become NaN, not 0."""
    df = process_rainfall_data(sample_gee_data, timezone='UTC')
    missing_row = df[df['date'].dt.strftime('%Y-%m-%d') == '2023-01-03']
    assert len(missing_row) == 1
    val = missing_row.iloc[0]['precipitation']
    assert np.isnan(val)
    
    zero_row = df[df['date'].dt.strftime('%Y-%m-%d') == '2023-01-02']
    assert zero_row.iloc[0]['precipitation'] == 0.0

def test_validate_row_count_valid():
    """Test validation with correct row count."""
    dates = pd.date_range(start='2023-01-01', end='2023-01-03')
    df = pd.DataFrame({'date': dates, 'precipitation': [1, 2, 3]})
    assert validate_row_count(df, '2023-01-01', '2023-01-03') is True

def test_validate_row_count_invalid_missing_dates():
    """Test validation with missing dates (row count mismatch)."""
    dates = pd.date_range(start='2023-01-01', end='2023-01-02') # Only 2 days
    df = pd.DataFrame({'date': dates, 'precipitation': [1, 2]})
    # Expected 3 days (1st, 2nd, 3rd)
    assert validate_row_count(df, '2023-01-01', '2023-01-03') is False

def test_validate_row_count_invalid_extra_dates():
    """Test validation with extra dates (row count mismatch)."""
    dates = pd.date_range(start='2023-01-01', end='2023-01-04') # 4 days
    df = pd.DataFrame({'date': dates, 'precipitation': [1, 2, 3, 4]})
    # Expected 3 days
    assert validate_row_count(df, '2023-01-01', '2023-01-03') is False
