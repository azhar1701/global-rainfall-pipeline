import pytest
import numpy as np
import pandas as pd
from src.pipeline.analytics import calculate_trend

def test_calculate_trend_upward():
    # Synthetic upward trend
    data = np.arange(20, dtype=float) + np.random.normal(0, 0.1, 20)
    df = pd.DataFrame({'precipitation': data})
    
    result = calculate_trend(df)
    
    assert result['trend'] == 'increasing'
    assert result['p_value'] < 0.05
    assert 'slope' in result

def test_calculate_trend_downward():
    # Synthetic downward trend
    data = 20 - np.arange(20, dtype=float) + np.random.normal(0, 0.1, 20)
    df = pd.DataFrame({'precipitation': data})
    
    result = calculate_trend(df)
    
    assert result['trend'] == 'decreasing'
    assert result['p_value'] < 0.05

def test_calculate_trend_no_trend():
    # Random noise (no trend)
    data = np.random.normal(10, 1.0, 50)
    df = pd.DataFrame({'precipitation': data})
    
    result = calculate_trend(df)
    
    assert result['trend'] == 'no_trend'
    assert result['p_value'] >= 0.05

def test_calculate_trend_insufficient_data():
    # Only 5 points
    df = pd.DataFrame({'precipitation': [1, 2, 3, 4, 5]})
    
    result = calculate_trend(df, min_points=10)
    
    assert result['status'] == 'insufficient_data'
    assert result['trend'] == 'unknown'
