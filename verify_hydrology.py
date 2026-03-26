
import pandas as pd
import numpy as np
from src.pipeline.analytics import calculate_spi, calculate_ensemble_metrics

def test_spi():
    print("Testing SPI Calculation...")
    # Create 90 days of synthetic rainfall data
    dates = pd.date_range("2024-01-01", periods=90)
    # Gamma-like distribution (mostly dry with occasional rain)
    rain = np.random.gamma(2, 2, 90)
    df = pd.DataFrame({'date': dates, 'precipitation': rain})
    
    result = calculate_spi(df)
    print(f"SPI Samples: {result['spi_30'].dropna().head().tolist()}")
    assert 'spi_30' in result.columns
    assert not result['spi_30'].dropna().empty
    print("SPI Test Passed.\n")

def test_ensemble():
    print("Testing Ensemble Metrics...")
    # Create merged dataframe
    df = pd.DataFrame({
        'date': pd.date_range("2024-01-01", periods=10),
        'precip_chirps': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'precip_gpm': [1.1, 1.9, 3.2, 3.8, 5.5, 5.9, 7.1, 8.2, 8.8, 10.5]
    })
    
    metrics = calculate_ensemble_metrics(df)
    print(f"Metrics: {metrics}")
    assert metrics['status'] == 'success'
    assert metrics['correlation'] > 0.9
    assert metrics['scientific_confidence'] == 'High'
    print("Ensemble Metrics Test Passed.\n")

if __name__ == "__main__":
    try:
        test_spi()
        test_ensemble()
        print("ALL ANALYTICS VERIFIED.")
    except Exception as e:
        print(f"VERIFICATION FAILED: {e}")
        exit(1)
