import numpy as np
import pandas as pd
from scipy import stats
from typing import Dict, Any

def calculate_trend(df: pd.DataFrame, column: str = 'precipitation', min_points: int = 10) -> Dict[str, Any]:
    """
    Calculates the statistical trend of a time series using Mann-Kendall (via Kendall Tau).
    Uses Theil-Sen estimator for the slope.
    """
    if len(df) < min_points:
        return {
            'status': 'insufficient_data',
            'trend': 'unknown',
            'p_value': None,
            'slope': 0.0
        }

    # Prepare data (clean NaNs and ensure numeric float type)
    valid_data = df[column].dropna().astype(float)
    if len(valid_data) < min_points:
        return {
            'status': 'insufficient_data',
            'trend': 'unknown',
            'p_value': None,
            'slope': 0.0
        }

    y = valid_data.values
    x = np.arange(len(y))

    # 1. Kendall Tau (Mann-Kendall equivalent)
    tau, p_value = stats.kendalltau(x, y)

    # 2. Theil-Sen Slope
    res = stats.theilslopes(y, x)
    slope = res[0]

    # Determine trend type
    if p_value < 0.05:
        trend = 'increasing' if slope > 0 else 'decreasing'
    else:
        trend = 'no_trend'

    return {
        'status': 'success',
        'trend': trend,
        'tau': float(tau),
        'p_value': float(p_value),
        'slope': float(slope),
        'intercept': float(res[1])
    }
