from typing import Dict, List, Union, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from src.pipeline.analytics import calculate_spi

def fill_missing_reciprocal(series: pd.Series, power: float = 1.0, window: int = 14, max_gap: int = 3) -> pd.Series:
    """
    Fills missing values in a 1D time series using the Reciprocal Method (Inverse Distance Weighting).
    Weights are calculated using the reciprocal of the temporal index distance: 1 / (distance^power).
    Only interpolates if the gap size is <= max_gap.
    """
    filled = series.copy()
    missing_mask = filled.isna()
    if not missing_mask.any():
        return filled

    # Group consecutive NaNs to find gap sizes
    is_missing = missing_mask.astype(int)
    gap_groups = (is_missing != is_missing.shift()).cumsum()
    gap_sizes = is_missing.groupby(gap_groups).transform('sum')

    # Only interpolate missing values that belong to a gap <= max_gap
    interpolable_mask = missing_mask & (gap_sizes <= max_gap)
    
    valid_indices = np.where(~missing_mask)[0]
    missing_indices = np.where(interpolable_mask)[0]

    if len(valid_indices) == 0:
        # Cannot interpolate if the entire series is empty
        return filled

    for missing_idx in missing_indices:
        dists = np.abs(valid_indices - missing_idx)
        mask_window = dists <= window
        
        valid_in_window = valid_indices[mask_window]
        dists_in_window = dists[mask_window]
        
        if len(valid_in_window) == 0:
            # Fallback to the nearest valid points if window is entirely empty
            valid_in_window = valid_indices
            dists_in_window = dists
            
        weights = 1.0 / (dists_in_window ** power)
        vals = filled.iloc[valid_in_window].values
        
        interpolated = np.sum(weights * vals) / np.sum(weights)
        filled.iloc[missing_idx] = interpolated
        
    return filled

def process_rainfall_data(raw_data: Union[Dict, List[Dict]], start_date: Optional[str] = None, end_date: Optional[str] = None, timezone: str = 'UTC') -> pd.DataFrame:
    """
    Converts GEE FeatureCollection JSON to DataFrame, handles missing values, and sets timezone.
    """
    if not raw_data:
        return pd.DataFrame(columns=['date', 'precipitation'])

    # Determine if raw_data is a GEE FeatureCollection dict
    parsed_data = []
    if isinstance(raw_data, dict) and 'type' in raw_data and raw_data['type'] == 'FeatureCollection':
        features = raw_data.get('features', [])
        for feat in features:
            props = feat.get('properties', {})
            # Earth engine time_start is in ms
            ts = props.get('system:time_start')
            if ts is not None:
                dt = pd.to_datetime(ts, unit='ms', utc=True)
            else:
                dt = None
            
            # Known rainfall property names
            known_keys = ['precipitation', 'precip', 'HQprecipitation', 'daily_precipitation']
            precip_val = None
            
            # First try known keys
            for k in known_keys:
                if k in props:
                    precip_val = props[k]
                    break
            
            # Fallback: first non-system property that is numeric
            if precip_val is None:
                for key, val in props.items():
                    # Skip failed markers and internal booleans
                    if key == 'is_failed' or key == 'error':
                        continue
                    if not key.startswith('system:') and isinstance(val, (int, float)) and not isinstance(val, bool):
                        precip_val = val
                        break
                    
            parsed_data.append({'date': dt, 'precipitation': precip_val})
            
        df = pd.DataFrame(parsed_data)
    else:
        # Fallback if already an array
        df = pd.DataFrame(raw_data)
    
    # Ensure columns exist
    if 'date' not in df.columns:
        df['date'] = pd.Series(dtype='datetime64[ns]')
    if 'precipitation' not in df.columns:
        df['precipitation'] = pd.Series(dtype='float64')

    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Handle timezone
    if df['date'].dt.tz is None:
        df['date'] = df['date'].dt.tz_localize('UTC')
    
    df['date'] = df['date'].dt.tz_convert(timezone)
    
    # Handle precipitation
    df['precipitation'] = pd.to_numeric(df['precipitation'], errors='coerce')
    
    df = df[['date', 'precipitation']].copy()
    
    # Sort by date to ensure rolling calculations and reciprocal indexing are correct
    df = df.sort_values(by='date').reset_index(drop=True)
    
    # Enforce strict daily frequency over the requested date range
    if start_date and end_date:
        full_index = pd.date_range(
            start=pd.to_datetime(start_date).tz_localize('UTC').tz_convert(timezone),
            end=pd.to_datetime(end_date).tz_localize('UTC').tz_convert(timezone),
            freq='D'
        )
        df.set_index('date', inplace=True)
        # Drop duplicates just in case (e.g. multiple failed chunk markers on same date)
        df = df[~df.index.duplicated(keep='first')]
        df = df.reindex(full_index)
        df.index.name = 'date'
        df.reset_index(inplace=True)
    
    # Impute missing values with the requested 'Metode Resiprocal' (Inverse Distance Weighting)
    df['precipitation'] = fill_missing_reciprocal(df['precipitation'], power=1.0, window=14)
    
    # 1. 7-Day Rolling Average
    df['rolling_avg_7d'] = df['precipitation'].rolling(window=7, min_periods=1).mean()
    
    # 2. Anomaly Detection (Z-Score > 2.0 or < -2.0)
    mean_precip = df['precipitation'].mean()
    std_precip = df['precipitation'].std()
    
    if pd.notna(std_precip) and std_precip > 0:
        df['z_score'] = (df['precipitation'] - mean_precip) / std_precip
    else:
        df['z_score'] = 0.0
        
    df['is_anomaly'] = df['z_score'].abs() > 2.0
    
    # 3. SPI-30 (Standardized Precipitation Index)
    df = calculate_spi(df, column='precipitation', window=30)
    
    return df

def validate_row_count(df: pd.DataFrame, start_date: str, end_date: str) -> bool:
    """
    Validates if the DataFrame row count matches the expected number of days.
    
    Args:
        df (pd.DataFrame): The processed DataFrame.
        start_date (str): Start date string (YYYY-MM-DD).
        end_date (str): End date string (YYYY-MM-DD).
        
    Returns:
        bool: True if counts match, False otherwise.
    """
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        expected_days = (end - start).days + 1
        return len(df) == expected_days
    except Exception:
        return False
