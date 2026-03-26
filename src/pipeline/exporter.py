import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

def export_data(df: pd.DataFrame, filepath: str, format: str) -> None:
    """
    Exports a DataFrame to a file in the specified format (CSV or Parquet).
    """
    if format.lower() == 'csv':
        df.to_csv(filepath, index=False)
        logger.info(f"Data exported to {filepath} in CSV format.")
    elif format.lower() == 'parquet':
        df.to_parquet(filepath, index=False)
        logger.info(f"Data exported to {filepath} in Parquet format.")
    else:
        raise ValueError(f"Unsupported format: {format}. Supported formats are 'csv' and 'parquet'.")

def export_binary(content: bytes, filepath: str) -> None:
    """
    Writes raw binary content (e.g. GeoTIFF) to a file.
    """
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            f.write(content)
        logger.info(f"Binary data exported to {filepath} ({len(content)} bytes).")
    except Exception as e:
        logger.error(f"Failed to export binary data to {filepath}: {e}")
        raise e
