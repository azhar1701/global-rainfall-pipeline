import pandas as pd
import logging

logger = logging.getLogger(__name__)

def export_data(df: pd.DataFrame, filepath: str, format: str) -> None:
    """
    Exports a DataFrame to a file in the specified format (CSV or Parquet).

    Args:
        df (pd.DataFrame): The DataFrame to export.
        filepath (str): The destination file path.
        format (str): The format to use ('csv' or 'parquet').

    Raises:
        ValueError: If the format is not supported.
    """
    if format.lower() == 'csv':
        df.to_csv(filepath, index=False)
        logger.info(f"Data exported to {filepath} in CSV format.")
    elif format.lower() == 'parquet':
        df.to_parquet(filepath, index=False)
        logger.info(f"Data exported to {filepath} in Parquet format.")
    else:
        raise ValueError(f"Unsupported format: {format}. Supported formats are 'csv' and 'parquet'.")
