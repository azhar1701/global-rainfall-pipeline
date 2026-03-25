import argparse
import sys
import logging
import json
import ee
from pathlib import Path
from typing import Optional

from .config import load_config
from .auth import authenticate_gee
from .providers.chirps import CHIRPSProvider
from .providers.gpm import GPMProvider
from .processor import process_rainfall_data
from .exporter import export_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_aoi(geojson_path: str) -> ee.Geometry:
    """
    Loads a GeoJSON file and returns an Earth Engine Geometry object.
    For simplicity, if it's a FeatureCollection, it takes the geometry of the first feature.
    """
    try:
        with open(geojson_path, 'r') as f:
            data = json.load(f)
        
        if data.get('type') == 'FeatureCollection':
            features = data.get('features', [])
            if not features:
                raise ValueError("FeatureCollection is empty")
            return ee.Geometry(features[0]['geometry'])
        elif data.get('type') == 'Feature':
            return ee.Geometry(data['geometry'])
        else:
            # Assume it's a geometry object
            return ee.Geometry(data)
    except Exception as e:
        logger.error(f"Failed to load AOI from {geojson_path}: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Global Rainfall Data Pipeline CLI")
    
    # Arguments
    parser.add_argument("--aoi", type=str, help="Path to AOI GeoJSON file")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--provider", type=str, choices=['chirps', 'gpm'], help="Rainfall data provider")
    parser.add_argument("--output", type=str, required=True, help="Output file path")
    parser.add_argument("--format", type=str, choices=['csv', 'parquet'], default='csv', help="Output format")

    args = parser.parse_args()

    try:
        # 1. Load Config
        config = load_config()

        # 2. Override Config with Args
        if args.aoi:
            config.aoi.geojson_path = args.aoi
        if args.start_date:
            config.date_range.start_date = args.start_date
        if args.end_date:
            config.date_range.end_date = args.end_date

        provider_name = args.provider if args.provider else "chirps" # Default or from config? config doesn't specify default provider type in root.

        # Validate required parameters
        if not config.aoi.geojson_path:
            logger.error("AOI path is required (via --aoi or config).")
            sys.exit(1)
        if not config.date_range.start_date or not config.date_range.end_date:
            logger.error("Start and End dates are required (via args or config).")
            sys.exit(1)

        # 3. Authenticate
        authenticate_gee()

        # 4. Load AOI
        aoi = load_aoi(config.aoi.geojson_path)

        # 5. Get Provider
        if provider_name.lower() == 'chirps':
            provider = CHIRPSProvider()
        elif provider_name.lower() == 'gpm':
            provider = GPMProvider()
        else:
            logger.error(f"Unknown provider: {provider_name}")
            sys.exit(1)

        # 6. Fetch Data
        logger.info(f"Fetching data from {provider_name} for {config.date_range.start_date} to {config.date_range.end_date}...")
        raw_data = provider.get_rainfall_data(aoi, config.date_range.start_date, config.date_range.end_date)
        
        # 7. Process Data
        logger.info("Processing data...")
        df = process_rainfall_data(raw_data, timezone=config.timezone)

        # 8. Export Data
        logger.info(f"Exporting data to {args.output}...")
        export_data(df, args.output, args.format)
        
        logger.info("Pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
