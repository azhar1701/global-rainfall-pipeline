import ee
from .base import BaseSatelliteProvider
from typing import Dict, List

class GPMProvider(BaseSatelliteProvider):
    """
    Provider for GPM (Global Precipitation Measurement) IMERG.
    Dataset ID: NASA/GPM_L3/IMERG_V07
    """
    
    def __init__(self, collection_id: str = "NASA/GPM_L3/IMERG_V07", band: str = "precipitation"):
        self.collection_id = collection_id
        self.band = band

    def get_rainfall_data(self, aoi: ee.Geometry, start_date: str, end_date: str) -> Dict:
        """
        Retrieves GPM IMERG rainfall data for a given AOI and date range.
        """
        collection = ee.ImageCollection(self.collection_id) \
            .filterBounds(aoi) \
            .filterDate(start_date, end_date) \
            .select(self.band) \
            .map(lambda img: img.clip(aoi))
            
        def reduce_to_mean(image):
            # Perform Zonal Statistics (Mean Precipitation)
            stats = image.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=aoi,
                scale=11132,  # ~0.1 degree resolution
                bestEffort=True
            )
            return ee.Feature(None, stats).set('system:time_start', image.get('system:time_start'))

        reduced_collection = collection.map(reduce_to_mean)
        return reduced_collection.getInfo()
