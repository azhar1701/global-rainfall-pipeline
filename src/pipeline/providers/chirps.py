import ee
from .base import BaseSatelliteProvider
from typing import Dict, List, Optional, TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from src.pipeline.client import GEEClient

class CHIRPSProvider(BaseSatelliteProvider):
    """
    Provider for CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data).
    Dataset ID: UCSB-CHG/CHIRPS/DAILY
    """
    
    def __init__(self, collection_id: str = "UCSB-CHG/CHIRPS/DAILY", band: str = f"precipitation", client: Optional['GEEClient'] = None):
        self.collection_id = collection_id
        self.band = band
        self.client = client

    def get_rainfall_data(self, aoi: ee.Geometry, start_date: str, end_date: str, progress_callback: Optional[Callable[[int, int], None]] = None, _is_chunk: bool = False) -> Dict:
        """
        Retrieves CHIRPS rainfall data for a given AOI and date range.
        """
        if self.client and not _is_chunk:
            return self.client.fetch_in_chunks(
                self, aoi, start_date, end_date, 
                chunk_days=30, max_workers=8, 
                progress_callback=progress_callback
            )
        
        # Fallback for direct usage
        collection = ee.ImageCollection(self.collection_id) \
            .filterBounds(aoi) \
            .filterDate(start_date, end_date) \
            .select(self.band)
            
        def reduce_to_mean(image):
            # Perform Zonal Statistics (Mean Precipitation)
            # reduceRegion is more efficient without a preceding clip()
            stats = image.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=aoi,
                scale=5566,  # ~0.05 degree resolution (CHIRPS native)
                bestEffort=True,
                maxPixels=1e9,
                tileScale=4
            )
            return ee.Feature(None, stats).set('system:time_start', image.get('system:time_start'))

        reduced_collection = collection.map(reduce_to_mean)
        
        if self.client:
            return self.client.get_info(reduced_collection)
        return reduced_collection.getInfo()
