import ee
from .base import BaseSatelliteProvider
from typing import Dict, List, Optional, TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from src.pipeline.client import GEEClient

class GPMProvider(BaseSatelliteProvider):
    """
    Provider for GPM (Global Precipitation Measurement) IMERG.
    Dataset ID: NASA/GPM_L3/IMERG_V07
    """
    
    def __init__(self, collection_id: str = "NASA/GPM_L3/IMERG_V07", band: str = "precipitation", client: Optional['GEEClient'] = None):
        self.collection_id = collection_id
        self.band = band
        self.client = client

    def get_rainfall_data(self, aoi: ee.Geometry, start_date: str, end_date: str, progress_callback: Optional[Callable[[int, int], None]] = None, _is_chunk: bool = False) -> Dict:
        """
        Retrieves GPM IMERG rainfall data for a given AOI and date range.
        Optimized by aggregating 30-minute data to daily sums on the server side.
        """
        if self.client and not _is_chunk:
            return self.client.fetch_in_chunks(
                self, aoi, start_date, end_date, 
                chunk_days=15, max_workers=6, 
                progress_callback=progress_callback
            )

        # Fallback
        collection = ee.ImageCollection(self.collection_id) \
            .filterBounds(aoi) \
            .filterDate(start_date, end_date) \
            .select(self.band)
            
        # 1. Temporal Aggregation: Group by day and mean (hourly rate)
        # GPM IMERG is 0.1 degree, 30-minute.
        diff = ee.Date(end_date).difference(ee.Date(start_date), 'day')
        days = ee.List.sequence(0, diff.subtract(1))
        
        def aggregate_daily(day_offset):
            date = ee.Date(start_date).advance(day_offset, 'day')
            daily_coll = collection.filterDate(date, date.advance(1, 'day'))
            
            # Use mean of the precipitation rate (mm/hr) and multiply by 24 for daily total
            # We set the time_start to the beginning of the day for consistency
            return daily_coll.mean().multiply(24) \
                .set('system:time_start', date.millis())

        daily_collection = ee.ImageCollection.fromImages(days.map(aggregate_daily))

        # 2. Zonal Statistics (Spatial Reduction)
        def reduce_to_mean(image):
            # scale=11132 is ~0.1 degree (matching GPM resolution)
            stats = image.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=aoi,
                scale=11132,
                bestEffort=True,
                maxPixels=1e9,
                tileScale=4
            )
            return ee.Feature(None, stats).set('system:time_start', image.get('system:time_start'))

        reduced_collection = daily_collection.map(reduce_to_mean)
        
        if self.client:
            return self.client.get_info(reduced_collection)
        else:
            # Fallback to direct call if for some reason client isn't passed (dev context)
            return reduced_collection.getInfo()

