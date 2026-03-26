import ee
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Callable

class BaseSatelliteProvider(ABC):
    """
    Abstract base class for satellite rainfall data providers (e.g., CHIRPS, GPM).
    """

    @abstractmethod
    def get_rainfall_data(self, aoi: ee.Geometry, start_date: str, end_date: str, progress_callback: Optional[Callable[[int, int], None]] = None) -> List[Dict]:
        """
        Retrieves rainfall data for a given Area of Interest (AOI) and date range.
        
        Args:
            aoi: Area of Interest as an Earth Engine Geometry object.
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
            
        Returns:
            Dict: A dictionary containing the retrieved rainfall data.
        """
        pass
