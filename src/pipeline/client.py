import ee
import time
import logging
import random
from typing import Callable, Any
from src.pipeline.auth import authenticate_gee

logger = logging.getLogger(__name__)

class GEEClient:
    """
    A robust client wrapper for Google Earth Engine interactions.
    Handles authentication, retries, and error normalization.
    """
    def __init__(self, max_retries: int = 5, initial_backoff: float = 1.0):
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self._ensure_authorized()

    def _ensure_authorized(self):
        try:
            # Check if already initialized by running a cheap command
            ee.Number(1).getInfo()
        except Exception:
            logger.info("Initializing GEE Client...")
            authenticate_gee()
            # Double check
            ee.Number(1).getInfo()

    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        Executes a GEE operation with exponential backoff retries.
        """
        retries = 0
        backoff = self.initial_backoff
        
        while retries < self.max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                retries += 1
                if retries == self.max_retries:
                    logger.error(f"Execution failed after {self.max_retries} attempts: {e}")
                    raise e
                
                # Jittered exponential backoff
                sleep_time = backoff * (2 ** (retries - 1)) + (random.uniform(0, 0.1) * backoff)
                logger.warning(f"GEE Error: {e}. Retrying in {sleep_time:.2f}s... (Attempt {retries}/{self.max_retries})")
                time.sleep(sleep_time)

    def get_collection(self, collection_name: str) -> ee.ImageCollection:
        """Helper to get an ImageCollection."""
        return ee.ImageCollection(collection_name)

    def get_info(self, ee_object: Any) -> Any:
        """Executes getInfo() with retries."""
        return self.execute_with_retry(ee_object.getInfo)
