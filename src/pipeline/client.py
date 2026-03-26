import ee
import time
import logging
import random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Any, List, Tuple, Dict, Optional
from datetime import datetime, timedelta
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

    @staticmethod
    def split_date_range(start_date: str, end_date: str, chunk_days: int = 30) -> List[Tuple[str, str]]:
        """
        Splits a date range into smaller chunks.
        """
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        chunks = []
        current = start
        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            chunks.append((current.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
            current = chunk_end + timedelta(days=1)
        return chunks

    def fetch_in_chunks(self, provider: Any, aoi: ee.Geometry, start_date: str, end_date: str, 
                        chunk_days: int = 30, max_workers: int = 8,
                        progress_callback: Optional[Callable[[int, int], None]] = None) -> Dict:
        """
        Fetches data in parallel chunks from a GEE provider.
        Chunk size of 10 days is optimized for memory-heavy GPM IMERG datasets.
        """
        chunks = self.split_date_range(start_date, end_date, chunk_days)
        total_chunks = len(chunks)
        logger.info(f"Splitting {start_date} to {end_date} into {total_chunks} chunks of {chunk_days} days each (max workers: {max_workers}).")
        
        all_results = []
        completed_count = 0
        
        def fetch_with_delay(s, e):
            # A very minimal jittered delay to prevent simultaneous identical HTTP connections
            delay = random.uniform(0.01, 0.1)
            time.sleep(delay)
            return provider.get_rainfall_data(aoi, s, e, _is_chunk=True)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_chunk = {
                executor.submit(fetch_with_delay, s, e): (s, e) 
                for s, e in chunks
            }
            
            for future in as_completed(future_to_chunk):
                completed_count += 1
                start, end = future_to_chunk[future]
                try:
                    data = future.result()
                    if data and 'features' in data:
                        all_results.extend(data['features'])
                        logger.info(f"Successfully fetched chunk {completed_count}/{total_chunks} ({start} to {end}).")
                    else:
                        logger.warning(f"Empty or invalid data for chunk {start} to {end}.")
                except Exception as e:
                    error_msg = str(e)
                    if "User memory limit exceeded" in error_msg:
                        logger.error(f"CRITICAL MEMORY ERROR for chunk {start} to {end}: {error_msg}")
                    else:
                        logger.error(f"Failed to fetch chunk {start} to {end}: {e}")
                    
                    # Store failure info
                    all_results.append({
                        "type": "Feature",
                        "properties": {
                            "system:time_start": int(pd.to_datetime(start).timestamp() * 1000),
                            "error": error_msg,
                            "is_failed": True
                        },
                        "geometry": None
                    })
                
                # Report progress callback
                if progress_callback:
                    try:
                        progress_callback(completed_count, total_chunks)
                    except Exception as cb_err:
                        logger.warning(f"Progress callback failed: {cb_err}")

        # Wrap the merged features back into a GEE-like structure
        return {"type": "FeatureCollection", "features": all_results}
