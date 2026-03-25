import ee
import time
import logging
from google.oauth2 import service_account
from .config import load_config

logger = logging.getLogger(__name__)

def authenticate_gee(max_retries: int = 3, backoff_factor: float = 2.0):
    """
    Authenticates with Google Earth Engine using service account credentials.
    
    Args:
        max_retries: Maximum number of retries for initialization.
        backoff_factor: Factor by which the delay increases between retries.
        
    Raises:
        ValueError: If credentials are not found in configuration.
        RuntimeError: If authentication or initialization fails after retries.
    """
    config = load_config()
    
    if not config.ee_service_account or not config.ee_private_key_path:
        raise ValueError("GEE credentials not found (EE_SERVICE_ACCOUNT or EE_PRIVATE_KEY_PATH)")

    try:
        credentials = service_account.Credentials.from_service_account_file(
            config.ee_private_key_path,
            scopes=['https://www.googleapis.com/auth/earthengine']
        )
    except Exception as e:
        raise ValueError(f"Failed to load service account credentials from {config.ee_private_key_path}: {e}")

    retry_count = 0
    while retry_count < max_retries:
        try:
            ee.Initialize(credentials)
            logger.info("Successfully initialized Google Earth Engine.")
            return
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"Failed to initialize GEE after {max_retries} attempts: {e}")
                raise RuntimeError(f"Failed to initialize Google Earth Engine: {e}")
            
            delay = backoff_factor ** retry_count
            logger.warning(f"GEE initialization attempt {retry_count} failed. Retrying in {delay}s... Error: {e}")
            time.sleep(delay)
