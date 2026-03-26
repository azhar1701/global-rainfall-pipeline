import os
import yaml
from typing import Optional, Dict
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel, Field

# Load .env file if it exists, overriding cached terminal session variables
load_dotenv(override=True)

class AOIConfig(BaseModel):
    geojson_path: Optional[str] = None
    scope: str = "global"

class DateRangeConfig(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    chunk_days: int = 10
    max_workers: int = 2  # Reduced default to prevent memory errors

class ProviderConfig(BaseModel):
    collection: str
    band: str

class PipelineConfig(BaseModel):
    ee_service_account: Optional[str] = None
    ee_private_key_path: Optional[str] = None
    aoi: AOIConfig = Field(default_factory=AOIConfig)
    date_range: DateRangeConfig = Field(default_factory=DateRangeConfig)
    timezone: str = "UTC"
    providers: Dict[str, ProviderConfig] = Field(default_factory=dict)

def load_config(config_path: str = "config.yaml") -> PipelineConfig:
    """
    Loads configuration from YAML and merges with environment variables.
    Environment variables take precedence for GEE credentials and settings.
    """
    path = Path(config_path)
    
    # If path is not absolute, search in current dir then project root
    if not path.is_absolute():
        if not path.exists():
            # Check project root relative to this file (src/pipeline/config.py -> ../../config.yaml)
            root_path = Path(__file__).resolve().parents[2]
            potential_path = root_path / config_path
            if potential_path.exists():
                path = potential_path

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path, "r") as f:
        try:
            config_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration: {e}")
    
    # Handle empty or malformed config.yaml
    if config_data is None:
        config_data = {}
    
    # Merge with environment variables
    # Credentials
    config_data["ee_service_account"] = os.getenv("EE_SERVICE_ACCOUNT") or config_data.get("ee_service_account")
    
    key_path = os.getenv("EE_PRIVATE_KEY_PATH") or config_data.get("ee_private_key_path")
    if key_path:
        # Sanitize python escape sequences if user cached a bad Windows path in their terminal
        key_path = key_path.replace('\x07', '\\a').replace('\b', '\\b').replace('\f', '\\f').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
    config_data["ee_private_key_path"] = key_path
    
    # AOI overrides
    if "aoi" not in config_data:
        config_data["aoi"] = {}
    
    env_aoi_path = os.getenv("AOI_PATH")
    if env_aoi_path:
        config_data["aoi"]["geojson_path"] = env_aoi_path
        
    env_aoi_scope = os.getenv("AOI_SCOPE")
    if env_aoi_scope:
        config_data["aoi"]["scope"] = env_aoi_scope

    # Date Range overrides
    if "date_range" not in config_data:
        config_data["date_range"] = {}
        
    env_start_date = os.getenv("START_DATE")
    if env_start_date:
        config_data["date_range"]["start_date"] = env_start_date
        
    env_end_date = os.getenv("END_DATE")
    if env_end_date:
        config_data["date_range"]["end_date"] = env_end_date
        
    env_chunk_days = os.getenv("CHUNK_DAYS")
    if env_chunk_days:
        config_data["date_range"]["chunk_days"] = int(env_chunk_days)
        
    env_max_workers = os.getenv("MAX_WORKERS")
    if env_max_workers:
        config_data["date_range"]["max_workers"] = int(env_max_workers)
    
    # Timezone override
    env_timezone = os.getenv("TIMEZONE")
    if env_timezone:
        config_data["timezone"] = env_timezone
        
    return PipelineConfig(**config_data)
