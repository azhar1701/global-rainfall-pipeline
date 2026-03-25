import os
import pytest
from pathlib import Path
from src.pipeline.config import load_config

def test_load_config_defaults(monkeypatch):
    # Mock environment variables
    monkeypatch.setenv("EE_SERVICE_ACCOUNT", "test-sa@test.com")
    monkeypatch.setenv("EE_PRIVATE_KEY_PATH", "/path/to/key.json")
    
    config = load_config()
    
    assert config.ee_service_account == "test-sa@test.com"
    assert config.ee_private_key_path == "/path/to/key.json"
    assert config.timezone == "UTC"
    assert config.aoi.scope == "global"

def test_load_config_env_override(monkeypatch):
    monkeypatch.setenv("EE_SERVICE_ACCOUNT", "override@test.com")
    monkeypatch.setenv("TIMEZONE", "EST")
    
    config = load_config()
    
    assert config.ee_service_account == "override@test.com"
    assert config.timezone == "EST"

def test_load_config_pathlib_path(tmp_path):
    # Create a temporary config file
    config_file = tmp_path / "custom_config.yaml"
    config_file.write_text("aoi:\n  scope: \"local\"\nproviders: {}")
    
    config = load_config(str(config_file))
    assert config.aoi.scope == "local"

def test_load_config_empty_file(tmp_path):
    # Create an empty config file
    config_file = tmp_path / "empty_config.yaml"
    config_file.write_text("")
    
    config = load_config(str(config_file))
    # Should use defaults
    assert config.aoi.scope == "global"
    assert config.timezone == "UTC"

def test_load_config_new_env_overrides(monkeypatch, tmp_path):
    # Create a minimal config file
    config_file = tmp_path / "min_config.yaml"
    config_file.write_text("providers: {}")
    
    monkeypatch.setenv("AOI_PATH", "env/path.geojson")
    monkeypatch.setenv("AOI_SCOPE", "env_scope")
    monkeypatch.setenv("START_DATE", "2024-02-01")
    monkeypatch.setenv("END_DATE", "2024-02-28")
    
    config = load_config(str(config_file))
    
    assert config.aoi.geojson_path == "env/path.geojson"
    assert config.aoi.scope == "env_scope"
    assert config.date_range.start_date == "2024-02-01"
    assert config.date_range.end_date == "2024-02-28"

def test_load_config_ee_credentials_defaults(tmp_path):
    # Minimal config
    config_file = tmp_path / "cred_config.yaml"
    config_file.write_text("providers: {}")
    
    # Ensure env vars are NOT set
    if "EE_SERVICE_ACCOUNT" in os.environ:
        del os.environ["EE_SERVICE_ACCOUNT"]
    if "EE_PRIVATE_KEY_PATH" in os.environ:
        del os.environ["EE_PRIVATE_KEY_PATH"]

    config = load_config(str(config_file))
    
    assert config.ee_service_account is None
    assert config.ee_private_key_path is None
