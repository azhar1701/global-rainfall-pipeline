import pytest
from unittest.mock import MagicMock, patch
from src.pipeline.auth import authenticate_gee
from src.pipeline.config import PipelineConfig

@patch("src.pipeline.auth.ee.Initialize")
@patch("src.pipeline.auth.service_account.Credentials.from_service_account_file")
@patch("src.pipeline.auth.load_config")
def test_authenticate_gee_success(mock_load_config, mock_from_file, mock_initialize):
    # Setup mock config
    mock_config = PipelineConfig(
        ee_service_account="test-account@test-project.iam.gserviceaccount.com",
        ee_private_key_path="path/to/key.json"
    )
    mock_load_config.return_value = mock_config
    
    # Setup mock credentials
    mock_creds = MagicMock()
    mock_from_file.return_value = mock_creds
    
    # Execute
    authenticate_gee()
    
    # Verify
    mock_from_file.assert_called_once_with(
        "path/to/key.json",
        scopes=['https://www.googleapis.com/auth/earthengine']
    )
    mock_initialize.assert_called_once_with(mock_creds)

@patch("src.pipeline.auth.load_config")
def test_authenticate_gee_missing_credentials(mock_load_config):
    # Setup mock config with missing credentials
    mock_config = PipelineConfig(
        ee_service_account=None,
        ee_private_key_path=None
    )
    mock_load_config.return_value = mock_config
    
    # Execute and Verify
    with pytest.raises(ValueError, match="GEE credentials not found"):
        authenticate_gee()

@patch("src.pipeline.auth.ee.Initialize")
@patch("src.pipeline.auth.service_account.Credentials.from_service_account_file")
@patch("src.pipeline.auth.load_config")
def test_authenticate_gee_initialization_error(mock_load_config, mock_from_file, mock_initialize):
    # Setup mock config
    mock_config = PipelineConfig(
        ee_service_account="test-account@test-project.iam.gserviceaccount.com",
        ee_private_key_path="path/to/key.json"
    )
    mock_load_config.return_value = mock_config
    
    # Mock initialization failure
    mock_initialize.side_effect = Exception("GEE Init Failed")
    
    # Execute and Verify
    with pytest.raises(RuntimeError, match="Failed to initialize Google Earth Engine"):
        authenticate_gee()

@patch("src.pipeline.auth.ee.Initialize")
@patch("src.pipeline.auth.service_account.Credentials.from_service_account_file")
@patch("src.pipeline.auth.load_config")
@patch("src.pipeline.auth.time.sleep") # Mock sleep to speed up tests
def test_authenticate_gee_retry_success(mock_sleep, mock_load_config, mock_from_file, mock_initialize):
    # Setup mock config
    mock_config = PipelineConfig(
        ee_service_account="test-account@test-project.iam.gserviceaccount.com",
        ee_private_key_path="path/to/key.json"
    )
    mock_load_config.return_value = mock_config
    
    # Mock initialization failure then success
    mock_initialize.side_effect = [Exception("Transient error"), None]
    
    # Execute
    authenticate_gee(max_retries=2)
    
    # Verify
    assert mock_initialize.call_count == 2
    mock_sleep.assert_called_once()
