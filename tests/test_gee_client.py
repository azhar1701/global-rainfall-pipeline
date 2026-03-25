import pytest
from unittest.mock import MagicMock, patch
from src.pipeline.client import GEEClient

@pytest.fixture
def mock_ee():
    with patch('src.pipeline.client.ee') as mock:
        yield mock

def test_gee_client_initialization(mock_ee):
    client = GEEClient()
    assert client is not None
    # EE should be authenticated on init if not already
    mock_ee.Number.assert_called()

def test_get_collection_success(mock_ee):
    client = GEEClient()
    mock_col = MagicMock()
    mock_ee.ImageCollection.return_value = mock_col
    
    result = client.get_collection("NASA/GPM_L3/IMERG_V07")
    
    mock_ee.ImageCollection.assert_called_with("NASA/GPM_L3/IMERG_V07")
    assert result == mock_col

def test_retry_on_network_error(mock_ee):
    client = GEEClient(max_retries=3, initial_backoff=0.1)
    
    # Mock a function that fails twice then succeeds
    mock_func = MagicMock()
    mock_func.side_effect = [Exception("Network Error"), Exception("Quota Exceeded"), "success"]
    
    result = client.execute_with_retry(mock_func)
    
    assert result == "success"
    assert mock_func.call_count == 3

def test_fails_after_max_retries(mock_ee):
    client = GEEClient(max_retries=2, initial_backoff=0.1)
    
    mock_func = MagicMock()
    mock_func.side_effect = Exception("Permanent Failure")
    
    with pytest.raises(Exception, match="Permanent Failure"):
        client.execute_with_retry(mock_func)
    
    assert mock_func.call_count == 2
