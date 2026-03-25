import pytest
import ee
from unittest.mock import MagicMock, patch
from src.pipeline.providers.chirps import CHIRPSProvider
from src.pipeline.providers.gpm import GPMProvider

@patch("src.pipeline.providers.chirps.ee")
def test_chirps_provider_get_rainfall_data(mock_ee):
    """Tests CHIRPS provider's extraction logic including spatial/temporal filtering and reduction."""
    # Setup mocks for GEE objects
    mock_collection = MagicMock()
    mock_ee.ImageCollection.return_value = mock_collection
    mock_collection.filterBounds.return_value = mock_collection
    mock_collection.filterDate.return_value = mock_collection
    mock_collection.select.return_value = mock_collection
    mock_collection.map.return_value = mock_collection
    
    expected_data = {"features": [{"properties": {"precipitation": 1.5, "system:time_start": 1704067200000}}]}
    mock_collection.getInfo.return_value = expected_data
    
    # Initialize provider
    provider = CHIRPSProvider()
    aoi = MagicMock(spec=ee.Geometry)
    start_date = "2024-01-01"
    end_date = "2024-01-02"
    
    # Execute
    data = provider.get_rainfall_data(aoi, start_date, end_date)
    
    # Verify GEE calls
    mock_ee.ImageCollection.assert_called_with("UCSB-CHG/CHIRPS/DAILY")
    mock_collection.filterBounds.assert_called_with(aoi)
    mock_collection.filterDate.assert_called_with(start_date, end_date)
    assert mock_collection.map.call_count == 2
    assert data == expected_data

@patch("src.pipeline.providers.gpm.ee")
def test_gpm_provider_get_rainfall_data(mock_ee):
    """Tests GPM provider's extraction logic including spatial/temporal filtering and reduction."""
    # Setup mocks for GEE objects
    mock_collection = MagicMock()
    mock_ee.ImageCollection.return_value = mock_collection
    mock_collection.filterBounds.return_value = mock_collection
    mock_collection.filterDate.return_value = mock_collection
    mock_collection.select.return_value = mock_collection
    mock_collection.map.return_value = mock_collection
    
    expected_data = {"features": [{"properties": {"precipitation": 2.5, "system:time_start": 1704067200000}}]}
    mock_collection.getInfo.return_value = expected_data
    
    # Initialize provider
    provider = GPMProvider()
    aoi = MagicMock(spec=ee.Geometry)
    start_date = "2024-01-01"
    end_date = "2024-01-02"
    
    # Execute
    data = provider.get_rainfall_data(aoi, start_date, end_date)
    
    # Verify GEE calls
    mock_ee.ImageCollection.assert_called_with("NASA/GPM_L3/IMERG_V07")
    mock_collection.filterBounds.assert_called_with(aoi)
    mock_collection.filterDate.assert_called_with(start_date, end_date)
    assert mock_collection.map.call_count == 2
    assert data == expected_data
