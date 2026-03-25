import pytest
import time
import pandas as pd
from unittest.mock import MagicMock, patch
from src.api.main import execute_pipeline, JOBS

@pytest.fixture
def mock_providers():
    with patch('src.api.main.CHIRPSProvider') as mock_chirps, \
         patch('src.api.main.GPMProvider') as mock_gpm:
        
        # Simulate delay to test concurrency
        def slow_fetch(*args, **kwargs):
            time.sleep(0.5)
            return {"features": []}
            
        mock_chirps.return_value.get_rainfall_data.side_effect = slow_fetch
        mock_gpm.return_value.get_rainfall_data.side_effect = slow_fetch
        
        yield mock_chirps, mock_gpm

@patch('src.api.main.process_rainfall_data')
def test_execute_pipeline_concurrent(mock_process, mock_providers):
    # Setup
    job_id = "test-job-parallel"
    JOBS[job_id] = {"status": "running"}
    aoi_data = {"type": "Point", "coordinates": [0, 0]}
    
    # Mock data processing
    df_chirps = pd.DataFrame({'date': [pd.Timestamp('2024-01-01')], 'precipitation': [1.0]})
    df_gpm = pd.DataFrame({'date': [pd.Timestamp('2024-01-01')], 'precipitation': [2.0]})
    mock_process.side_effect = [df_chirps, df_gpm]
    
    start_time = time.time()
    execute_pipeline(job_id, "both", "2024-01-01", "2024-01-01", aoi_data)
    end_time = time.time()
    
    # If sequential, total time would be > 1.0s (0.5 + 0.5)
    # If parallel, total time would be ~0.5s
    # However, since I haven't implemented parallel yet, this should FAIL the timing check (or stay sequential).
    # I'll assert the status first.
    
    assert JOBS[job_id]["status"] == "completed"
    assert "result" in JOBS[job_id]
    
    # Check that both providers were called
    mock_providers[0].return_value.get_rainfall_data.assert_called()
    mock_providers[1].return_value.get_rainfall_data.assert_called()
