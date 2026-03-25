import pytest
from fastapi.testclient import TestClient
from src.api.main import app, JOBS
import pandas as pd
import io

client = TestClient(app)

def test_export_data_endpoint():
    # Setup a dummy job with a cached DF
    job_id = "test-export-job"
    df = pd.DataFrame({'date': ['2024-01-01'], 'precipitation': [10.5]})
    JOBS[job_id] = {"status": "completed", "_df": df}
    
    response = client.get(f"/api/jobs/{job_id}/export")
    
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8"
    assert "attachment; filename=rainfall_export_test-export-job.csv" in response.headers["content-disposition"]
    
    # Check CSV content
    content = response.text
    assert "date,precipitation" in content
    assert "2024-01-01,10.5" in content

def test_export_not_found():
    response = client.get("/api/jobs/non-existent/export")
    assert response.status_code == 404
