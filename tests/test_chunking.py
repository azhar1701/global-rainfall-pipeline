import pytest
from src.pipeline.client import GEEClient

def test_split_date_range_exact():
    chunks = GEEClient.split_date_range("2024-01-01", "2024-01-30", chunk_days=10)
    assert len(chunks) == 3
    assert chunks[0] == ("2024-01-01", "2024-01-10")
    assert chunks[1] == ("2024-01-11", "2024-01-20")
    assert chunks[2] == ("2024-01-21", "2024-01-30")

def test_split_date_range_uneven():
    chunks = GEEClient.split_date_range("2024-01-01", "2024-01-15", chunk_days=10)
    assert len(chunks) == 2
    assert chunks[0] == ("2024-01-01", "2024-01-10")
    assert chunks[1] == ("2024-01-11", "2024-01-15")

def test_split_date_range_single_day():
    chunks = GEEClient.split_date_range("2024-01-01", "2024-01-01", chunk_days=10)
    assert len(chunks) == 1
    assert chunks[0] == ("2024-01-01", "2024-01-01")

def test_split_date_range_leap_year():
    chunks = GEEClient.split_date_range("2024-02-25", "2024-03-05", chunk_days=10)
    # 2024 is leap year: Feb 25, 26, 27, 28, 29 (5 days) + Mar 1, 2, 3, 4, 5 (5 days) = 10 days
    assert len(chunks) == 1
    assert chunks[0] == ("2024-02-25", "2024-03-05")
