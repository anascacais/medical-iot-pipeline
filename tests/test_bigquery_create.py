import pytest
import numpy as np
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Import functions under test
from src_code import bigquery_create


# Test format_datetime

def test_format_datetime_returns_iso_utc():
    dt = datetime(2024, 1, 1, 12, 0, 0)
    result = bigquery_create.format_datetime(dt)

    assert result.endswith("+00:00")
    assert "2024-01-01T12:00:00" in result


# Test format_measurement

def test_format_measurement_nan_to_none():
    assert bigquery_create.format_measurement(np.nan) is None


def test_format_measurement_valid_float():
    assert bigquery_create.format_measurement(42.0) == 42.0


# Test resolve_flag_code

@pytest.mark.parametrize(
    "modality,flags,expected",
    [
        ("hr", ["TS_INV"], 3),
        ("hr", ["TS_IMP"], 4),
        ("hr", ["hr_NAN"], 2),
        ("hr", ["hr_INV"], 1),
        ("hr", [], 0),
    ],
)
def test_resolve_flag_code(modality, flags, expected):
    assert bigquery_create.resolve_flag_code(modality, flags) == expected


# Test write_to_bigquery

@patch("src_code.bigquery_create.bigquery.Client")
def test_write_to_bigquery_success(mock_client_cls):
    mock_client = MagicMock()
    mock_client.insert_rows_json.return_value = []
    mock_client_cls.return_value = mock_client

    sample = {
        "sensor_id": "sensor_1",
        "ts_smp": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "ts_ing": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "hr": 60.0,
        "flags": [],
    }
    bigquery_create.write_to_bigquery(mock_client, sample)

    mock_client.insert_rows_json.assert_called_once()
    table_id, rows = mock_client.insert_rows_json.call_args[0]

    assert len(rows) == 1
    assert rows[0]["modality"] == "hr"
    assert rows[0]["value"] == 60.0
    assert rows[0]["flag_type_code"] == 0
