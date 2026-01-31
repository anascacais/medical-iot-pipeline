import json
import numpy as np
import pytest
from datetime import datetime, timedelta, timezone

from src_code import ingest
from src_code.ingest import MIN_TS, PHYSIOLOGICAL_RANGES


@pytest.fixture
def now():
    return datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def valid_packet():
    return json.dumps(
        {
            "sensor_id": "sensor-1",
            "event_timestamp": "2026-01-01T00:00:00Z",
            "heart_rate": 80,
            "body_temperature": 36.5,
            "spO2": 98,
            "battery_level": 50,
        }
    )


@pytest.fixture
def last_seen():
    return {}


@pytest.fixture
def mock_bigtable(monkeypatch):
    """
    Mock get_last_seen_timestamps so Bigtable is never touched.
    """

    def fake_get_last_seen(_table, _sensor_id):
        return None

    monkeypatch.setattr(
        "src_code.ingest.get_last_seen_timestamps",
        fake_get_last_seen,
    )

    return None

# Test is_impossible_timestamp


def test_timestamp_in_future(now):
    future = now + timedelta(seconds=10)
    assert ingest.is_impossible_timestamp(future, None, now)


def test_timestamp_earlier_than_last_seen(now):
    last_seen = now
    earlier = now - timedelta(seconds=5)
    assert ingest.is_impossible_timestamp(earlier, last_seen, now)


def test_valid_timestamp(now):
    event = now - timedelta(seconds=1)
    assert not ingest.is_impossible_timestamp(event, None, now)


# Test validate_measurement

@pytest.mark.parametrize(
    "value,valid_range,expected_status,expected_value",
    [
        (None, (0, 10), "NAN", np.nan),
        (np.nan, (0, 10), "NAN", np.nan),
        ("abc", (0, 10), "NAN", np.nan),
        (-1, (0, 10), "INV", -1),
        (11, (0, 10), "INV", 11),
        (5, (0, 10), "OK", 5.0),
        ("5.5", (0, 10), "OK", 5.5),
    ],
)
def test_validate_measurement(value, valid_range, expected_status, expected_value):
    status, result = ingest.validate_measurement(value, valid_range)
    assert status == expected_status
    if np.isnan(expected_value):
        assert np.isnan(result)
    else:
        assert result == expected_value


# Test process_packet

def test_valid_packet_processing(
    valid_packet, last_seen, mock_bigtable
):
    output = ingest.process_packet(
        valid_packet, last_seen, data_stream_table=None)

    assert output["sensor_id"] == "sensor-1"
    assert output["flags"] == []

    # Mapped output keys
    assert output["hr"] == 80.0
    assert output["temp"] == 36.5
    assert output["SpO2"] == 98.0
    assert output["battery"] == 50.0

    assert isinstance(output["ts_smp"], datetime)
    assert isinstance(output["ts_ing"], datetime)


def test_malformed_packet():
    output = ingest.process_packet("not-json", {}, None)

    assert "TS_INV" in output["flags"]
    for key in PHYSIOLOGICAL_RANGES:
        assert f"{key}_NAN" in output["flags"]


def test_missing_timestamp(valid_packet, last_seen, mock_bigtable):
    pkt = json.loads(valid_packet)
    pkt.pop("event_timestamp")

    output = ingest.process_packet(json.dumps(pkt), last_seen, None)

    assert "TS_INV" in output["flags"]
    assert output["ts_smp"] == ingest.ts2dt(MIN_TS)


def test_invalid_physiology(valid_packet, last_seen, mock_bigtable):
    pkt = json.loads(valid_packet)
    pkt["heart_rate"] = 1000  # impossible

    output = ingest.process_packet(json.dumps(pkt), last_seen, None)

    assert "hr_INV" in output["flags"]
    assert output["hr"] == pkt["heart_rate"]


def test_nan_measurement(valid_packet, last_seen, mock_bigtable):
    pkt = json.loads(valid_packet)
    pkt["spO2"] = None

    output = ingest.process_packet(json.dumps(pkt), last_seen, None)

    assert "SpO2_NAN" in output["flags"]
    assert np.isnan(output["SpO2"])
