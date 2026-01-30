import struct
import pytest
from unittest.mock import MagicMock


from src_code import bigtable_load
from src_code.bigtable_load import MAX_TS, MIN_TS
from src_code.time_aux import ts2dt


class FakeCell:
    def __init__(self, value, timestamp=123456):
        self.value = value
        self.timestamp = timestamp


class FakeRow:
    def __init__(self, row_key, cells):
        self.row_key = row_key
        self.cells = cells


class FakeDirectRow:
    def __init__(self, row_key):
        self.row_key = row_key
        self.set_calls = []

    def set_cell(self, family, qualifier, value):
        self.set_calls.append((family, qualifier, value))

    def commit(self):
        pass


class FakeTable:
    def __init__(self):
        self.rows = []
        self.direct_rows = []

    def read_rows(self, start_key=None, end_key=None, limit=None):
        return iter(self.rows[:limit])

    def direct_row(self, row_key):
        row = FakeDirectRow(row_key)
        self.direct_rows.append(row)
        return row


# Test decode_row

def test_decode_row_decodes_all_families():
    ts_ms = 1700000000000

    row = FakeRow(
        row_key=b"sensor#123",
        cells={
            "vitals": {
                b"hr": [FakeCell(struct.pack(">d", 400.5))],
            },
            "meta": {
                b"ts_smp": [FakeCell(struct.pack(">Q", ts_ms))],
            },
            "flag": {
                b"hr_INV": [FakeCell(b"1")],
            },
        },
    )

    decoded = bigtable_load.decode_row(row)

    assert decoded["vitals"]["hr"][0] == pytest.approx(400.5)
    assert decoded["meta"]["ts_smp"][0] == ts2dt(ts_ms)
    assert decoded["flag"]["hr_INV"][0] == 1


# Test get_last_seen_timestamps

def test_get_last_seen_timestamps_returns_latest():
    sensor_id = "sensorA"
    event_ts = 1700000000000
    reversed_ts = MAX_TS - event_ts

    table = FakeTable()
    table.rows.append(
        FakeRow(
            row_key=f"{sensor_id}#{reversed_ts}".encode(),
            cells={}
        )
    )

    result = bigtable_load.get_last_seen_timestamps(table, sensor_id)

    assert result == ts2dt(event_ts)


def test_get_last_seen_timestamps_no_rows():
    table = FakeTable()

    result = bigtable_load.get_last_seen_timestamps(table, "sensorX")

    assert result == ts2dt(MIN_TS)


# Test write_to_bigtable

def sample_payload(flags):
    return {
        "sensor_id": "sensor1",
        "ts_ing": ts2dt(1700000001000),
        "ts_smp": ts2dt(1700000000000),
        "flags": flags,
        "hr": 70.0,
        "temp": 36.6,
        "SpO2": 98.0,
        "battery": 90,
    }


def test_write_to_bigtable_stream_data_only():
    stream_table = FakeTable()
    health_table = FakeTable()

    bigtable_load.write_to_bigtable(
        stream_table, health_table, sample_payload(flags=[]))

    # one stream row written
    assert len(stream_table.direct_rows) == 1
    assert len(health_table.direct_rows) == 0

    row = stream_table.direct_rows[0]
    families = {c[0] for c in row.set_calls}

    assert "meta" in families
    assert "vitals" in families


def test_write_to_bigtable_health_check_only():
    stream_table = FakeTable()
    health_table = FakeTable()

    bigtable_load.write_to_bigtable(
        stream_table, health_table, sample_payload(flags=["TS_IMP"]))

    # stream suppressed, health written
    assert len(stream_table.direct_rows) == 0
    assert len(health_table.direct_rows) == 1

    row = health_table.direct_rows[0]
    qualifiers = {c[1] for c in row.set_calls}

    assert "TS_IMP" in qualifiers
    assert "hr" in qualifiers


def test_write_to_bigtable_both_paths():
    stream_table = FakeTable()
    health_table = FakeTable()

    bigtable_load.write_to_bigtable(
        stream_table, health_table, sample_payload(flags=["BAT_INV"]))

    assert len(stream_table.direct_rows) == 1
    assert len(health_table.direct_rows) == 1
