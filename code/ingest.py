import time
import json
import os
import datetime
from dotenv import load_dotenv
from bigtable_load import get_last_seen_timestamps, get_table, write_to_bigtable, decode_row
from datetime import datetime, timezone
import numpy as np
from time_aux import ts2dt

# Load environment variables
load_dotenv()

EMULATOR_HOST = os.getenv("BIGTABLE_EMULATOR_HOST")
PROJECT_ID = os.getenv("PROJECT_ID")
INSTANCE_ID = os.getenv("INSTANCE_ID_BT")
MIN_TS = int(os.getenv("MIN_TS"))

PHYSIOLOGICAL_RANGES = {
    "heart_rate": (0., 350.),          # bpm
    "body_temperature": (25., 45.),     # °C
    "spO2": (0.0, 100.0),                # %
    "battery_level": (0.0, 100.0),        # %
}

MAP_IO_NAMES = {
    "heart_rate": "hr",
    "body_temperature": "temp",
    "spO2": "SpO2",
    "battery_level": "battery",
}


def simulate(file_path, delay_s=0.1):
    '''Simulate real-time data ingestion by yielding one sample at a time from a text file.

    Parameters
    ---------- 
    file_path : str
        Path to a text file where each line is a dictionary representing a raw sample.
    delay_s : float, optional
        Time in seconds to wait between yielding samples, simulating real-time ingestion.

    Returns
    -------
    raw_sample : dict
        A single raw data sample from the file, yielded one at a time.
    '''
    with open(file_path, "r") as f:
        # loads entire file onto RAM, but okay for use case
        for line in reversed(f.readlines()):
            yield line
            time.sleep(delay_s)


def parse_event_timestamp(ts):
    """
    Parse an ISO-8601 timestamp string into a timezone-aware datetime.

    Parameters
    ----------
    ts : Any
        Raw timestamp value from the packet.

    Returns
    -------
    datetime or None
        Parsed datetime object if valid, otherwise None.
    """
    if not isinstance(ts, str):
        return ts2dt(MIN_TS)

    try:
        return datetime.fromisoformat(ts).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return ts2dt(MIN_TS)


def is_impossible_timestamp(event_time, last_seen_time, now):
    """
    Check whether a timestamp is impossible.

    A timestamp is considered impossible if it is:
    - In the future relative to system time
    - Earlier than the last observed timestamp for the same sensor

    Parameters
    ----------
    event_time : datetime
        Parsed event timestamp.
    last_seen_time : datetime or None
        Last recorded timestamp for the same sensor.

    Returns
    -------
    bool
        True if timestamp is impossible, False otherwise.
    """

    if event_time > now:
        return True

    if last_seen_time and event_time < last_seen_time:
        return True

    return False


def validate_measurement(value, valid_range):
    """
    Validate a physiological or battery measurement.

    Parameters
    ----------
    value : Any
        Raw measurement value.
    valid_range : tuple
        (min, max) acceptable range.

    Returns
    -------
    str
        One of:
        - "OK"    : value is valid
        - "*_NAN" : value is missing or null
        - "*_INV" : value is outside plausible range
    float or np.nan (compatible with float)

    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "NAN", np.nan

    try:
        value = float(value)
    except (TypeError, ValueError):
        return "INV", np.nan

    if not (valid_range[0] <= value <= valid_range[1]):
        return "INV", np.nan

    return "OK", value


def process_packet(packet, last_seen_timestamps, data_stream_table):
    """
    Validate and flag an incoming sensor packet.

    Raw values are preserved. Invalid or missing data is flagged
    according to modality-specific rules.

    Parameters
    ----------
    packet : Any
        Incoming data packet (expected to be a dictionary).
    last_seen_timestamps : dict
        Mapping from sensor_id to last observed event timestamp.

    Returns
    -------
    dict
        Processed packet including validation flags.
    """

    malformed = False
    output = {

        "flags": [],
    }
    output["ts_ing"] = datetime.now(timezone.utc)

    # Malformed packet
    try:
        packet = json.loads(packet)
    except json.JSONDecodeError:
        malformed = True
    if not isinstance(packet, dict):
        malformed = True

    if malformed:
        output["flags"] += ["TS_INV"]
        for key in PHYSIOLOGICAL_RANGES:
            output["flags"] += [f"{key}_NAN"]
        return output

    sensor_id = packet.get("sensor_id")
    output["sensor_id"] = sensor_id
    event_ts_raw = packet.get("event_timestamp")

    # Timestamp validation
    event_time = parse_event_timestamp(event_ts_raw)
    output["ts_smp"] = event_time

    if event_time == MIN_TS:
        output["flags"] += ["TS_INV"]
    else:
        if not sensor_id in last_seen_timestamps.keys():
            last_seen_timestamps[sensor_id] = get_last_seen_timestamps(
                data_stream_table, sensor_id)
        last_seen = last_seen_timestamps.get(sensor_id)
        if is_impossible_timestamp(event_time, last_seen, output["ts_ing"]):
            output["flags"] += ["TS_IMP"]
        else:
            last_seen_timestamps[sensor_id] = event_time

    # Physiological & battery validation
    for field, valid_range in PHYSIOLOGICAL_RANGES.items():
        status, measure = validate_measurement(packet.get(field), valid_range)

        if status == "NAN":
            output["flags"] += [f"{MAP_IO_NAMES[field]}_NAN"]
        elif status == "INV":
            output["flags"] += [f"{MAP_IO_NAMES[field]}_INV"]

        output[MAP_IO_NAMES[field]] = measure

    print(output)
    return output


def main(file_path):

    # initiate like this and populate as we see new sensor_ids
    last_seen_timestamps = {}

    data_stream_table = get_table(PROJECT_ID, INSTANCE_ID, "stream_data")
    health_check_table = get_table(PROJECT_ID, INSTANCE_ID, "health_check")

    i = 0
    for raw in simulate(file_path):
        if i == 10:
            break
        clean_sample = process_packet(
            raw, last_seen_timestamps, data_stream_table)
        write_to_bigtable(data_stream_table, health_check_table, clean_sample)
        # write_to_bigquery(clean_sample)
        i += 1


if __name__ == "__main__":
    main(file_path="data/vitals_raw.txt")
