import ast
import time
import os
import datetime
from dotenv import load_dotenv
from bigtable_load import get_table, write_to_bigtable


# Load environment variables
load_dotenv()

EMULATOR_HOST = os.getenv("BIGTABLE_EMULATOR_HOST")
PROJECT_ID = os.getenv("PROJECT_ID")
INSTANCE_ID = os.getenv("INSTANCE_ID_BT")


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
        return None

    try:
        return datetime.fromisoformat(ts).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


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
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "NAN"

    try:
        value = float(value)
    except (TypeError, ValueError):
        return "INV"

    if not (valid_range[0] <= value <= valid_range[1]):
        return "INV"

    return "OK"

def main(file_path):

    data_stream_table = get_table(PROJECT_ID, INSTANCE_ID, "stream_data")
    health_check_table = get_table(PROJECT_ID, INSTANCE_ID, "health_check")

    for raw in simulate(file_path):
        clean_sample = clean(raw)
        # write_to_bigtable(data_stream_table, clean_sample)
        # write_to_bigtable(health_check_table, clean_sample)
        # write_to_bigquery(clean_sample)


if __name__ == "__main__":
    main(file_path="data/vitals_raw.txt")
