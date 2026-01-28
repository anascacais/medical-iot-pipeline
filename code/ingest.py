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
        i = 0
        for line in f:
            if line.strip():  # what if this is not true? Could it be a malformed payload or not really? I will assume no since this is just simulating the data stream
                yield ast.literal_eval(line)
                time.sleep(delay_s)
                i += 1
            if i > 3:
                break


def clean(raw):
    # add logic to check which data is huploaded to data stream and to health check
    return {
        "hr": 77.0,
        "temp": 36.0,
        "SpO2": 102,
        "battery": 55,
        "ts_ing": datetime.datetime.now(),
        "ts_smp": datetime.datetime.now(),
        "flags": ["SPO2_INV"],
        "sensor_id": "icu-monitor-004"
    }


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
