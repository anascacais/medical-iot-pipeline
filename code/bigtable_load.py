import os
from dotenv import load_dotenv
from google.cloud import bigtable
from google.cloud.bigtable import column_family
import struct
# local
from code.time_aux import ts2dt, dt2ts


# Load environment variables
load_dotenv()

EMULATOR_HOST = os.getenv("BIGTABLE_EMULATOR_HOST")
PROJECT_ID = os.getenv("PROJECT_ID")
INSTANCE_ID = os.getenv("INSTANCE_ID_BT")
MAX_TS = int(os.getenv("MAX_TS"))
MIN_TS = int(os.getenv("MIN_TS"))
EXPECTED_KEYS = ["hr", "temp", "SpO2", "battery"]


def decode_row(row):
    """
    Decode a Bigtable row into human-readable dict.

    Floats in 'vitals' are unpacked from '>d'.
    INT64 timestamps in 'meta' are unpacked from '>Q'.
    Flags remain as bytes or converted to int 1/0.
    """
    decoded = {}
    for family, columns in row.cells.items():
        decoded[family] = {}
        for qualifier_bytes, cells in columns.items():
            qualifier = qualifier_bytes.decode("utf-8")
            cell = cells[-1]  # latest version
            value_bytes = cell.value

            if family == "vitals":
                value = struct.unpack(">d", value_bytes)[0]
            elif family == "meta" and qualifier in ("ts_ing", "ts_smp"):
                ts_ms = struct.unpack(">Q", value_bytes)[0]
                value = ts2dt(ts_ms)
            elif family == "flag":
                value = int(value_bytes)  # b'1' -> 1
            else:
                value = value_bytes

            decoded[family][qualifier] = (value, cell.timestamp)
    return decoded


def get_last_seen_timestamps(table, sensor_id):
    """
    Retrieve the last seen event timestamp for each sensor from Bigtable.

    This function performs a prefix scan for the ``sensor_id`` and retrieves the most recent row based on the reversed-timestamp row key design (``sensor_id#(MAX_TS - event_timestamp_ms)``). If no data exists for the given sensor, it is omitted from the output. If the table contains no rows, an empty dictionary is returned.

    Parameters
    ----------
    table : google.cloud.bigtable.table.Table
        Bigtable table instance containing time-series sensor data.
    sensor_id : str
        Sensor identifier for which the last seen event timestamp should be retrieved.

    Returns
    -------
    int
        Last observed event timestamp in milliseconds since the Unix epoch. Returns 0 if no data exists for the given sensor.
    """
    prefix = f"{sensor_id}#".encode("utf-8")

    rows = table.read_rows(
        start_key=prefix,
        end_key=prefix + b"\xff",
        limit=1
    )

    for row in rows:
        row_key = row.row_key.decode()
        _, reversed_ts = row_key.split("#")
        return ts2dt(MAX_TS - int(reversed_ts))

    # No rows found
    return ts2dt(MIN_TS)


def get_table(project_id, instance_id, table_id):
    '''Get a Google Cloud Bigtable table instance.

    Parameters
    ----------
    project_id : str
        Google Cloud project ID.
    instance_id : str
        Bigtable instance ID.
    table_id : str
        Name of the Bigtable table.

    Returns
    -------
    table : google.cloud.bigtable.table.Table
        Bigtable table object that can be used for read and write operations.
    '''
    client = bigtable.Client(
        project=project_id,
        admin=True
    )

    instance = client.instance(instance_id)
    table = instance.table(table_id)

    return table


# call this for each new data stream after being cleaned (once for each table)
def write_to_bigtable(stream_data_table, health_check_table, sample):
    """
    Write a cleaned sample to Bigtable.

    Parameters
    ----------
    table : google.cloud.bigtable.table.Table
        Bigtable table instance.
    sample : dict
        Cleaned sample with optional fields.
    """
    if not ("TS_INV" in sample["flags"] or "TS_IMP" in sample["flags"]):
        # stream_data
        row_key = f"{sample['sensor_id']}#{MAX_TS - dt2ts(sample['ts_smp'])}".encode(
        )
        row = stream_data_table.direct_row(row_key)

        row.set_cell("meta", "ts_ing", struct.pack(
            ">Q", dt2ts(sample["ts_ing"])))

        for key in EXPECTED_KEYS:
            row.set_cell("vitals", key, struct.pack(">d", sample[key]))

        row.commit()

    # health_check
    if sample["flags"] != []:
        row_key = f"{sample['sensor_id']}#{MAX_TS - dt2ts(sample['ts_ing'])}".encode(
        )
        row = health_check_table.direct_row(row_key)

        row.set_cell("meta", "ts_smp", struct.pack(
            ">Q", dt2ts(sample["ts_smp"])))

        for flag in sample["flags"]:
            row.set_cell("flag", flag, b"1")
            key = flag.split('_')[0]
            if key in EXPECTED_KEYS:
                row.set_cell("vitals", key, struct.pack(">d", sample[key]))

        row.commit()


def create_table(instance, table_id, column_families):
    '''Create a Bigtable table with specified column families with Garbage Collection Rule Max Versions equal to 1.

    Parameters
    ----------
    instance : google.cloud.bigtable.instance.Instance
        The Bigtable instance object where the table will be created.
    table_id : str
        Name of the table to create.
    column_families : list of str
        List of column family names to create in the table.
    '''
    column_families_dict = {
        key: column_family.MaxVersionsGCRule(1) for key in column_families}

    table = instance.table(table_id)
    if not table.exists():
        print(f"Creating table {table_id}...")
        table.create(column_families=column_families_dict)
        print(f"Table {table_id} created.")
    else:
        print(f"Table {table_id} already exists.")


def main():

    # Make sure the emulator is running
    if not EMULATOR_HOST:
        raise RuntimeError(
            "BIGTABLE_EMULATOR_HOST is not set. Start the emulator first.")

    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(
        INSTANCE_ID,
        instance_type=bigtable.enums.Instance.Type.DEVELOPMENT
    )

    # Create tables
    create_table(instance, table_id="stream_data",
                 column_families=["vitals", "meta"])
    create_table(instance, table_id="health_check",
                 column_families=["vitals", "meta", "flag"])

    # Health check
    tables = instance.list_tables()
    print("Tables in instance:", [table.table_id for table in tables])


if __name__ == "__main__":
    main()
