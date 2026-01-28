import os
from dotenv import load_dotenv
from google.cloud import bigtable
from google.cloud.bigtable import column_family


# Load environment variables
load_dotenv()

EMULATOR_HOST = os.getenv("BIGTABLE_EMULATOR_HOST")
PROJECT_ID = os.getenv("PROJECT_ID")
INSTANCE_ID = os.getenv("INSTANCE_ID_BT")


# call this at the beginning of ingestion.py for both tables
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
def write_to_bigtable(table, sample):
    """
    Write a cleaned sample to Bigtable.

    Parameters
    ----------
    table : google.cloud.bigtable.table.Table
        Bigtable table instance.
    sample : dict
        Cleaned sample with optional fields.
    """
    if table.table_id == "stream_data":
        row_key = f"{sample['sensor_id']}#{sample['ts_smp']}".encode()
    elif table.table_id == "health_check":
        row_key = f"{sample['sensor_id']}#{sample['ts_ing']}".encode()
    else:
        raise ValueError(
            f"Table ID {table.table_id} does not match any existing table.")

    row = table.direct_row(row_key)

    # Validate column family according to currently allowed values
    for key, value in sample:
        if key in ["hr", "temp", "SpO2", "battery"]:
            row.set_cell("vitals", key, value)
        elif key in ["ts_ing", "ts_smp"]:
            row.set_cell("meta", key, value)
        elif key in ["flags"]:
            for flag in value:
                row.set_cell("flag", flag, 1)
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
