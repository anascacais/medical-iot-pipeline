import os
from dotenv import load_dotenv
from google.cloud import bigtable
from google.cloud.bigtable import column_family


# Load environment variables
load_dotenv()

EMULATOR_HOST = os.getenv("BIGTABLE_EMULATOR_HOST")
PROJECT_ID = os.getenv("PROJECT_ID")
INSTANCE_ID = os.getenv("INSTANCE_ID_BT")


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
