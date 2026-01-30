from google.cloud import bigquery
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
import os
import numpy as np

from src_code.time_aux import ts2dt, dt2ts, ensure_utc


# Load environment variables
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET_BQ")
TABLE = os.getenv("TABLE_BQ")
REGION = os.getenv("REGION")
META_KEYS = {"sensor_id", "ts_smp", "ts_ing", "flags"}
FLAG_CODE_MAP = {
    "INV_VALUE": 1,
    "NAN_VALUE": 2,
    "INV_TS": 3,
    "IMP_TS": 4,
}


def format_datetime(dt):
    return ensure_utc(dt).isoformat()


def format_measurement(measurement):
    if isinstance(measurement, float) and np.isnan(measurement):
        measurement = None
    return measurement


def resolve_flag_code(modality: str, flags: list[str]) -> int:
    """
    Resolve flag_type_code for a given modality.
    """

    # Timestamp-level flags override everything
    if "TS_INV" in flags:
        return FLAG_CODE_MAP["INV_TS"]
    if "TS_IMP" in flags:
        return FLAG_CODE_MAP["IMP_TS"]

    modality_lower = modality.lower()

    if f"{modality_lower}_INV" in flags:
        return FLAG_CODE_MAP["INV_VALUE"]
    if f"{modality_lower}_NAN" in flags:
        return FLAG_CODE_MAP["NAN_VALUE"]

    return 0


def write_to_bigquery(sample):
    """
    Load a wide sensor event into a BigQuery table using a row-based schema.

    The input event is expected to contain metadata fields and one or more
    modality fields. Each modality is expanded into a separate BigQuery row.
    Quality flags, if present, are resolved to a single flag code and applied
    uniformly to all generated rows.

    Parameters
    ----------
    sample : dict
        Sensor event payload containing metadata and modality values.
        Expected structure::

            {
                "sensor_id": str,
                "ts_smp": datetime,
                "ts_ing": datetime,
                "<modality_1>": float,
                "<modality_2>": float,
                ...
                "flags": list[str], optional
            }

        Timestamps must be expressed as epoch milliseconds.

    Returns
    -------
    int
        Number of rows successfully inserted into BigQuery. One row is created
        per modality found in the input event.

    Raises
    ------
    RuntimeError
        If BigQuery reports insertion errors.
    """
    client = bigquery.Client(project=PROJECT_ID)

    flags = sample.get("flags", [])

    rows = []
    for key, value in sample.items():
        if key in META_KEYS:
            continue

        flag_code = resolve_flag_code(key, flags)

        rows.append(
            {
                # truncate to ms
                "ts_smp": format_datetime(ts2dt(dt2ts(sample["ts_smp"]))),
                "ts_ing": format_datetime(sample["ts_ing"]),
                "sensor_id": sample["sensor_id"],
                "modality": key,
                "value": format_measurement(value),
                "flag_type_code": flag_code,
            }
        )

    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    errors = client.insert_rows_json(table_id, rows)

    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")


def create_table(client, table_id):
    """
    Create a BigQuery table if it does not already exist.

    Parameters
    ----------
    client : google.cloud.bigquery.Client
        BigQuery client used to interact with the service.
    table_id : str
        Fully-qualified table ID in the format "project.dataset.table".
    """
    try:
        table = client.get_table(table_id)
        print(f"Table {table_id} already exists.")
    except NotFound:
        # Read schema SQL and replace placeholders
        with open("sql/vitals_schema.sql") as f:
            sql = f.read()

        sql = (
            sql.replace("{{PROJECT_ID}}", PROJECT_ID)
            .replace("{{DATASET}}", DATASET)
            .replace("{{TABLE}}", TABLE)
        )

        client.query(sql).result()
        print(f"Successfully created BigQuery table {table_id}.")


def create_dataset(client, dataset_id):
    """
    Create a BigQuery dataset if it does not already exist.

    Parameters
    ----------
    client : google.cloud.bigquery.Client
        BigQuery client used to interact with the service.
    dataset_id : str
        Fully-qualified dataset ID in the format "project.dataset".
    """
    try:
        dataset = client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = REGION
        dataset = client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created in {dataset.location}")


def main():

    client = bigquery.Client(project=PROJECT_ID)

    # Create dataset
    dataset_id = f"{PROJECT_ID}.{DATASET}"
    create_dataset(client, dataset_id)

    # Create table
    table_id = f"{dataset_id}.{TABLE}"
    create_table(client, table_id)


if __name__ == "__main__":
    main()
