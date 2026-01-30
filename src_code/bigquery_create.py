from google.cloud import bigquery
from dotenv import load_dotenv
import os


# Load environment variables
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET_BQ")
REGION = os.getenv("REGION")


def main():

    client = bigquery.Client(project=PROJECT_ID)

    # Create dataset
    dataset_id = f"{PROJECT_ID}.{DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION

    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset_id} created in {dataset.location}")

    # Create table
    with open("sql/vitals_schema.sql") as f:
        sql = f.read()

    sql = sql.replace("{{PROJECT_ID}}", PROJECT_ID).replace(
        "{{DATASET}}", DATASET)
    client.query(sql).result()

    print("Successfully created BigQuery table.")


if __name__ == "__main__":
    main()
