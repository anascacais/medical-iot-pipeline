# Medical IoT Data Pipeline

The system must support two distinct use cases:

1. Real-Time Dashboarding: Low-latency lookups (e.g., "Show me the last 1 hour of vitals for Patient X").
2. Long-Term Analytics: Storing historical data for "Septic Shock" prediction models.

## Necessary Services

1. To use the Google Cloud Bigtable Emulator make sure you have Docker installed on your machine and run ([Test using the emulator](https://docs.cloud.google.com/bigtable/docs/emulator#install-docker)):

```
docker run -d -p 127.0.0.1:8086:8086 --name bigtable-emulator google/cloud-sdk gcloud beta emulators bigtable start --host-port=0.0.0.0:8086
```

2. Before running anything Pipeline-/BigQuery-related, create a **project** on GCP. Take note of the **project number** and **project ID**.

3. On `Cloud Storage > Buckets`, create a bucket which you'll use as your `PIPELINE_ROOT`

4. Register all necessary variables in a `.env` file in you root repository, this includes: `PROJECT_ID`, `PROJECT_NUMBER`, `REGION`, `DATASET_BQ`, `TABLE_BQ`, `TABLE_BQ_LABELS`, `INSTANCE_ID_BT`, `BIGTABLE_EMULATOR_HOST`, `MAX_TS`, `MIN_TS`, `PIPELINE_ROOT`, `MODEL_ID`

## How to Run

1. To create the Bigtable tables, run:

```
pipenv run python -m bigtable_load
```

2. To create the BigQuery dataset and table, run:

```
pipenv run python -m bigquery_create
```

3. To simulate live data ingestion into both databases, add a `data` folder into the root repository and add `vitals_raw.txt`; or manually update the file path on the script; and run:

```
pipenv run python -m ingest
```

4. To compile and submit the pipeline to GCP, run:

```
pipenv run python -m vertex_pipeline
```
