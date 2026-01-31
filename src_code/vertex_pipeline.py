import os
from kfp import dsl, compiler
from google.cloud import aiplatform
from kfp.dsl import (
    component,
    Dataset,
    Model,
    Metrics,
    Artifact,
    Output,
    Input
)

PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
DATASET = os.getenv("DATASET_BQ")
TABLE_LABELS = os.getenv("TABLE_BQ_LABELS")
TABLE_DATA = os.getenv("TABLE_BQ")
MODEL_ID = os.getenv("MODEL_ID")
PIPELINE_ROOT = os.getenv("PIPELINE_ROOT")
PIPELINE_PACKAGE_PATH = "septic_shock_pipeline.json"


@component(
    base_image="python:3.11",
    packages_to_install=[
        "pandas", "google-cloud-bigquery", "google-cloud-aiplatform"],
)
def ingest_from_bigquery(
    project_id: str,
    dataset_id: str,
    data_table_id: str,
    label_table_id: str,
    prev_model_resource_name: str,
    output_dataset: Output[Dataset],
):
    """Prepares a dataset for training and evaluating a new model while accounting for
    data used by the currently deployed model. Implements a train-test split strategy
    designed to balance new data and historical model evaluation, helping to monitor
    performance and mitigate catastrophic forgetting.

    The split logic is as follows:
        - **New data** (data that arrives after the currently deployed model was trained):
            - 70% of these samples are assigned to the training set.
            - 30% are assigned to the test set for evaluating the new model.
        - **Old training data** (used to train the currently deployed model):
            - A portion of the last x% of this data is appended to the new training set.
            This ensures that the new training set contains a balanced mix of old and new
            samples (~50% each), helping maintain performance on historical distributions.
        - **Old test data** (used to evaluate the currently deployed model):
            - Fully retained for testing to assess whether the new model maintains performance
            on previously unseen evaluation data (mitigating catastrophic forgetting).

    Parameters
    ----------
    project_id : str
        Google Cloud project ID containing the BigQuery datasets.
    dataset_id : str
        BigQuery dataset ID where the tables reside.
    data_table_id : str
        Name of the table containing the feature data.
    label_table_id : str
        Name of the table containing the labels corresponding to the feature data.
    prev_model_resource_name : str
        Resource name for currently deployed model whose training and evaluation data are used to guide
        the new split strategy.

    Returns
    -------
    output_dataset : Dataset
        A KFP Dataset artifact containing the full dataset with feature columns + label column + an additional column (flag)
        indicating sample roles:
            - `TRAIN`: Sample belongs to the new training dataset.
            - `TEST_0`: Sample belongs to the dataset used to evaluate the currently deployed model.
            - `TEST_1`: Sample belongs to the dataset used to evaluate the new model on new data.
    """
    from google.cloud import aiplatform
    from google.cloud import bigquery
    import pandas as pd

    model = aiplatform.Model(prev_model_resource_name)
    prev_train_start_ts = model.metadata["train_start_ts"]
    prev_train_end_ts = model.metadata["train_end_ts"]
    prev_test_start_ts = model.metadata["test_start_ts"]
    prev_test_end_ts = model.metadata["test_end_ts"]

    client = bigquery.Client(project=project_id)
    # load all data that falls within and after old model's ingestion period
    query = f"""
    WITH data AS (
        SELECT *
            FROM (
                SELECT *
                FROM `{project_id}.{dataset_id}.{data_table_id}`
                WHERE ts_ing >= {prev_train_start_ts}
                AND flag_type_code = 0
            )
            PIVOT (
                MAX(value) FOR modality IN ('hr', 'temp', 'SpO2')
            )
        )

        SELECT
            data.*,
            labels.sceptic_shock_label
            FROM `{project_id}.{dataset_id}.{label_table_id}` AS labels
            JOIN data
            ON data.ts_smp = labels.ts_smp
            AND data.sensor_id = labels.sensor_id;
    """

    df = client.query(query).to_dataframe()

    # remove incomplete samples are unecessary columns
    df = df.dropna()
    df = df.drop(["ts_smp", "sensor_id", "flag_type_code"], axis=1)

    # get new data
    new_data = df[df["ts_ing"] > prev_test_end_ts]
    test_df = new_data.iloc[int(len(new_data) * 0.7):]
    train_df = new_data.iloc[:int(len(new_data) * 0.7)]

    # get prev data
    legacy_test_df = df[(df["ts_ing"] >= prev_test_start_ts)
                        & (df["ts_ing"] <= prev_test_end_ts)]
    prev_train_df = df[(df["ts_ing"] >= prev_train_start_ts)
                       & (df["ts_ing"] <= prev_train_end_ts)]

    # get joint train data
    train_df = pd.concat([
        prev_train_df.iloc[len(train_df):],
        train_df
    ])

    # label samples and concat
    train_df["flag"] = "TRAIN"
    legacy_test_df["flag"] = "TEST_0"
    test_df["flag"] = "TEST_1"

    dataset_df = pd.concat([
        train_df,
        legacy_test_df,
        test_df
    ], ignore_index=True)

    dataset_df.to_csv(output_dataset.path, index=False)


@component(
    base_image="python:3.11",
    packages_to_install=["pandas", "scikit-learn"],
)
def train_model(
    dataset: Input[Dataset],
    model_metadata: Output[dict],
    new_model: Output[Model],
    metrics: Output[Artifact]
):
    """
    Train a dummy regression model on a dataset and log evaluation metrics.

    The function reads a dataset from a CSV, splits it into training and 
    test subsets according to a 'flag' column, trains a DummyRegressor, 
    evaluates it on both current and legacy test sets, and logs the 
    results as metrics.

    Parameters
    ----------
    dataset : Input[Dataset]
        Input dataset in CSV format containing features, a 'label' column, 
        a 'flag' column indicating TRAIN/TEST_1/TEST_0, and a 'ts_smp' timestamp column.
    model_metadata : Output[dict]
        Dictionary to store metadata about the dataset split, including 
        training and testing timestamps.
    new_model : Output[Model]
        Output model object after fitting.
    metrics : Output[Artifact]
        Output object used to log evaluation metrics (AUPRC for test and legacy test sets).

    Returns
    -------
    None
        The outputs (model, metadata, metrics) are stored in the provided Output objects.
    """
    import pandas as pd
    from sklearn.dummy import DummyRegressor
    from sklearn.metrics import average_precision_score
    import json

    def split_dataset(df, model_metadata):
        train_ids = df[df["flag"] == "TRAIN"].index
        test_ids = df[df["flag"] == "TEST_1"].index
        test_legacy_ids = df[df["flag"] == "TEST_0"].index

        label_df = df["label"]
        df = df.drop(["label", "flag"], axis=1)

        model_metadata["train_start_ts"] = df.loc[train_ids[0], "ts_smp"]
        model_metadata["train_end_ts"] = df.loc[train_ids[-1], "ts_smp"]
        model_metadata["test_start_ts"] = df.loc[test_ids[0], "ts_smp"]
        model_metadata["test_end_ts"] = df.loc[test_ids[-1], "ts_smp"]

        return df.loc[train_ids], label_df.loc[train_ids], df.loc[test_ids], label_df.loc[test_ids], df.loc[test_legacy_ids], label_df.loc[test_legacy_ids]

    dataset_df = pd.read_csv(dataset.path)

    # split dataset according to flag column - or maybe return ts so its easier to create metadata
    (X_train, y_train), (X_test, y_test), (X_legacy_test,
                                           y_legacy_test) = split_dataset(dataset_df, model_metadata)

    # define model and fit
    new_model = DummyRegressor()
    new_model.fit(X_train, y_train)

    # evaluate on new and legacy test data
    preds = new_model.predict(X_test)
    performance_test = average_precision_score(y_test, preds)
    metrics.log_metric("AUPRC", performance_test)

    preds = new_model.predict(X_legacy_test)
    performance_legacy = average_precision_score(y_legacy_test, preds)
    metrics.log_metric("AUPRC_legacy", performance_legacy)

    metrics_data = {
        "AUPRC": performance_test,
        "AUPRC_legacy": performance_legacy
    }

    with open(metrics.path, "w") as f:
        json.dump(metrics_data, f)


@component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-aiplatform"],
)
def compare_models(
    prev_model_resource_name: str,
    new_metrics: Input[Metrics],
    deploy: Output[bool],
    tolerance: float = 0.01
):
    import json
    from google.cloud import aiplatform

    # load metrics
    with open(new_metrics.path, "r") as f:
        metrics_data = json.load(f)

    # load previous model metadata
    prev_model = aiplatform.Model(prev_model_resource_name)
    with open(prev_model.metadata_path, "r") as f:
        prev_metadata = json.load(f)

    # decision logic
    if metrics_data["AUPRC_legacy"] < prev_metadata["AUPRC"] - tolerance:
        decision = False
    elif metrics_data["AUPRC"] > prev_metadata["AUPRC"] + tolerance:
        decision = True
    else:
        decision = False

    # write decision to deploy output
    with open(deploy.path, "w") as f:
        f.write(decision)


@component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-aiplatform"],
)
def register_model(
    project_id: str,
    region: str,
    model: Input[Model],
    prev_model_resource_name: str,
    training_metadata: Input[Artifact],
    registered_model: Output[Model],
):
    """
    Upload a trained model to Vertex AI, optionally using metadata from the previous model.

    Parameters
    ----------
    project_id : str
        GCP project ID.
    region : str
        GCP region.
    model : Input[Model]
        Model to register.
    prev_model : Input[Model]
        Previously deployed model (used to reuse display name or serving container URI).
    training_metadata : Input[Artifact]
        Metadata from training to attach to the model.
    registered_model : Output[Model]
        Output model artifact pointing to the Vertex AI model.
    """
    import json
    from google.cloud import aiplatform

    # initialize Vertex AI SDK
    aiplatform.init(project=project_id, location=region)

    with open(training_metadata.path, "r") as f:
        metadata_dict = json.load(f)

    prev_model = aiplatform.Model(prev_model_resource_name)
    with open(prev_model.metadata_path, "r") as f:
        prev_metadata = json.load(f)

    display_name = prev_metadata.get("display_name", "septic-shock-predictor")

    # use a serving container URI (reuse from previous model metadata if stored)
    serving_container_image_uri = prev_metadata.get(
        "serving_container_image_uri", "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest")

    # upload model to Vertex AI
    uploaded_model = aiplatform.Model.upload(
        display_name=display_name,
        artifact_uri=model.uri,
        serving_container_image_uri=serving_container_image_uri,
        metadata=metadata_dict,
    )

    # assign resource name to the output
    registered_model.uri = uploaded_model.resource_name


@component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-aiplatform"],
)
def deploy_model(
    project_id: str,
    region: str,
    registered_model: Model,
    endpoint_name: str = "septic-shock-endpoint",
    machine_type: str = "n1-standard-2",
):
    """
    Deploy a Vertex AI model to an endpoint. If the endpoint exists, it reuses it; otherwise, creates a new one.

    Parameters
    ----------
    registered_model : Model
        The model artifact to deploy (Vertex AI resource name).
    project_id : str
        GCP project ID.
    region : str
        GCP region.
    endpoint_name : str
        Name of the Vertex AI endpoint.
    machine_type : str
        Machine type for deployment.
    """
    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=region)
    model_obj = aiplatform.Model(registered_model.uri)

    # reuse existing endpoint if available
    endpoints = aiplatform.Endpoint.list(
        filter=f'display_name="{endpoint_name}"')
    if endpoints:
        endpoint = endpoints[0]
    else:
        endpoint = aiplatform.Endpoint.create(display_name=endpoint_name)

    # deploy the model
    model_obj.deploy(
        endpoint=endpoint,
        machine_type=machine_type,
    )

# Pipeline Definition


@dsl.pipeline(
    name="septic-shock-training-pipeline",
    description="BQ ingestion → training → model registration → deployment",
)
def septic_shock_pipeline(
    project_id: str,
    region: str,
    model_id: str,
    bq_dataset: str,
    bq_data_table: str,
    bq_label_table: str,
):

    prev_model_resource_name = f"projects/{project_id}/locations/{region}/models/{model_id}"

    ingest_task = ingest_from_bigquery(
        project_id=project_id,
        dataset_id=bq_dataset,
        data_table_id=bq_data_table,
        label_table_id=bq_label_table,
        prev_model_resource_name=prev_model_resource_name
    )

    train_task = train_model(
        dataset=ingest_task.outputs["output_dataset"]
    )

    comparison_task = compare_models(
        prev_model_resource_name=prev_model_resource_name,
        new_metrics=train_task.outputs["metrics"]
    )

    with dsl.Condition(comparison_task.outputs["deploy"]):
        register_task = register_model(
            project_id=project_id,
            region=region,
            model=train_task.outputs["new_model"],
            prev_model_resource_name=prev_model_resource_name,
            training_metadata=train_task.outputs["model_metadata"]
        )

        deploy_model(
            registered_model=register_task.outputs["registered_model"],
            project_id=project_id,
            region=region,
        )


def run_pipeline():
    # initialize Vertex AI
    aiplatform.init(
        project=PROJECT_ID,
        location=REGION,
    )

    # compile pipeline
    compiler.Compiler().compile(
        pipeline_func=septic_shock_pipeline,
        package_path=PIPELINE_PACKAGE_PATH,
    )

    # submit pipeline job
    job = aiplatform.PipelineJob(
        display_name="septic-shock-training-pipeline",
        template_path=PIPELINE_PACKAGE_PATH,
        pipeline_root=PIPELINE_ROOT,
        parameter_values={
            "project_id": PROJECT_ID,
            "region": REGION,
            "bq_dataset": os.getenv("DATASET_BQ"),
            "bq_data_table": os.getenv("TABLE_BQ"),
            "bq_label_table": os.getenv("TABLE_BQ_LABELS"),
        },
        enable_caching=True,
    )

    job.run(sync=False)


if __name__ == "__main__":
    run_pipeline()
