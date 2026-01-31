from google.cloud import aiplatform
from datetime import datetime, timezone
import os
from kfp import dsl
from kfp.dsl import (
    component,
    Dataset,
    Model,
    Metrics,
    Artifact,
    Output,
    Input
)
from typing import NamedTuple
from google.cloud import aiplatform


PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
DATASET = os.getenv("DATASET_BQ")
TABLE_LABELS = os.getenv("TABLE_BQ_LABELS")
TABLE_DATA = os.getenv("TABLE_BQ")


# output type definitions
class OutputDataset(NamedTuple):
    output_dataset: Dataset


@component(
    base_image="python:3.11",
    packages_to_install=["pandas", "google-cloud-bigquery"],
)
def ingest_from_bigquery(
    project_id: str,
    dataset_id: str,
    data_table_id: str,
    label_table_id: str,
    model: Model,
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
    model : kfp.dsl.Model
        Currently deployed model whose training and evaluation data are used to guide
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
    model.metadata["training_data"]["train_end_ts"]

    # dataset must be wide and not long + drop NaN + feature extraction
    pass


@component(
    base_image="python:3.11",
    packages_to_install=["pandas", "scikit-learn", "joblib"],
)
def train_model(
    dataset: Input[Dataset],
    model_metadata: Output[dict],
    new_model: Output[Model],
    metrics: Output[Metrics]
):
    import pandas as pd
    from sklearn.linear_model import LogisticRegression

    dataset_df = pd.read_csv(dataset.path)

    # split dataset according to flag column - or maybe return ts so its easier to create metadata
    (X_train, y_train), (X_test, y_test), (X_legacy_test,
                                           y_legacy_test), model_metadata = split_dataset(dataset_df)

    # define model and fit
    new_model = LogisticRegression()  # ?? risk prediction ??
    new_model.fit(X_train, y_train)

    # evaluate on new and legacy test data
    preds = new_model.predict(X_test)
    performance_test = perfomance_metric(y_test, preds)
    metrics.log_metric("?", performance_test)

    preds = new_model.predict(X_legacy_test)
    performance_legacy = perfomance_metric(y_legacy_test, preds)
    metrics.log_metric("?", performance_legacy)


@component(
    base_image="python:3.11",
    packages_to_install=["pandas", "scikit-learn", "joblib"],
)
def compare_models(
    previous_model: Input[Model],
    new_model: Input[Model],
    new_metrics: Input[Metrics]
):
    pass


@component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-aiplatform"],
)
def register_model(
    project_id: str,
    region: str,
    model: Input[Model],
    training_metadata: Input[dict],
    registered_model: Output[Model],

):
    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=region)

    uploaded_model = aiplatform.Model.upload(
        display_name="septic-shock-risk",
        artifact_uri="gs://my-bucket/models/septic-risk/v3/",
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest",
        metadata=training_metadata,
    )

    registered_model.uri = uploaded_model.resource_name


@component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-aiplatform"],
)
def deploy_model(
    model: Model,
    project_id: str,
    region: str,
):
    from google.cloud import aiplatform

    aiplatform.init(project=project_id, location=region)

    model_obj = aiplatform.Model(model.uri)

    endpoint = aiplatform.Endpoint.create(
        display_name="septic-shock-endpoint"
    )

    model_obj.deploy(
        endpoint=endpoint,
        machine_type="n1-standard-2",
    )


# Pipeline Definition

@dsl.pipeline(
    name="septic-shock-training-pipeline",
    description="BQ ingestion → training → model registration → deployment",
)
def septic_shock_pipeline(
    project_id: str,
    region: str,
    bq_dataset: str,
    bq_data_table: str,
    bq_label_table: str,
):

    model = aiplatform.Model("projects/.../locations/.../models/123456789")

    ingest_task = ingest_from_bigquery(
        project_id=project_id,
        dataset_id=bq_dataset,
        data_table_id=bq_data_table,
        label_table_id=bq_label_table,
        model=model
    )

    train_task = train_model(
        dataset=ingest_task.output_dataset
    )

    comparison_task = compare_models(

    )

    with dsl.Condition(comparison_task.outputs["deploy"] == "true"):
        register_task = register_model(
            model=train_task.outputs["model"],
            project_id=project_id,
            region=region,
        )

        deploy_model(
            model=register_task.outputs["registered_model"],
            project_id=project_id,
            region=region,
        ).after(register_task)
