"""Retrains and rescores the arrival delay model whenever the daily refresh publishes fresh gold data."""

import pendulum
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import dag
from metlink_common import DATABRICKS_CONN_ID, GOLD_REFRESHED, ML_JOB_NAME


@dag(
    schedule=[GOLD_REFRESHED],
    start_date=pendulum.datetime(2026, 9, 1, tz="Pacific/Auckland"),
    catchup=False,
    tags=["metlink", "ml"],
)
def metlink_ml_retrain():
    DatabricksRunNowOperator(
        task_id="run_ml_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name=ML_JOB_NAME,
    )


metlink_ml_retrain()
