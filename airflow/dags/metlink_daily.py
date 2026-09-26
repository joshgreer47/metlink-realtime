"""Daily refresh orchestrated from Airflow: the same flow as the `metlink-daily` Databricks job.

check API -> load static GTFS -> update pipeline -> data quality notebook -> SQL freshness check -> publish asset
"""

import os

import pendulum
import requests
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.sdk import dag, task
from metlink_common import CATALOG, DATABRICKS_CONN_ID, GOLD_REFRESHED, notebook_task, pipeline_id, submit_run


@dag(
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Pacific/Auckland"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["metlink"],
)
def metlink_daily():
    @task
    def check_metlink_api() -> None:
        response = requests.get(
            "https://api.opendata.metlink.org.nz/v1/gtfs-rt/servicealerts",
            headers={"x-api-key": os.environ["METLINK_API_KEY"], "Accept": "application/x-protobuf"},
            timeout=20,
        )
        response.raise_for_status()

    @task
    def resolve_pipeline() -> str:
        return pipeline_id(DatabricksHook(DATABRICKS_CONN_ID))

    load_static = DatabricksSubmitRunOperator(
        task_id="load_static_gtfs",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=submit_run("airflow: load static GTFS", notebook_task("02_load_static_gtfs", catalog=CATALOG)),
    )

    refresh = DatabricksSubmitRunOperator(
        task_id="refresh_pipeline",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=submit_run(
            "airflow: refresh pipeline",
            {"pipeline_task": {"pipeline_id": "{{ ti.xcom_pull(task_ids='resolve_pipeline') }}"}},
        ),
    )

    quality = DatabricksSubmitRunOperator(
        task_id="data_quality",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=submit_run(
            "airflow: data quality",
            notebook_task(
                "03_data_quality",
                catalog=CATALOG,
                pipeline_id="{{ ti.xcom_pull(task_ids='resolve_pipeline') }}",
            ),
        ),
    )

    fresh = SQLCheckOperator(
        task_id="check_fresh_arrivals",
        conn_id=DATABRICKS_CONN_ID,
        sql=f"""
            SELECT max(arrival_at) >= current_timestamp() - INTERVAL 2 HOURS
            FROM {CATALOG}.silver.stop_arrivals
        """,
        outlets=[GOLD_REFRESHED],
    )

    check_metlink_api() >> load_static
    [load_static, resolve_pipeline()] >> refresh >> quality >> fresh


metlink_daily()
