"""Rebuilds pipeline tables on demand, for example after a logic change or a gap in collection.

Trigger with parameters: `full_refresh` rebuilds everything from the landed files (bronze included); otherwise
`tables` lists the silver and gold tables to recompute. Both are idempotent: rerunning gives the same result.
"""

import time

import pendulum
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.sdk import Param, dag, task
from metlink_common import CATALOG, DATABRICKS_CONN_ID, pipeline_id

POLL_SECONDS = 30
TERMINAL = {"COMPLETED", "FAILED", "CANCELED"}


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="Pacific/Auckland"),
    catchup=False,
    params={
        "full_refresh": Param(False, type="boolean", description="Rebuild every table from the landed files"),
        "tables": Param(
            ["silver.stop_arrivals", "gold.route_ontime_hourly", "gold.headways"],
            type="array",
            description="Tables to recompute when full_refresh is false (schema.table)",
        ),
    },
    tags=["metlink"],
)
def metlink_backfill():
    @task
    def refresh(params: dict) -> str:
        hook = DatabricksHook(DATABRICKS_CONN_ID)
        pipeline = pipeline_id(hook)
        body = (
            {"full_refresh": True}
            if params["full_refresh"]
            else {"full_refresh_selection": [f"{CATALOG}.{t}" for t in params["tables"]]}
        )
        update_id = hook._do_api_call(("POST", f"2.0/pipelines/{pipeline}/updates"), body)["update_id"]
        while True:
            update = hook._do_api_call(("GET", f"2.0/pipelines/{pipeline}/updates/{update_id}"), {})["update"]
            if update["state"] in TERMINAL:
                break
            time.sleep(POLL_SECONDS)
        if update["state"] != "COMPLETED":
            raise RuntimeError(f"pipeline update {update_id} ended {update['state']}")
        return update_id

    refresh()


metlink_backfill()
