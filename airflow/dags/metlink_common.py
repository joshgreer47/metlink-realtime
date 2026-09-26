"""Shared settings and helpers for the Metlink DAGs."""

import os

from airflow.sdk import Asset

DATABRICKS_CONN_ID = "databricks_default"
CATALOG = os.environ.get("METLINK_CATALOG", "metlink")
PIPELINE_NAME = os.environ.get("METLINK_PIPELINE_NAME", "metlink-realtime")
ML_JOB_NAME = os.environ.get("METLINK_ML_JOB_NAME", "metlink-ml-arrival-delay")
BUNDLE_ROOT = os.environ.get("METLINK_BUNDLE_ROOT", "")

# Published when a daily refresh has passed its data quality checks.
GOLD_REFRESHED = Asset(f"databricks://{CATALOG}/gold")


def submit_run(run_name: str, task: dict) -> dict:
    """A one-task runs/submit request. With no cluster specified, the task runs on serverless compute."""
    return {"run_name": run_name, "tasks": [{"task_key": "main", **task}]}


def notebook_task(notebook: str, **parameters: str) -> dict:
    """A notebook task pointing at the deployed bundle's files."""
    return {
        "notebook_task": {
            "notebook_path": f"{BUNDLE_ROOT}/src/notebooks/{notebook}",
            "base_parameters": parameters,
            "source": "WORKSPACE",
        }
    }


def pipeline_id(hook) -> str:
    """Look up the deployed pipeline's id by name."""
    response = hook._do_api_call(("GET", "2.0/pipelines"), {"filter": f"name LIKE '{PIPELINE_NAME}'"})
    matches = [p for p in response.get("statuses", []) if p["name"] == PIPELINE_NAME]
    if len(matches) != 1:
        raise ValueError(f"expected one pipeline named {PIPELINE_NAME!r}, found {len(matches)}")
    return matches[0]["pipeline_id"]
