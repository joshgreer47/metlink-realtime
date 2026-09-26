# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Egress check
# MAGIC Reports whether serverless compute in this workspace can reach the Metlink endpoints.
# MAGIC Workspaces with restricted outbound access have to use the local collector (`poller/`) for ingestion.
# MAGIC
# MAGIC Any HTTP status, including `403` from the unauthenticated realtime API, means the host is reachable.
# MAGIC A timeout or connection error means it is blocked.

# COMMAND ----------

import requests

targets = {
    "static GTFS": "https://static.opendata.metlink.org.nz/v1/gtfs/full.zip",
    "realtime API": "https://api.opendata.metlink.org.nz/v1/gtfs-rt/vehiclepositions",
    "control (pypi.org)": "https://pypi.org/simple/",
}

for name, url in targets.items():
    try:
        r = requests.head(url, timeout=10, allow_redirects=True)
        print(f"{name:20} reachable  HTTP {r.status_code}")
    except requests.RequestException as e:
        print(f"{name:20} BLOCKED    {type(e).__name__}: {e}")
