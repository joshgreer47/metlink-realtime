# Airflow orchestration

An [Astro](https://www.astronomer.io/docs/astro/cli/overview) project that orchestrates the Databricks resources
deployed by the bundle from Apache Airflow 3, as an alternative to the `metlink-daily` Databricks job.

| DAG | Schedule | What it does |
|---|---|---|
| `metlink_daily` | 05:00 daily | Checks the Metlink API, loads static GTFS, updates the pipeline, runs the data quality notebook, checks freshness in SQL, then publishes the `databricks://<catalog>/gold` asset |
| `metlink_ml_retrain` | On the gold asset | Runs the `metlink-ml-arrival-delay` job |
| `metlink_backfill` | Manual | Recomputes selected tables, or everything, through the Pipelines API |

Tasks run on serverless compute through the Jobs `runs/submit` API, against the notebooks deployed by
`databricks bundle deploy`.

## Run locally

Requires [Docker Desktop](https://www.docker.com/products/docker-desktop/) and the Astro CLI
(`winget install -e --id Astronomer.Astro`).

```bash
cd airflow
astro dev init          # first time only: adds the Dockerfile and project files around the existing dags/
cp .env.example .env    # fill in the connection, API key and deployed resource names
astro dev start         # Airflow UI at http://localhost:8080
```

`astro dev init` asks before writing into a non-empty folder. It keeps the existing `dags/` and `requirements.txt`.

Avoid running the Airflow DAG and the `metlink-daily` job for the same day: they do the same work.
