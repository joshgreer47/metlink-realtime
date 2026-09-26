# Metlink Realtime

A Databricks lakehouse for Wellington's [Metlink](https://www.metlink.org.nz) public transport network, built on the
static GTFS timetable and the GTFS-Realtime trip update, vehicle position and service alert feeds.

It collects the realtime feeds continuously, lands them in Unity Catalog, and models them through a bronze / silver /
gold medallion architecture for on-time performance analytics, delay prediction and live departure information.

## Architecture

```
            ┌──────────────────────────────────────┐
            │ Metlink Open Data API                │
            │  GTFS-RT: trip updates, vehicle      │
            │  positions, service alerts           │
            │  Static GTFS (daily zip)             │
            └──────────────┬───────────────────────┘
                           │ poll every 30s
            ┌──────────────▼───────────────────────┐
            │ Collector (poller/)                  │
            │  protobuf → JSONL, dedupe unchanged  │
            │  feeds, 5-min batches, local spool   │
            │  with retry                          │
            └──────────────┬───────────────────────┘
                           │ Databricks Files API
  /Volumes/metlink/bronze/raw/realtime/<feed>/date=YYYY-MM-DD/*.jsonl
  /Volumes/metlink/bronze/raw/gtfs_static/<version>/*.txt
                           │
            ┌──────────────▼───────────────────────┐
            │ Databricks                           │
            │  bronze → silver → gold (Lakeflow)   │
            │  Jobs · AI/BI dashboards · MLflow    │
            └──────────────────────────────────────┘
```

Collection runs outside Databricks and pushes files into a Unity Catalog volume, so the platform needs no outbound
network access. Polls are batched into one file per feed every five minutes, which keeps file counts low for
incremental ingestion.

### Unity Catalog layout

| Object | Purpose |
|---|---|
| `metlink.bronze` | Raw data as landed: static GTFS tables (`gtfs_*`) and realtime polls |
| `metlink.bronze.raw` | Managed volume: landing zone for collector uploads |
| `metlink.silver` | Cleaned, typed and deduplicated |
| `metlink.gold` | Aggregates for BI and ML |
| `metlink.ml` | Feature tables and registered models |

## Repository layout

```
poller/                   Collectors (run outside Databricks)
  metlink_poller.py       GTFS-RT poller: batches polls to JSONL and uploads to the volume
  static_gtfs.py          Static GTFS download and upload, versioned by feed date
  uploader.py             Databricks Files API client
  config.py               Environment-based settings
src/notebooks/            Databricks notebooks
  00_setup.py             Catalog, schemas and volume
  01_connectivity_test.py Workspace egress check
  02_load_static_gtfs.py  Static GTFS CSVs to bronze Delta tables
src/pipelines/realtime/   Lakeflow Declarative Pipeline: realtime bronze and silver
databricks.yml            Databricks Asset Bundle (pipeline and jobs)
tests/                    Unit tests
```

## Getting started

### Prerequisites
- Python 3.10+
- A Databricks workspace with Unity Catalog, and a personal access token
- A Metlink Open Data API key from [opendata.metlink.org.nz](https://opendata.metlink.org.nz)
- Optional: the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) to deploy the bundle

### Install
```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
cp .env.example .env             # then fill in the values
```

| Variable | Description |
|---|---|
| `METLINK_API_KEY` | Metlink Open Data API key |
| `DATABRICKS_HOST` | Workspace URL, e.g. `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal access token |
| `METLINK_VOLUME_PATH` | Target volume (default `/Volumes/metlink/bronze/raw`) |
| `POLL_INTERVAL_SECONDS` | Realtime poll interval (default `30`) |
| `BATCH_SECONDS` | Upload batch interval (default `300`) |

### Provision the workspace
Deploy the notebooks with the bundle (`databricks bundle deploy`) or through a Databricks Git folder. Then run
`00_setup` to create the catalog, schemas and volume.

### Load the static timetable
```bash
python -m poller.static_gtfs
```
Then run `02_load_static_gtfs`, or the `metlink-static-gtfs-refresh` job deployed by the bundle.

### Collect realtime data
```bash
python -m poller.metlink_poller --once --no-upload -v   # smoke test, writes to data/spool/
python -m poller.metlink_poller                         # run continuously
```
Stop with `Ctrl+C`. Buffered polls are flushed and uploaded on exit.

## Realtime pipeline

`metlink-realtime` is a serverless Lakeflow Declarative Pipeline. It runs in triggered mode, refreshed hourly by the
`metlink-realtime-refresh` job.

| Table | Type | Grain |
|---|---|---|
| `bronze.rt_trip_updates`, `bronze.rt_vehicle_positions`, `bronze.rt_service_alerts` | Streaming (Auto Loader) | One row per poll |
| `silver.vehicle_positions` | Streaming | One row per vehicle position report |
| `silver.vehicle_current` | AUTO CDC, SCD1 | One row per vehicle, latest position |
| `silver.trip_updates` | Streaming | One row per stop-time prediction |
| `silver.trip_stop_delays` | AUTO CDC, SCD2 | One version per change in predicted delay, per trip and stop |
| `silver.service_alerts` | AUTO CDC, SCD2 | One version per change to an alert |
| `silver.service_alert_entities` | Materialized view | One row per route, stop or trip affected by an active alert |

Silver timestamps are UTC, with `*_local` companions in `Pacific/Auckland`. `service_date` is the GTFS trip start
date. Data quality expectations drop records without keys or with positions outside the Wellington region.

## Data format

Each realtime file is JSON Lines, with one record per poll of one feed:

```json
{"feed": "vehiclepositions", "fetched_at": "2026-09-26T00:15:00+00:00",
 "header_timestamp": 1790000000, "entity_count": 135, "feed_message": { ... }}
```

- `feed_message` is the GTFS-RT `FeedMessage` in JSON form, using the spec's snake_case field names.
- A poll is skipped if the feed header timestamp hasn't changed since the previous poll.
- Partition dates and file timestamps are UTC.
- 64-bit integer fields inside `feed_message`, such as timestamps, are encoded as strings, following the protobuf
  JSON mapping.
- Failed uploads stay in `data/spool/` and are retried on the next flush.

## Development

```bash
ruff check . && ruff format --check .
pytest
```

CI runs linting and tests on every push and pull request (`.github/workflows/ci.yml`).

## Roadmap

- [x] Static GTFS ingestion to bronze Delta tables
- [x] Realtime collection to Unity Catalog volumes
- [x] Incremental bronze ingestion of realtime feeds (Auto Loader)
- [x] Silver models: vehicle positions, current vehicle state (SCD1), trip delay history (SCD2), service alerts
- [ ] Gold metrics: on-time performance, stop delay statistics, headway regularity, bus bunching
- [ ] Orchestration: multi-task jobs with data quality checks, and an Apache Airflow deployment
- [ ] AI/BI dashboards and a Genie space
- [ ] Arrival delay prediction (MLflow, Unity Catalog model registry)
- [ ] Departures board app (Databricks Apps)

## Data attribution

Contains public transport data provided by Metlink / Greater Wellington Regional Council through the
[Metlink Open Data portal](https://opendata.metlink.org.nz), used in line with its terms of use. This project is not
affiliated with or endorsed by Metlink.
