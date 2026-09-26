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
  runtime.py              Logging, single-instance lock, sleep prevention
  official_performance.py Downloads Metlink's published per-route bus performance files
src/notebooks/            Databricks notebooks
  00_setup.py             Catalog, schemas and volume
  01_connectivity_test.py Workspace egress check
  02_load_static_gtfs.py  Static GTFS CSVs to bronze Delta tables (skips versions already loaded)
  03_data_quality.py      Freshness, coverage, expectation and reconciliation checks
  10_ml_features.py       Point-in-time route delay feature table
  11_ml_train.py          Arrival delay model: training, MLflow tracking, UC registration
  12_ml_batch_inference.py  Batch scoring of in-progress trips
  20_classify_alerts.py   Service alert classification with AI Functions
src/pipelines/realtime/   Lakeflow Declarative Pipeline: realtime bronze, silver and gold
src/dashboards/           AI/BI dashboard definitions
src/app/                  Departures board (Databricks Apps, Streamlit)
resources/                Bundle resources: Genie space, SQL alerts, app, secret scope
airflow/                  Astro project: the same orchestration as Airflow DAGs
scripts/windows/          Scheduled-task install/uninstall for the collector
scripts/grant_app_access.py  Grants an app's service principal read access to a catalog
simulator/                Synthetic GTFS-RT history, calibrated from collected data
  generate.py             Timetable-driven feed generator and uploader
  calibrate.py            Measures model parameters from the silver tables
  calibration.json        Current calibration
databricks.yml            Databricks Asset Bundle (pipeline, jobs and dashboard; dev and sim targets)
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
| `STATIC_REFRESH_HOURS` | How often the collector checks for a new static GTFS version (default `24`, `0` disables) |

### Provision the workspace
Deploy with the bundle, then create the catalog, schemas and volume:
```bash
databricks bundle deploy
databricks bundle run setup
```

The bundle also creates a secret scope, `metlink-<target>`. Store the Metlink API key in it for workspace-side
use (the `01_connectivity_test` notebook makes an authenticated request with it):
```bash
databricks secrets put-secret metlink-dev metlink-api-key
```

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

Only one collector can run at a time (`data/metlink_poller.lock`). Options:

| Flag | Effect |
|---|---|
| `--log-file PATH` | Also log to a file, rotated at 5 MB |
| `--keep-awake` | Prevent Windows from sleeping while collecting |
| `--once` | Poll each feed once, then exit |
| `--no-upload` | Keep batches in `data/spool/` |

#### Run as a background service (Windows)
From an elevated PowerShell:
```powershell
.\scripts\windows\install-collector-task.ps1     # starts now, at boot and at logon; restarts on failure
.\scripts\windows\uninstall-collector-task.ps1   # remove
```
The task runs without an interactive logon, keeps the machine awake, and logs to `data/logs/poller.log`.
Closing a laptop lid still sleeps the machine unless the lid action is set to "Do nothing".

## Realtime pipeline

`metlink-realtime` is a serverless Lakeflow Declarative Pipeline in triggered mode.

### Jobs

| Job | Schedule | Tasks |
|---|---|---|
| `metlink-daily` | 05:00 Pacific/Auckland | Load a new static GTFS version if there is one, update the pipeline, then run data quality checks and classify new service alerts. Emails the owner on failure. |
| `metlink-setup` | On demand | Create the catalog, schemas and volume |
| `metlink-ml-arrival-delay` | On demand | Refresh the feature table, retrain, and score trips in progress |
| `metlink-realtime-on-arrival` | File arrival, paused | Event-driven alternative: update the pipeline when new batches land, at most hourly |

The collector checks for a new static GTFS version once a day and uploads it in the background, so the daily job
picks it up without manual steps.

### Benchmark against Metlink's published figures

Metlink publishes per-route daily and weekly bus performance files. The collector downloads new releases once a day
(`poller/official_performance.py`), and the pipeline reproduces Metlink's measure from the realtime feed:

> **Bus punctuality:** the share of trips sighted departing their first stop less than 1 min 15 s early and less than
> 5 min 15 s late. The denominator is trips sighted at the first stop.

| Table | Grain |
|---|---|
| `gold.origin_departures` | One row per bus trip sighted leaving its first stop |
| `gold.route_punctuality_daily`, `gold.route_punctuality_weekly` | Per route and day or Monday-starting week, laid out like Metlink's files |
| `silver.official_bus_performance_daily`, `silver.official_bus_performance_weekly` | Metlink's published figures (latest release) |
| `gold.punctuality_benchmark` | Per route plus `All routes`: our punctuality with a 95% Wilson interval, against Metlink's figure for the same weeks or, until those are published, its latest published weeks |

This measure (departure from the first stop) differs from the dashboard's on-time rate, which covers arrivals at
every observed stop. The first shows whether services start on time, the second whether they stay on time.

### Network analysis

| Table | Grain |
|---|---|
| `gold.road_speeds_h3` | Median in-service vehicle speed per H3 cell (resolution 9), mode group, day type and hour |
| `gold.congestion_hotspots` | Bus speed per cell in the weekday morning peak (07:00–09:00) against midday, with the peak slowdown |
| `gold.segment_runtimes` | Actual against scheduled running time between consecutive observed stops, per route, direction, day type and hour |
| `gold.timetable_shortfalls` | Segments where the median trip takes at least 60 s longer than timetabled, from at least 20 trips |
| `gold.alert_classifications` | AI-assigned cause and impact categories and a one-line summary per distinct alert text (`20_classify_alerts`) |

Speeds come from consecutive position reports, so they include time at stops and signals. Compare cells across
hours rather than reading them as free-flow speeds. `silver.vehicle_current` has Change Data Feed enabled.

### Dashboard

`Metlink network performance` is an AI/BI dashboard deployed with the bundle. Its datasets resolve against the
target's catalog, so the `sim` target shows simulated data. It has three pages:

- **Network performance**: on-time rate, average delay and arrivals; on-time rate by hour; a delay heatmap by
  weekday and hour; and all routes ranked by on-time rate.
- **Headways and bunching**: bunching counts and rates, headway regularity, bunching by hour and route, and an
  event list.
- **Latest vehicles and alerts**: a map of vehicle positions from the latest refresh, and the active service alerts.

To change the dashboard, edit it in the workspace, then pull the changes back into the repository:
```bash
databricks bundle generate dashboard --resource network_performance --force
```

### Data quality

`03_data_quality` appends one row per check to `ops.data_quality_results`, and fails the job if any check fails:

| Check | Fails when |
|---|---|
| `freshness` | No poll of any feed in the last 60 minutes |
| `collection_coverage` | Fewer than 90% of 5-minute slots in the last 24 hours contain a poll |
| `expectation_drop_rate` | Any dropping expectation rejects more than 1% of rows in the latest update |
| `gold_reconciliation` | `gold.route_ontime_hourly` arrivals differ from final `silver.stop_arrivals` |
| `static_join_coverage` | Fewer than 99% of arrivals match a static route and stop |
| `timetable_validity` | The static timetable ends within 3 days |

Freshness and coverage checks are disabled for simulated data.

| Table | Type | Grain |
|---|---|---|
| `bronze.rt_trip_updates`, `bronze.rt_vehicle_positions`, `bronze.rt_service_alerts` | Streaming (Auto Loader) | One row per poll |
| `silver.vehicle_positions` | Streaming | One row per vehicle position report |
| `silver.vehicle_current` | AUTO CDC, SCD1 | One row per vehicle, latest position |
| `silver.trip_updates` | Streaming | One row per stop-time prediction |
| `silver.trip_stop_delays` | AUTO CDC, SCD2 | One version per change in predicted delay, per trip and stop |
| `silver.service_alerts` | AUTO CDC, SCD2 | One version per change to an alert |
| `silver.service_alert_entities` | Materialized view | One row per route, stop or trip affected by an active alert |
| `silver.routes` | Materialized view | One row per route: mode, mode group (bus, rail, ferry & cable car) and display label |
| `silver.stop_arrivals` | Materialized view | One row per trip and stop: final observed arrival vs schedule, with route and stop attributes |
| `gold.route_ontime_hourly` | Materialized view | Punctuality per route, direction, service date and hour |
| `gold.stop_delay_stats` | Materialized view | Punctuality per stop, day type (weekday, Saturday, Sunday) and hour |
| `gold.headways` | Materialized view | Actual vs scheduled headway between consecutive trips at each stop |
| `gold.headway_regularity` | Materialized view | Headway coefficient of variation and bunching counts per route and hour |
| `gold.bus_bunching_events` | Materialized view | Consecutive trips arriving within 25% of the scheduled headway |

Silver timestamps are UTC, with `*_local` companions in `Pacific/Auckland`. `service_date` is the GTFS trip start
date. Data quality expectations drop records without keys or with positions outside the Wellington region.

Arrivals are classified as **early** (more than 1 minute ahead of schedule), **on time**, or **late** (5 minutes or
more behind). Metlink publishes predictions for each trip's next stop only, so the last prediction recorded for a
stop is used as its observed arrival. The scheduled time is derived from the prediction and its reported delay.

## Arrival delay model

The `metlink-ml-arrival-delay` job predicts the arrival delay 1–15 stops ahead of a trip's current position. The
feed itself only predicts the next stop.

1. `10_ml_features` maintains `ml.route_delay_15min`, a Unity Catalog feature table of recent delay per route,
   keyed by the end of each 15-minute window.
2. `11_ml_train` builds (current stop, later stop) examples from final arrivals and joins route features
   point-in-time. It trains a gradient-boosted regressor and logs it to MLflow. It registers the model as
   `ml.arrival_delay` and gives it the `champion` alias when it beats both the "current delay persists" baseline
   and the existing champion on held-out days.
3. `12_ml_batch_inference` scores every trip in progress at the latest refresh (or at `as_of`) and writes
   `ml.arrival_predictions`.

On 14 days of simulated data, the model's held-out mean absolute error is 91 s, against 102 s for the baseline.
Simulated delays follow the simulator's assumptions, so these figures validate the workflow, not real-world accuracy.

## Genie space and alerts

- **Genie space** `Metlink network performance` (`resources/genie.yml`) covers the gold tables and `silver.routes`,
  with instructions that define both punctuality measures and example SQL.
- **SQL alert** `Metlink route average delay over 10 minutes` (`resources/alerts.yml`) runs at 06:30 after the
  daily job. It emails the owner when any route with at least 50 arrivals averaged more than 10 minutes late on the
  latest service date.

## Departures app

`metlink-departures-<target>` is a Streamlit app on Databricks Apps. It shows expected arrivals (from
`ml.arrival_predictions`), recent arrivals and active alerts for a chosen stop. It runs as its own service
principal, which needs read access to the target catalog once:

```bash
databricks bundle deploy -t sim
python scripts/grant_app_access.py --app metlink-departures-sim --catalog metlink_sim
databricks bundle run -t sim departures      # starts the app and prints its URL
```

On Free Edition, a running app counts towards the serverless compute limit. Stop it
(`databricks apps stop metlink-departures-sim`) before running pipelines.

## Simulated data

`simulator/` generates realistic GTFS-RT history so downstream models and metrics can be developed before enough real
history has been collected. It runs every scheduled trip in the static timetable for the requested service dates,
applying a stochastic delay model, and writes the three feeds in exactly the collector's format.

- **Measured** parameters (initial delay distribution, stop-to-stop delay drift, hourly mean delay and occupancy, per
  mode) are in `simulator/calibration.json`. Regenerate them from collected data with `python -m simulator.calibrate`.
- **Assumed** parameters (weekday peak uplift, disruption incidents, holding at timepoints) are constants at the top of
  `simulator/model.py`.
- Output is deterministic for a given seed. Files are gzip-compressed, and Auto Loader reads them natively.
- Service alerts are the current live alerts, replayed unchanged.

Simulated data lives in its own catalog, `metlink_sim`, and is processed by the same pipeline through the `sim` bundle
target. The pipeline reads the static timetable from `metlink`.

```bash
databricks bundle deploy -t sim
databricks bundle run -t sim setup            # once: creates the metlink_sim catalog, schemas and volume
python -m simulator.generate                  # 14 service days ending yesterday, 60 s polls
databricks bundle run -t sim metlink_realtime
```

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

CI runs linting and tests on every push and pull request (`.github/workflows/ci.yml`). The deploy workflow
(`.github/workflows/deploy.yml`) validates both bundle targets on pull requests and deploys `dev` on pushes to
`main`. It needs `DATABRICKS_HOST` and `DATABRICKS_TOKEN` repository secrets, and skips cleanly without them.

The same daily flow can be orchestrated from Apache Airflow instead; see [airflow/README.md](airflow/README.md).

## Roadmap

- [x] Static GTFS ingestion to bronze Delta tables
- [x] Realtime collection to Unity Catalog volumes
- [x] Incremental bronze ingestion of realtime feeds (Auto Loader)
- [x] Silver models: vehicle positions, current vehicle state (SCD1), trip delay history (SCD2), service alerts
- [x] Gold metrics: on-time performance, stop delay statistics, headway regularity, bus bunching
- [x] Benchmark against Metlink's published punctuality
- [x] Congestion hotspots (H3) and timetable shortfall analysis
- [x] Calibrated feed simulator for synthetic history
- [x] Orchestration: daily multi-task job with data quality checks and failure alerts
- [x] Orchestration with Apache Airflow (Astro project)
- [x] AI/BI dashboard
- [x] Genie space and SQL alerts
- [x] Arrival delay prediction (feature store, MLflow, Unity Catalog model registry, batch scoring)
- [x] Service alert classification (AI Functions)
- [x] Departures board app (Databricks Apps)
- [x] CI/CD: bundle validation and deployment from GitHub Actions
- [ ] Retrain and evaluate the arrival delay model on collected (not simulated) data
- [ ] Transfer reliability, bunching root causes and service change detection

## Data attribution

Contains public transport data provided by Metlink / Greater Wellington Regional Council through the
[Metlink Open Data portal](https://opendata.metlink.org.nz), used in line with its terms of use. This project is not
affiliated with or endorsed by Metlink.
