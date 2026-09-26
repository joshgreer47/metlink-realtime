# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Data quality checks
# MAGIC Runs after each pipeline update. Every result is appended to `<catalog>.ops.data_quality_results`, and the
# MAGIC notebook fails if any check fails, so the job fails and sends its notification.
# MAGIC
# MAGIC | Parameter | Default | Notes |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `metlink` | |
# MAGIC | `static_catalog` | `metlink` | Catalog holding the static GTFS tables |
# MAGIC | `pipeline_id` | | Pipeline whose event log is checked for expectation drop rates |
# MAGIC | `check_freshness` | `true` | Set to `false` for simulated data |
# MAGIC | `max_staleness_minutes` | `60` | Newest collected poll (any feed) must be younger than this |
# MAGIC | `min_collection_coverage` | `0.9` | Share of 5-minute slots with at least one poll over the last 24 hours |
# MAGIC | `max_drop_rate` | `0.01` | Largest acceptable share of rows failing any expectation |
# MAGIC | `informational_expectations` | `has_trip,plausible_delay` | Warn-only expectations excluded from the drop-rate check |
# MAGIC | `min_timetable_days` | `3` | Static timetable must cover at least this many days ahead |

# COMMAND ----------

dbutils.widgets.text("catalog", "metlink")
dbutils.widgets.text("static_catalog", "metlink")
dbutils.widgets.text("pipeline_id", "")
dbutils.widgets.dropdown("check_freshness", "true", ["true", "false"])
dbutils.widgets.text("max_staleness_minutes", "60")
dbutils.widgets.text("min_collection_coverage", "0.9")
dbutils.widgets.text("max_drop_rate", "0.01")
dbutils.widgets.text("informational_expectations", "has_trip,plausible_delay")
dbutils.widgets.text("min_timetable_days", "3")

catalog = dbutils.widgets.get("catalog")
static_catalog = dbutils.widgets.get("static_catalog")
pipeline_id = dbutils.widgets.get("pipeline_id")
check_freshness = dbutils.widgets.get("check_freshness") == "true"
max_staleness_minutes = float(dbutils.widgets.get("max_staleness_minutes"))
min_collection_coverage = float(dbutils.widgets.get("min_collection_coverage"))
max_drop_rate = float(dbutils.widgets.get("max_drop_rate"))
informational = {e.strip() for e in dbutils.widgets.get("informational_expectations").split(",") if e.strip()}
min_timetable_days = int(dbutils.widgets.get("min_timetable_days"))

BRONZE_TABLES = ("rt_trip_updates", "rt_vehicle_positions", "rt_service_alerts")
SLOT_MINUTES = 5

# COMMAND ----------

from datetime import datetime, timezone

run_at = datetime.now(timezone.utc)
results = []


def record(check: str, subject: str, passed: bool, value, threshold, detail: str = "") -> None:
    results.append(
        {
            "run_at": run_at,
            "catalog": catalog,
            "check": check,
            "subject": subject,
            "status": "pass" if passed else "fail",
            "value": None if value is None else float(value),
            "threshold": None if threshold is None else float(threshold),
            "detail": detail,
        }
    )


def scalar(sql: str):
    return spark.sql(sql).first()[0]

# COMMAND ----------

# Freshness and gaps in collection, measured on bronze across all feeds. The collector skips polls of unchanged
# feeds, so a single quiet feed is normal; no poll of any feed means the collector was down.
if check_freshness:
    polls = " UNION ALL ".join(
        f"SELECT CAST(fetched_at AS TIMESTAMP) AS fetched_at FROM {catalog}.bronze.{t}" for t in BRONZE_TABLES
    )
    row = spark.sql(f"""
        WITH polls AS ({polls})
        SELECT
          (unix_timestamp(current_timestamp()) - unix_timestamp(max(fetched_at))) / 60 AS age_min,
          count(DISTINCT CASE WHEN fetched_at >= current_timestamp() - INTERVAL 24 HOURS
                              THEN floor(unix_timestamp(fetched_at) / ({SLOT_MINUTES} * 60)) END) AS slots,
          (unix_timestamp(current_timestamp())
             - unix_timestamp(greatest(min(fetched_at), current_timestamp() - INTERVAL 24 HOURS))) / 60 AS window_min
        FROM polls
    """).first()
    if row.age_min is None:
        record("freshness", "collector", False, None, max_staleness_minutes, "no collected data")
    else:
        record("freshness", "collector", row.age_min <= max_staleness_minutes, row.age_min, max_staleness_minutes,
               "minutes since the newest poll of any feed")
        expected = max(row.window_min // SLOT_MINUTES, 1)
        coverage = min(row.slots / expected, 1.0)
        record("collection_coverage", "collector", coverage >= min_collection_coverage, coverage,
               min_collection_coverage, f"{row.slots} of ~{int(expected)} 5-minute slots polled in the last 24 hours")

# COMMAND ----------

# Expectation drop rates for the pipeline's latest update.
if pipeline_id:
    latest_update = scalar(f"""
        SELECT origin.update_id FROM event_log('{pipeline_id}')
        WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1
    """)
    expectations = spark.sql(f"""
        SELECT e.dataset, e.name, sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
        FROM (
          SELECT explode(from_json(details:flow_progress:data_quality:expectations,
            'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>')) AS e
          FROM event_log('{pipeline_id}')
          WHERE event_type = 'flow_progress' AND origin.update_id = '{latest_update}'
        )
        GROUP BY ALL
    """).collect()
    for e in expectations:
        if e.name in informational:
            continue
        total = (e.passed or 0) + (e.failed or 0)
        rate = (e.failed or 0) / total if total else 0.0
        record("expectation_drop_rate", f"{e.dataset}.{e.name}", rate <= max_drop_rate, rate, max_drop_rate,
               f"{e.failed or 0} of {total} rows in update {latest_update}")

# COMMAND ----------

# Gold reconciles with silver, and every arrival joins to the static timetable.
final_arrivals = scalar(f"SELECT count(*) FROM {catalog}.silver.stop_arrivals WHERE is_final")
gold_arrivals = scalar(f"SELECT coalesce(sum(arrivals), 0) FROM {catalog}.gold.route_ontime_hourly")
record("gold_reconciliation", "route_ontime_hourly", final_arrivals == gold_arrivals, gold_arrivals, final_arrivals,
       "sum(arrivals) vs final stop_arrivals")

coverage = scalar(f"""
    SELECT avg(CASE WHEN route_short_name IS NOT NULL AND stop_name IS NOT NULL THEN 1.0 ELSE 0.0 END)
    FROM {catalog}.silver.stop_arrivals
""")
record("static_join_coverage", "stop_arrivals", coverage is None or coverage >= 0.99, coverage, 0.99,
       "share of arrivals matched to a route and stop")

# The static timetable must stay valid for a few days ahead, otherwise new trips stop matching.
days_ahead = scalar(f"""
    SELECT datediff(max(to_date(end_date, 'yyyyMMdd')), current_date()) FROM {static_catalog}.bronze.gtfs_calendar
""")
record("timetable_validity", "gtfs_calendar", days_ahead is not None and days_ahead >= min_timetable_days,
       days_ahead, min_timetable_days, "days until the latest calendar end_date")

# COMMAND ----------

from pyspark.sql import types as T

schema = T.StructType([
    T.StructField("run_at", T.TimestampType()),
    T.StructField("catalog", T.StringType()),
    T.StructField("check", T.StringType()),
    T.StructField("subject", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("value", T.DoubleType()),
    T.StructField("threshold", T.DoubleType()),
    T.StructField("detail", T.StringType()),
])
df = spark.createDataFrame(results, schema)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")
df.write.mode("append").saveAsTable(f"{catalog}.ops.data_quality_results")
display(df)

# COMMAND ----------

failures = [r for r in results if r["status"] == "fail"]
if failures:
    summary = "; ".join(f"{r['check']}[{r['subject']}]={r['value']} (threshold {r['threshold']})" for r in failures)
    raise AssertionError(f"{len(failures)} data quality check(s) failed: {summary}")
print(f"All {len(results)} checks passed")
