# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · Arrival delay model: batch scoring
# MAGIC Scores the `@champion` model for every trip in progress at the latest refresh. For each trip's most recent
# MAGIC final stop, it predicts the delay at each of the next stops (up to 15), and writes
# MAGIC `<catalog>.ml.arrival_predictions`, replacing the previous snapshot. Route features are looked up point-in-time
# MAGIC by the feature store client.
# MAGIC
# MAGIC | Parameter | Default |
# MAGIC |---|---|
# MAGIC | `catalog` | `metlink` |
# MAGIC | `static_catalog` | `metlink` |
# MAGIC | `as_of` | blank | Local (Pacific/Auckland) time to score at, `YYYY-MM-DD HH:MM:SS`; blank means the latest arrival |

# COMMAND ----------

# MAGIC %pip install -q databricks-feature-engineering mlflow scikit-learn
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "metlink")
dbutils.widgets.text("static_catalog", "metlink")
dbutils.widgets.text("as_of", "")
catalog = dbutils.widgets.get("catalog")
as_of = dbutils.widgets.get("as_of").strip()
static_catalog = dbutils.widgets.get("static_catalog")
model_uri = f"models:/{catalog}.ml.arrival_delay@champion"
MAX_STOPS_AHEAD = 15
IN_PROGRESS_WINDOW = "INTERVAL 15 MINUTES"
MODES = ["bus", "school bus", "rail", "ferry", "cable car"]

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

arrivals = spark.table(f"{catalog}.silver.stop_arrivals").where("is_final")
if as_of:
    snapshot_at = spark.sql(f"SELECT to_utc_timestamp('{as_of}', 'Pacific/Auckland')").first()[0]
    arrivals = arrivals.where(F.col("arrival_at") <= F.lit(snapshot_at))
else:
    snapshot_at = arrivals.agg(F.max("arrival_at")).first()[0]
print(f"scoring trips in progress at {snapshot_at} UTC")

# Each in-progress trip's latest final stop.
latest = (
    arrivals.where(F.col("arrival_at") >= F.lit(snapshot_at) - F.expr(IN_PROGRESS_WINDOW))
    .withColumn("rn", F.row_number().over(Window.partitionBy("trip_id", "service_date").orderBy(F.desc("stop_sequence"))))
    .where("rn = 1")
    .drop("rn")
)

# Remaining scheduled stops, timed relative to the current stop's scheduled arrival.
offset_s = F.expr(
    "CAST(split(arrival_time, ':')[0] AS INT) * 3600 + CAST(split(arrival_time, ':')[1] AS INT) * 60 "
    "+ CAST(split(arrival_time, ':')[2] AS INT)"
)
stop_times = spark.table(f"{static_catalog}.bronze.gtfs_stop_times").select(
    "trip_id", F.col("stop_sequence").cast("int").alias("stop_sequence"), "stop_id", offset_s.alias("offset_s")
)
stops = spark.table(f"{static_catalog}.bronze.gtfs_stops").select("stop_id", "stop_name")
mode_code = F.array_position(F.array(*[F.lit(m) for m in MODES]), F.col("mode")).cast("int")

current = latest.join(
    stop_times.select("trip_id", "stop_sequence", F.col("offset_s").alias("current_offset_s")),
    ["trip_id", "stop_sequence"],
)
candidates = (
    current.alias("c")
    .join(stop_times.alias("t"), "trip_id")
    .where(F.col("t.stop_sequence") > F.col("c.stop_sequence"))
    .where(F.col("t.stop_sequence") - F.col("c.stop_sequence") <= MAX_STOPS_AHEAD)
    .select(
        F.col("c.trip_id").alias("trip_id"),
        F.col("c.service_date").alias("service_date"),
        F.col("c.route_id").alias("route_id"),
        F.col("c.route_label").alias("route_label"),
        F.col("c.vehicle_id").alias("vehicle_id"),
        F.col("c.arrival_at").alias("observed_at"),
        F.col("c.arrival_delay_s").cast("double").alias("current_delay_s"),
        F.col("t.stop_sequence").alias("target_stop_sequence"),
        F.col("t.stop_id").alias("target_stop_id"),
        (F.col("t.stop_sequence") - F.col("c.stop_sequence")).cast("int").alias("stops_ahead"),
        ((F.col("t.offset_s") - F.col("c.current_offset_s")) / 60).cast("double").alias("scheduled_minutes_ahead"),
        F.expr("timestampadd(SECOND, t.offset_s - c.current_offset_s, c.scheduled_arrival_at)").alias(
            "target_scheduled_at"
        ),
        F.hour("c.scheduled_arrival_at_local").alias("hour"),
        F.dayofweek("c.scheduled_arrival_at_local").alias("day_of_week"),
        mode_code.alias("mode_code"),
    )
)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()
scored = (
    fe.score_batch(model_uri=model_uri, df=candidates, result_type="double")
    .withColumnRenamed("prediction", "predicted_delay_s")
    .join(stops.withColumnRenamed("stop_id", "target_stop_id"), "target_stop_id", "left")
    .select(
        F.lit(snapshot_at).alias("snapshot_at"),
        "trip_id",
        "service_date",
        "route_id",
        "route_label",
        "vehicle_id",
        "current_delay_s",
        "target_stop_sequence",
        "target_stop_id",
        "stop_name",
        "stops_ahead",
        "target_scheduled_at",
        F.round("predicted_delay_s").cast("int").alias("predicted_delay_s"),
        F.expr("timestampadd(SECOND, CAST(round(predicted_delay_s) AS INT), target_scheduled_at)").alias(
            "predicted_arrival_at"
        ),
    )
)
scored.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{catalog}.ml.arrival_predictions")
print(f"scored {spark.table(f'{catalog}.ml.arrival_predictions').count():,} stop predictions as of {snapshot_at}")
