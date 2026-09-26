"""Silver: final observed arrival per trip and stop, conformed to the static timetable.

Metlink trip updates only predict the next stop, so the last prediction recorded for a stop, before the trip
moves on, is used as its arrival. The scheduled time is the predicted time minus the reported delay.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "metlink")
STATIC_CATALOG = spark.conf.get("static_catalog", CATALOG)
TZ = "Pacific/Auckland"
# A stop still predicted this long after its arrival time is treated as passed.
FINAL_AFTER = "INTERVAL 10 MINUTES"


@dp.materialized_view(
    name=f"{CATALOG}.silver.stop_arrivals",
    comment="Final observed arrival per trip and stop, with scheduled time and static GTFS attributes",
    cluster_by=["service_date", "route_id"],
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("has_delay", "arrival_delay_s IS NOT NULL AND arrival_at IS NOT NULL")
def stop_arrivals():
    last = "updated_at"
    arrivals = (
        spark.read.table(f"{CATALOG}.silver.trip_updates")
        .where("stop_schedule_relationship IS NULL OR stop_schedule_relationship != 'SKIPPED'")
        .groupBy("trip_id", "service_date", "stop_sequence")
        .agg(
            F.max_by("stop_id", last).alias("stop_id"),
            F.max_by("route_id", last).alias("route_id"),
            F.max_by("direction_id", last).alias("direction_id"),
            F.max_by("vehicle_id", last).alias("vehicle_id"),
            F.max_by("start_time", last).alias("start_time"),
            F.max_by("arrival_delay_s", last).alias("arrival_delay_s"),
            F.max_by("predicted_arrival_at", last).alias("arrival_at"),
            F.max(last).alias("last_updated_at"),
            F.count("*").alias("prediction_count"),
        )
        .withColumn("scheduled_arrival_at", F.expr("timestampadd(SECOND, -arrival_delay_s, arrival_at)"))
        .withColumn(
            "is_final",
            (F.col("stop_sequence") < F.max("stop_sequence").over(Window.partitionBy("trip_id", "service_date")))
            | (F.col("arrival_at") < F.expr(f"current_timestamp() - {FINAL_AFTER}")),
        )
    )

    routes = spark.read.table(f"{CATALOG}.silver.routes")
    stops = spark.read.table(f"{STATIC_CATALOG}.bronze.gtfs_stops").select(
        "stop_id",
        "stop_name",
        F.col("stop_lat").cast("double").alias("stop_lat"),
        F.col("stop_lon").cast("double").alias("stop_lon"),
    )
    trips = spark.read.table(f"{STATIC_CATALOG}.bronze.gtfs_trips").select("trip_id", "trip_headsign")
    stop_times = (
        spark.read.table(f"{STATIC_CATALOG}.bronze.gtfs_stop_times")
        .select(
            "trip_id",
            F.col("stop_sequence").cast("int").alias("stop_sequence"),
            (F.col("timepoint") == "1").alias("is_timepoint"),
        )
        .withColumn("is_origin", F.col("stop_sequence") == F.min("stop_sequence").over(Window.partitionBy("trip_id")))
    )

    return (
        arrivals.join(routes, "route_id", "left")
        .join(stops, "stop_id", "left")
        .join(trips, "trip_id", "left")
        .join(stop_times, ["trip_id", "stop_sequence"], "left")
        .withColumn("arrival_at_local", F.from_utc_timestamp("arrival_at", TZ))
        .withColumn("scheduled_arrival_at_local", F.from_utc_timestamp("scheduled_arrival_at", TZ))
    )
