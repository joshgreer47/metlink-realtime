"""Silver: trip update predictions and per-stop delay history."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "metlink")
TZ = "Pacific/Auckland"


def epoch_to_ts(col: str):
    return F.timestamp_seconds(F.col(col).cast("bigint"))


@dp.table(
    name=f"{CATALOG}.silver.trip_updates",
    comment="One row per stop-time prediction in a trip update, deduplicated across polls",
    cluster_by=["service_date", "route_id"],
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("has_keys", "trip_id IS NOT NULL AND service_date IS NOT NULL AND stop_sequence IS NOT NULL")
@dp.expect_or_drop("has_update_time", "updated_at IS NOT NULL")
@dp.expect("plausible_delay", "abs(arrival_delay_s) < 10800")
def trip_updates():
    return (
        spark.readStream.table(f"{CATALOG}.bronze.rt_trip_updates")
        .select(
            F.col("fetched_at").cast("timestamp").alias("fetched_at"),
            F.explode("feed_message.entity").alias("e"),
        )
        .select(
            "fetched_at",
            F.col("e.trip_update.trip.trip_id").alias("trip_id"),
            F.col("e.trip_update.trip.route_id").alias("route_id"),
            F.col("e.trip_update.trip.direction_id").cast("int").alias("direction_id"),
            F.to_date("e.trip_update.trip.start_date", "yyyyMMdd").alias("service_date"),
            F.col("e.trip_update.trip.start_time").alias("start_time"),
            F.col("e.trip_update.trip.schedule_relationship").alias("trip_schedule_relationship"),
            F.col("e.trip_update.vehicle.id").alias("vehicle_id"),
            epoch_to_ts("e.trip_update.timestamp").alias("updated_at"),
            F.explode("e.trip_update.stop_time_update").alias("stu"),
        )
        .select(
            "*",
            F.col("stu.stop_id").alias("stop_id"),
            F.col("stu.stop_sequence").cast("int").alias("stop_sequence"),
            F.col("stu.schedule_relationship").alias("stop_schedule_relationship"),
            F.col("stu.arrival.delay").cast("int").alias("arrival_delay_s"),
            epoch_to_ts("stu.arrival.time").alias("predicted_arrival_at"),
        )
        .drop("stu")
        .withColumn("updated_at_local", F.from_utc_timestamp("updated_at", TZ))
        .withColumn("predicted_arrival_at_local", F.from_utc_timestamp("predicted_arrival_at", TZ))
        .withWatermark("updated_at", "30 minutes")
        .dropDuplicatesWithinWatermark(["trip_id", "service_date", "stop_sequence", "updated_at"])
    )


dp.create_streaming_table(
    name=f"{CATALOG}.silver.trip_stop_delays",
    comment="History of predicted arrival delay for each trip and stop (SCD type 2)",
    cluster_by=["service_date", "route_id"],
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.trip_stop_delays",
    source=f"{CATALOG}.silver.trip_updates",
    keys=["trip_id", "service_date", "stop_sequence"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=2,
    track_history_column_list=["arrival_delay_s", "predicted_arrival_at"],
)
