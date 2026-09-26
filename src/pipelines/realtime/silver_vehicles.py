"""Silver: vehicle positions and current vehicle state."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "metlink")
TZ = "Pacific/Auckland"


def epoch_to_ts(col: str):
    return F.timestamp_seconds(F.col(col).cast("bigint"))


@dp.table(
    name=f"{CATALOG}.silver.vehicle_positions",
    comment="One row per reported vehicle position, deduplicated across polls",
    cluster_by=["service_date", "route_id"],
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("has_vehicle_id", "vehicle_id IS NOT NULL")
@dp.expect_or_drop("has_position_time", "position_at IS NOT NULL")
@dp.expect_or_drop("in_region", "latitude BETWEEN -42 AND -40 AND longitude BETWEEN 174 AND 177")
@dp.expect("has_trip", "trip_id IS NOT NULL")
def vehicle_positions():
    return (
        spark.readStream.table(f"{CATALOG}.bronze.rt_vehicle_positions")
        .select(
            F.col("fetched_at").cast("timestamp").alias("fetched_at"),
            F.explode("feed_message.entity").alias("e"),
        )
        .select(
            F.col("e.vehicle.vehicle.id").alias("vehicle_id"),
            F.col("e.vehicle.trip.trip_id").alias("trip_id"),
            F.col("e.vehicle.trip.route_id").alias("route_id"),
            F.col("e.vehicle.trip.direction_id").cast("int").alias("direction_id"),
            F.to_date("e.vehicle.trip.start_date", "yyyyMMdd").alias("service_date"),
            F.col("e.vehicle.trip.start_time").alias("start_time"),
            F.col("e.vehicle.trip.schedule_relationship").alias("schedule_relationship"),
            F.col("e.vehicle.position.latitude").cast("double").alias("latitude"),
            F.col("e.vehicle.position.longitude").cast("double").alias("longitude"),
            F.col("e.vehicle.position.bearing").cast("double").alias("bearing"),
            F.col("e.vehicle.occupancy_status").alias("occupancy_status"),
            epoch_to_ts("e.vehicle.timestamp").alias("position_at"),
            "fetched_at",
        )
        .withColumn("position_at_local", F.from_utc_timestamp("position_at", TZ))
        .withWatermark("position_at", "30 minutes")
        .dropDuplicatesWithinWatermark(["vehicle_id", "position_at"])
    )


dp.create_streaming_table(
    name=f"{CATALOG}.silver.vehicle_current",
    comment="Latest known position of each vehicle (SCD type 1)",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.vehicle_current",
    source=f"{CATALOG}.silver.vehicle_positions",
    keys=["vehicle_id"],
    sequence_by=F.col("position_at"),
    stored_as_scd_type=1,
)
