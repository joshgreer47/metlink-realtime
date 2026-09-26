"""Gold: punctuality, headway and bunching metrics.

Punctuality bands: early is more than 1 minute ahead of schedule, late is 5 minutes or more behind.
A vehicle is bunched when it arrives less than a quarter of the scheduled headway after the one in front.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "metlink")
EARLY_S = -60
LATE_S = 300
BUNCHING_RATIO = 0.25
MAX_SCHEDULED_HEADWAY_S = 7200

PROPS = {"quality": "gold"}


def final_arrivals():
    return (
        spark.read.table(f"{CATALOG}.silver.stop_arrivals")
        .where("is_final")
        .withColumn(
            "punctuality",
            F.when(F.col("arrival_delay_s") < EARLY_S, "early")
            .when(F.col("arrival_delay_s") >= LATE_S, "late")
            .otherwise("on_time"),
        )
        .withColumn("hour", F.hour("scheduled_arrival_at_local"))
    )


def punctuality_metrics():
    return [
        F.count("*").alias("arrivals"),
        F.sum(F.when(F.col("punctuality") == "early", 1).otherwise(0)).alias("early"),
        F.sum(F.when(F.col("punctuality") == "on_time", 1).otherwise(0)).alias("on_time"),
        F.sum(F.when(F.col("punctuality") == "late", 1).otherwise(0)).alias("late"),
        F.round(F.avg(F.when(F.col("punctuality") == "on_time", 1.0).otherwise(0.0)), 4).alias("on_time_rate"),
        F.round(F.avg("arrival_delay_s"), 1).alias("avg_delay_s"),
        F.percentile_approx("arrival_delay_s", 0.5).alias("p50_delay_s"),
        F.percentile_approx("arrival_delay_s", 0.9).alias("p90_delay_s"),
    ]


@dp.materialized_view(
    name=f"{CATALOG}.gold.route_ontime_hourly",
    comment="Punctuality by route, direction, service date and scheduled hour (local time)",
    cluster_by=["service_date", "route_id"],
    table_properties=PROPS,
)
def route_ontime_hourly():
    return (
        final_arrivals()
        .groupBy(
            "service_date",
            "hour",
            "route_id",
            "route_short_name",
            "route_label",
            "route_type",
            "mode",
            "mode_group",
            "direction_id",
        )
        .agg(*punctuality_metrics(), F.countDistinct("trip_id").alias("trips"))
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.stop_delay_stats",
    comment="Punctuality by stop, day type and scheduled hour (local time), across all history",
    cluster_by=["stop_id"],
    table_properties=PROPS,
)
def stop_delay_stats():
    dow = F.dayofweek("scheduled_arrival_at_local")
    return (
        final_arrivals()
        .withColumn("day_type", F.when(dow == 1, "sunday").when(dow == 7, "saturday").otherwise("weekday"))
        .groupBy("stop_id", "stop_name", "stop_lat", "stop_lon", "day_type", "hour")
        .agg(
            *punctuality_metrics(), F.min("service_date").alias("first_date"), F.max("service_date").alias("last_date")
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.headways",
    comment="Actual vs scheduled headway between consecutive trips at each stop",
    cluster_by=["service_date", "route_id"],
    table_properties=PROPS,
)
def headways():
    w = Window.partitionBy("route_id", "direction_id", "stop_id", "service_date").orderBy("scheduled_arrival_at")
    return (
        final_arrivals()
        .select(
            "service_date",
            "hour",
            "route_id",
            "route_short_name",
            "route_label",
            "route_type",
            "mode",
            "mode_group",
            "direction_id",
            "stop_id",
            "stop_name",
            "trip_id",
            "vehicle_id",
            "scheduled_arrival_at",
            "arrival_at",
            F.lag("trip_id").over(w).alias("leading_trip_id"),
            F.lag("vehicle_id").over(w).alias("leading_vehicle_id"),
            F.lag("scheduled_arrival_at").over(w).alias("leading_scheduled_arrival_at"),
            F.lag("arrival_at").over(w).alias("leading_arrival_at"),
        )
        .where("leading_trip_id IS NOT NULL")
        .withColumn(
            "scheduled_headway_s",
            F.unix_timestamp("scheduled_arrival_at") - F.unix_timestamp("leading_scheduled_arrival_at"),
        )
        .withColumn("actual_headway_s", F.unix_timestamp("arrival_at") - F.unix_timestamp("leading_arrival_at"))
        .where(F.col("scheduled_headway_s").between(1, MAX_SCHEDULED_HEADWAY_S))
        .withColumn("headway_ratio", F.round(F.col("actual_headway_s") / F.col("scheduled_headway_s"), 3))
        .withColumn("is_bunched", F.col("headway_ratio") < BUNCHING_RATIO)
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.headway_regularity",
    comment="Headway regularity by route, direction, service date and hour. "
    "headway_cv is stddev(actual - scheduled headway) / mean(scheduled headway); lower is more regular.",
    cluster_by=["service_date", "route_id"],
    table_properties=PROPS,
)
def headway_regularity():
    deviation = F.col("actual_headway_s") - F.col("scheduled_headway_s")
    return (
        spark.read.table(f"{CATALOG}.gold.headways")
        .groupBy(
            "service_date",
            "hour",
            "route_id",
            "route_short_name",
            "route_label",
            "route_type",
            "mode",
            "mode_group",
            "direction_id",
        )
        .agg(
            F.count("*").alias("headways"),
            F.round(F.avg("scheduled_headway_s"), 1).alias("avg_scheduled_headway_s"),
            F.round(F.avg("actual_headway_s"), 1).alias("avg_actual_headway_s"),
            F.round(F.stddev_samp(deviation) / F.avg("scheduled_headway_s"), 3).alias("headway_cv"),
            F.sum(F.col("is_bunched").cast("int")).alias("bunched"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.bus_bunching_events",
    comment="Pairs of consecutive trips arriving at a stop within a quarter of the scheduled headway",
    cluster_by=["service_date", "route_id"],
    table_properties=PROPS,
)
def bus_bunching_events():
    return spark.read.table(f"{CATALOG}.gold.headways").where("is_bunched")


@dp.materialized_view(
    name=f"{CATALOG}.gold.vehicle_latest",
    comment="Vehicles reporting within 10 minutes of the newest position in the latest refresh, with route attributes",
    table_properties=PROPS,
)
def vehicle_latest():
    routes = spark.read.table(f"{CATALOG}.silver.routes")
    in_service = F.col("trip_id").isNotNull()
    return (
        spark.read.table(f"{CATALOG}.silver.vehicle_current")
        .join(routes, "route_id", "left")
        .withColumn("snapshot_at", F.max("position_at").over(Window.partitionBy()))
        .where("position_at >= snapshot_at - INTERVAL 10 MINUTES")
        .select(
            "vehicle_id",
            "trip_id",
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_label",
            "route_type",
            F.when(in_service, F.col("mode")).otherwise("not in service").alias("mode"),
            F.when(in_service, F.col("mode_group")).otherwise("not in service").alias("mode_group"),
            "direction_id",
            "latitude",
            "longitude",
            "bearing",
            "occupancy_status",
            "position_at",
            "position_at_local",
            "snapshot_at",
        )
    )
