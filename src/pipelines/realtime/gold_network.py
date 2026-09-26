"""Gold: where the network loses time. Road speeds by H3 cell, and timetabled vs actual segment running times.

Speeds are measured between consecutive position reports from the same in-service vehicle, so they include time
spent at stops and traffic signals; compare cells by hour rather than reading them as free-flow speeds.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "metlink")
H3_RESOLUTION = 9  # ~0.1 km2 cells
MIN_GAP_S, MAX_GAP_S = 10, 180
MAX_SPEED_KMH = 110
PEAK_HOURS = (7, 8)  # weekday morning peak, compared against midday
MIDDAY_HOURS = (10, 11, 12, 13, 14)
MIN_SAMPLES = 30
MIN_MIDDAY_SPEED_KMH = 10  # exclude cells dominated by vehicles waiting at termini
SHORTFALL_S = 60  # median excess running time that counts as a consistent timetable shortfall
MIN_TRIPS = 20

PROPS = {"quality": "gold"}


def day_type(ts):
    dow = F.dayofweek(ts)
    return F.when(dow == 1, "sunday").when(dow == 7, "saturday").otherwise("weekday")


def haversine_m(lat1: str, lon1: str, lat2: str, lon2: str):
    """Great-circle distance in metres between two points given as column names."""
    lat1, lon1, lat2, lon2 = (F.col(c) for c in (lat1, lon1, lat2, lon2))
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * 6371000 * F.asin(F.sqrt(a))


@dp.materialized_view(
    name=f"{CATALOG}.gold.road_speeds_h3",
    comment="Median in-service vehicle speed per H3 cell (resolution 9), mode group, day type and local hour",
    cluster_by=["h3_cell"],
    table_properties=PROPS,
)
def road_speeds_h3():
    w = Window.partitionBy("vehicle_id").orderBy("position_at")
    routes = spark.read.table(f"{CATALOG}.silver.routes").select("route_id", "mode_group")
    steps = (
        spark.read.table(f"{CATALOG}.silver.vehicle_positions")
        .where("trip_id IS NOT NULL")
        .select("vehicle_id", "trip_id", "route_id", "latitude", "longitude", "position_at", "position_at_local")
        .withColumn("prev_lat", F.lag("latitude").over(w))
        .withColumn("prev_lon", F.lag("longitude").over(w))
        .withColumn("prev_at", F.lag("position_at").over(w))
        .withColumn("prev_trip", F.lag("trip_id").over(w))
        .where("prev_trip = trip_id")
        .withColumn("gap_s", F.unix_timestamp("position_at") - F.unix_timestamp("prev_at"))
        .where(F.col("gap_s").between(MIN_GAP_S, MAX_GAP_S))
        .withColumn("speed_kmh", haversine_m("prev_lat", "prev_lon", "latitude", "longitude") / F.col("gap_s") * 3.6)
        .where(F.col("speed_kmh") <= MAX_SPEED_KMH)
        .withColumn("mid_lat", (F.col("latitude") + F.col("prev_lat")) / 2)
        .withColumn("mid_lon", (F.col("longitude") + F.col("prev_lon")) / 2)
        .withColumn("h3_cell", F.expr(f"h3_longlatash3(mid_lon, mid_lat, {H3_RESOLUTION})"))
        .join(routes, "route_id", "left")
    )
    return steps.groupBy(
        "h3_cell",
        "mode_group",
        day_type("position_at_local").alias("day_type"),
        F.hour("position_at_local").alias("hour"),
    ).agg(
        F.count("*").alias("samples"),
        F.round(F.percentile_approx("speed_kmh", 0.5), 1).alias("median_speed_kmh"),
        F.round(F.avg("mid_lat"), 6).alias("latitude"),
        F.round(F.avg("mid_lon"), 6).alias("longitude"),
        F.countDistinct("route_id").alias("routes"),
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.congestion_hotspots",
    comment="Bus speed in each H3 cell in the weekday morning peak (07:00-09:00) against weekday midday (10:00-15:00)",
    cluster_by=["h3_cell"],
    table_properties=PROPS,
)
def congestion_hotspots():
    speeds = spark.read.table(f"{CATALOG}.gold.road_speeds_h3").where("day_type = 'weekday' AND mode_group = 'bus'")

    def band(hours, prefix):
        return (
            speeds.where(F.col("hour").isin(*hours))
            .groupBy("h3_cell")
            .agg(
                F.sum("samples").alias(f"{prefix}_samples"),
                F.round(F.sum(F.col("median_speed_kmh") * F.col("samples")) / F.sum("samples"), 1).alias(
                    f"{prefix}_speed_kmh"
                ),
                F.avg("latitude").alias(f"{prefix}_lat"),
                F.avg("longitude").alias(f"{prefix}_lon"),
                F.max("routes").alias(f"{prefix}_routes"),
            )
        )

    return (
        band(PEAK_HOURS, "peak")
        .join(band(MIDDAY_HOURS, "midday"), "h3_cell")
        .where(F.col("peak_samples") >= MIN_SAMPLES)
        .where(F.col("midday_samples") >= MIN_SAMPLES)
        .where(F.col("midday_speed_kmh") >= MIN_MIDDAY_SPEED_KMH)
        .select(
            "h3_cell",
            F.round("peak_lat", 6).alias("latitude"),
            F.round("peak_lon", 6).alias("longitude"),
            F.greatest("peak_routes", "midday_routes").alias("routes"),
            "peak_samples",
            "midday_samples",
            "peak_speed_kmh",
            "midday_speed_kmh",
            F.round((1 - F.col("peak_speed_kmh") / F.col("midday_speed_kmh")) * 100, 1).alias("peak_slowdown_pct"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.segment_runtimes",
    comment="Actual vs scheduled running time between consecutive observed stops, per route, direction, day type and "
    "scheduled hour. excess_s = actual - scheduled; positive means slower than the timetable.",
    cluster_by=["route_id"],
    table_properties=PROPS,
)
def segment_runtimes():
    w = Window.partitionBy("trip_id", "service_date").orderBy("stop_sequence")
    return (
        spark.read.table(f"{CATALOG}.silver.stop_arrivals")
        .where("is_final")
        .select(
            "trip_id",
            "service_date",
            "route_id",
            "route_label",
            "mode_group",
            "direction_id",
            "stop_sequence",
            "stop_id",
            "stop_name",
            "arrival_at",
            "scheduled_arrival_at",
            "scheduled_arrival_at_local",
        )
        .withColumn("from_stop_sequence", F.lag("stop_sequence").over(w))
        .withColumn("from_stop_id", F.lag("stop_id").over(w))
        .withColumn("from_stop_name", F.lag("stop_name").over(w))
        .withColumn("from_arrival_at", F.lag("arrival_at").over(w))
        .withColumn("from_scheduled_at", F.lag("scheduled_arrival_at").over(w))
        .withColumn("from_scheduled_local", F.lag("scheduled_arrival_at_local").over(w))
        .where("from_stop_id IS NOT NULL")
        .withColumn("scheduled_s", F.unix_timestamp("scheduled_arrival_at") - F.unix_timestamp("from_scheduled_at"))
        .withColumn("actual_s", F.unix_timestamp("arrival_at") - F.unix_timestamp("from_arrival_at"))
        .where("scheduled_s > 0")
        .groupBy(
            "route_id",
            "route_label",
            "mode_group",
            "direction_id",
            "from_stop_id",
            "from_stop_name",
            F.col("stop_id").alias("to_stop_id"),
            F.col("stop_name").alias("to_stop_name"),
            (F.col("stop_sequence") - F.col("from_stop_sequence")).alias("stops_spanned"),
            day_type("from_scheduled_local").alias("day_type"),
            F.hour("from_scheduled_local").alias("hour"),
        )
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("scheduled_s"), 1).alias("scheduled_s"),
            F.percentile_approx("actual_s", 0.5).alias("median_actual_s"),
            F.percentile_approx(F.col("actual_s") - F.col("scheduled_s"), 0.5).alias("median_excess_s"),
            F.percentile_approx(F.col("actual_s") - F.col("scheduled_s"), 0.85).alias("p85_excess_s"),
            F.round(F.avg((F.col("actual_s") - F.col("scheduled_s") > SHORTFALL_S).cast("double")), 3).alias(
                "share_over_60s"
            ),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.timetable_shortfalls",
    comment=f"Segments where the median trip takes at least {SHORTFALL_S} s longer than timetabled, from at least "
    f"{MIN_TRIPS} trips. suggested_extra_s is the median excess.",
    table_properties=PROPS,
)
def timetable_shortfalls():
    return (
        spark.read.table(f"{CATALOG}.gold.segment_runtimes")
        .where(F.col("trips") >= MIN_TRIPS)
        .where(F.col("median_excess_s") >= SHORTFALL_S)
        .withColumn("suggested_extra_s", F.col("median_excess_s"))
        .withColumn("excess_trip_minutes", F.round(F.col("median_excess_s") * F.col("trips") / 60, 1))
    )
