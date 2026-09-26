"""Gold: bus punctuality measured the way Metlink reports it, benchmarked against Metlink's published figures.

Metlink's bus punctuality is the share of trips sighted departing their first stop less than 1 min 15 s early and
less than 5 min 15 s late; the denominator is trips sighted at the first stop. Published per-route daily and weekly
files are loaded from the reference catalog's volume (uploaded by `python -m poller.official_performance`).
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "metlink")
STATIC_CATALOG = spark.conf.get("static_catalog", CATALOG)
OFFICIAL_ROOT = f"/Volumes/{STATIC_CATALOG}/bronze/raw/official/bus_performance"

EARLY_S = -75
LATE_S = 315
# Metlink peak: 06:30-09:00 and 15:00-18:00 on weekdays (public holidays are not excluded here).
PEAK_MINUTES = ((6 * 60 + 30, 9 * 60), (15 * 60, 18 * 60))
BENCHMARK_WEEKS = 8
Z = 1.96

PROPS = {"quality": "gold"}


def is_peak(ts_col: str):
    minutes = F.hour(ts_col) * 60 + F.minute(ts_col)
    in_window = F.lit(False)
    for start, end in PEAK_MINUTES:
        in_window = in_window | ((minutes >= start) & (minutes < end))
    return F.dayofweek(ts_col).between(2, 6) & in_window


def wilson(k: str, n: str, sign: int):
    """Wilson score interval bound for k successes in n trials (sign -1 = lower, +1 = upper)."""
    p = f"({k} / {n})"
    return F.expr(
        f"CASE WHEN {n} > 0 THEN ({p} + {Z}*{Z}/(2*{n}) {'+' if sign > 0 else '-'} "
        f"{Z} * sqrt({p} * (1 - {p}) / {n} + {Z}*{Z}/(4*{n}*{n}))) / (1 + {Z}*{Z}/{n}) END"
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.origin_departures",
    comment="Bus trips sighted departing their first stop, classified with Metlink's punctuality thresholds",
    cluster_by=["service_date", "route_id"],
    table_properties=PROPS,
)
def origin_departures():
    return (
        spark.read.table(f"{CATALOG}.silver.stop_arrivals")
        .where("is_origin AND is_final AND mode_group = 'bus'")
        .select(
            "service_date",
            F.expr("CAST(date_trunc('WEEK', service_date) AS DATE)").alias("week"),
            "route_id",
            "route_short_name",
            "route_label",
            "mode",
            "trip_id",
            "vehicle_id",
            F.col("scheduled_arrival_at").alias("scheduled_departure_at"),
            F.col("scheduled_arrival_at_local").alias("scheduled_departure_at_local"),
            F.col("arrival_delay_s").alias("departure_delay_s"),
            ((F.col("arrival_delay_s") > EARLY_S) & (F.col("arrival_delay_s") < LATE_S)).alias("is_punctual"),
            is_peak("scheduled_arrival_at_local").alias("is_peak"),
        )
    )


def scheduled_trips_by_route():
    """Scheduled trips per service date and route, for dates with observations, from the static calendar."""
    dates = spark.read.table(f"{CATALOG}.gold.origin_departures").select("service_date").distinct()
    calendar = spark.read.table(f"{STATIC_CATALOG}.bronze.gtfs_calendar").select(
        "service_id",
        F.to_date("start_date", "yyyyMMdd").alias("start"),
        F.to_date("end_date", "yyyyMMdd").alias("end"),
        F.array("sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday").alias("days"),
    )
    regular = (
        dates.crossJoin(calendar)
        .where("service_date BETWEEN start AND end AND element_at(days, dayofweek(service_date)) = '1'")
        .select("service_date", "service_id")
    )
    exceptions = spark.read.table(f"{STATIC_CATALOG}.bronze.gtfs_calendar_dates").select(
        "service_id", F.to_date("date", "yyyyMMdd").alias("service_date"), "exception_type"
    )
    added = exceptions.where("exception_type = '1'").join(dates, "service_date").select("service_date", "service_id")
    removed = exceptions.where("exception_type = '2'").select("service_date", "service_id")
    active = regular.unionByName(added).distinct().join(removed, ["service_date", "service_id"], "left_anti")
    trips = spark.read.table(f"{STATIC_CATALOG}.bronze.gtfs_trips").select("trip_id", "route_id", "service_id")
    routes = spark.read.table(f"{CATALOG}.silver.routes").select("route_id", "route_short_name")
    return (
        active.join(trips, "service_id")
        .join(routes, "route_id")
        .groupBy("service_date", "route_short_name")
        .agg(F.count("*").alias("scheduled_trips"))
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.route_punctuality_daily",
    comment="Bus punctuality per route and service date using Metlink's definition (comparable with its daily file)",
    cluster_by=["service_date"],
    table_properties=PROPS,
)
def route_punctuality_daily():
    punctual, peak = F.col("is_punctual").cast("int"), F.col("is_peak")
    return (
        spark.read.table(f"{CATALOG}.gold.origin_departures")
        .groupBy("service_date", "week", "route_short_name", "route_label", "mode")
        .agg(
            F.count("*").alias("punctuality_denominator"),
            F.sum(punctual).alias("punctuality_numerator"),
            F.round(F.avg("departure_delay_s"), 1).alias("mean_departure_delay_s"),
            F.sum(peak.cast("int")).alias("peak_punctuality_denominator"),
            F.sum(F.when(peak, punctual).otherwise(0)).alias("peak_punctuality_numerator"),
        )
        .join(scheduled_trips_by_route(), ["service_date", "route_short_name"], "left")
        .withColumn("punctuality", F.round(F.col("punctuality_numerator") / F.col("punctuality_denominator"), 4))
        .withColumn(
            "punctuality_contributing_percent",
            F.round(F.col("punctuality_denominator") / F.col("scheduled_trips"), 4),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.route_punctuality_weekly",
    comment="Bus punctuality per route and Monday-starting week using Metlink's definition",
    cluster_by=["week"],
    table_properties=PROPS,
)
def route_punctuality_weekly():
    return (
        spark.read.table(f"{CATALOG}.gold.route_punctuality_daily")
        .groupBy("week", "route_short_name", "route_label", "mode")
        .agg(
            F.count("*").alias("days_observed"),
            F.sum("scheduled_trips").alias("scheduled_trips"),
            F.sum("punctuality_denominator").alias("punctuality_denominator"),
            F.sum("punctuality_numerator").alias("punctuality_numerator"),
            F.sum("peak_punctuality_denominator").alias("peak_punctuality_denominator"),
            F.sum("peak_punctuality_numerator").alias("peak_punctuality_numerator"),
        )
        .withColumn("punctuality", F.round(F.col("punctuality_numerator") / F.col("punctuality_denominator"), 4))
    )


def official(kind: str, period: str):
    """Rows from the most recent published release of Metlink's daily or weekly file."""
    ints = [
        "scheduled_trips",
        "punctuality_numerator",
        "punctuality_denominator",
        "peak_punctuality_numerator",
        "peak_punctuality_denominator",
        "cancellations",
        "patronage",
    ]
    floats = [
        "reliability",
        "punctuality",
        "punctuality_contributing_percent",
        "mean_departure_time_variance",
        "cancellations_rate",
        "trips_with_some_standing_rate",
    ]
    raw = (
        spark.read.option("header", True)
        .csv(f"{OFFICIAL_ROOT}/{kind}/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    return (
        raw.withColumn("_latest", F.max("_source_file").over(Window.partitionBy()))
        .where("_source_file = _latest")
        .select(
            F.to_date(period).alias(period),
            F.col("route").alias("route_short_name"),
            *[F.expr(f"try_cast({c} AS INT)").alias(c) for c in ints],
            *[F.expr(f"try_cast({c} AS DOUBLE)").alias(c) for c in floats],
            F.regexp_extract("_source_file", r"to-(\d{4}-\d{2}-\d{2})", 1).alias("published_to"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.official_bus_performance_daily",
    comment="Metlink's published per-route daily bus performance (latest release)",
    table_properties={"quality": "silver"},
)
def official_bus_performance_daily():
    return official("daily", "day")


@dp.materialized_view(
    name=f"{CATALOG}.silver.official_bus_performance_weekly",
    comment="Metlink's published per-route weekly bus performance (latest release)",
    table_properties={"quality": "silver"},
)
def official_bus_performance_weekly():
    return official("weekly", "week")


@dp.materialized_view(
    name=f"{CATALOG}.gold.punctuality_benchmark",
    comment="Our route punctuality (Metlink definition, with 95% Wilson interval) against Metlink's published figures: "
    "the same weeks where both exist, otherwise Metlink's latest published weeks. One row per route plus 'All routes'.",
    table_properties=PROPS,
)
def punctuality_benchmark():
    ours = spark.read.table(f"{CATALOG}.gold.route_punctuality_weekly")
    published = spark.read.table(f"{CATALOG}.silver.official_bus_performance_weekly").where(
        "punctuality_denominator > 0"
    )

    recent_weeks = published.select("week").distinct().orderBy(F.col("week").desc()).limit(BENCHMARK_WEEKS)
    recent = published.join(recent_weeks, "week")
    same = published.join(ours.select("week", "route_short_name"), ["week", "route_short_name"])

    def by_route(df, prefix):
        with_total = df.unionByName(df.withColumn("route_short_name", F.lit("All routes")))
        return with_total.groupBy("route_short_name").agg(
            F.count("*").alias(f"{prefix}_weeks"),
            F.min("week").alias(f"{prefix}_from"),
            F.max("week").alias(f"{prefix}_to"),
            (F.sum("punctuality_numerator") / F.sum("punctuality_denominator")).alias(f"{prefix}_punctuality"),
            F.sum("punctuality_denominator").alias(f"{prefix}_trips"),
        )

    ours_by_route = (
        ours.unionByName(
            ours.withColumn("route_short_name", F.lit("All routes")).withColumn("route_label", F.lit(None))
        )
        .groupBy("route_short_name")
        .agg(
            F.first("route_label", ignorenulls=True).alias("route_label"),
            F.countDistinct("week").alias("our_weeks"),
            F.sum("punctuality_denominator").alias("our_trips"),
            F.sum("punctuality_numerator").alias("our_punctual"),
        )
        .withColumn("our_punctuality", F.col("our_punctual") / F.col("our_trips"))
        .withColumn("our_punctuality_low", wilson("our_punctual", "our_trips", -1))
        .withColumn("our_punctuality_high", wilson("our_punctual", "our_trips", +1))
    )
    result = ours_by_route.join(by_route(same, "official_same_period"), "route_short_name", "left").join(
        by_route(recent, "official_recent"), "route_short_name", "left"
    )
    reference = F.coalesce(F.col("official_same_period_punctuality"), F.col("official_recent_punctuality"))
    return (
        result.withColumn(
            "comparison_basis",
            F.when(F.col("official_same_period_punctuality").isNotNull(), "same weeks")
            .when(F.col("official_recent_punctuality").isNotNull(), "latest published weeks")
            .otherwise("no published figure"),
        )
        .withColumn("difference_pp", F.round((F.col("our_punctuality") - reference) * 100, 1))
        .withColumn(
            "reference_within_interval",
            reference.between(F.col("our_punctuality_low"), F.col("our_punctuality_high")),
        )
        .select(
            "route_short_name",
            "route_label",
            "comparison_basis",
            "our_weeks",
            "our_trips",
            F.round("our_punctuality", 4).alias("our_punctuality"),
            F.round("our_punctuality_low", 4).alias("our_punctuality_low"),
            F.round("our_punctuality_high", 4).alias("our_punctuality_high"),
            F.round("official_same_period_punctuality", 4).alias("official_same_period_punctuality"),
            "official_same_period_weeks",
            F.round("official_recent_punctuality", 4).alias("official_recent_punctuality"),
            "official_recent_from",
            "official_recent_to",
            "difference_pp",
            "reference_within_interval",
        )
    )
