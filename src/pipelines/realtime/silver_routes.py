"""Silver: route dimension with readable mode and display labels, derived from the static timetable."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "metlink")
STATIC_CATALOG = spark.conf.get("static_catalog", CATALOG)
LABEL_MAX = 40

# GTFS route_type (including extended types used by Metlink) to a readable mode.
MODE = """CASE route_type WHEN 2 THEN 'rail' WHEN 100 THEN 'rail' WHEN 3 THEN 'bus' WHEN 700 THEN 'bus'
    WHEN 712 THEN 'school bus' WHEN 4 THEN 'ferry' WHEN 5 THEN 'cable car' ELSE 'other' END"""


@dp.materialized_view(
    name=f"{CATALOG}.silver.routes",
    comment="One row per route: mode, mode group (bus, rail, ferry & cable car) and a display label",
    table_properties={"quality": "silver"},
)
def routes():
    long_name = F.col("route_long_name")
    truncated = F.when(F.length(long_name) > LABEL_MAX, F.concat(F.substring(long_name, 1, LABEL_MAX - 1), F.lit("…")))
    return (
        spark.read.table(f"{STATIC_CATALOG}.bronze.gtfs_routes")
        .select(
            "route_id",
            "route_short_name",
            "route_long_name",
            F.col("route_type").cast("int").alias("route_type"),
        )
        .withColumn("mode", F.expr(MODE))
        .withColumn(
            "mode_group",
            F.when(F.col("mode").isin("bus", "school bus"), "bus")
            .when(F.col("mode") == "rail", "rail")
            .otherwise("ferry & cable car"),
        )
        .withColumn(
            "route_label",
            # Rail lines and the cable car are known by name ("Kāpiti Line"); other routes by number and description.
            F.when(F.col("mode").isin("rail", "cable car"), F.regexp_replace(long_name, r"\s*\(.*\)$", "")).otherwise(
                F.concat_ws(" · ", "route_short_name", F.coalesce(truncated, long_name))
            ),
        )
    )
