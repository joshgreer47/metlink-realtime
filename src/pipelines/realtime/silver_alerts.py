"""Silver: service alert versions and the entities each active alert affects."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "metlink")


def epoch_to_ts(col):
    return F.timestamp_seconds(col.cast("bigint"))


def translated_text(field: str):
    """English translation of a GTFS-RT TranslatedString, falling back to the first one."""
    return F.expr(
        f"coalesce(try_element_at(filter({field}.translation, t -> t.language = 'en'), 1).text, "
        f"try_element_at({field}.translation, 1).text)"
    )


TRACKED = [
    "cause",
    "effect",
    "severity_level",
    "header_text",
    "description_text",
    "url",
    "active_start_at",
    "active_end_at",
    "informed_entities",
]


@dp.temporary_view(comment="Flattened service alerts, one row per alert per poll")
@dp.expect_or_drop("has_alert_id", "alert_id IS NOT NULL")
def service_alert_polls():
    return (
        spark.readStream.table(f"{CATALOG}.bronze.rt_service_alerts")
        .select(
            F.col("fetched_at").cast("timestamp").alias("fetched_at"),
            F.timestamp_seconds("header_timestamp").alias("feed_at"),
            F.explode("feed_message.entity").alias("e"),
        )
        .select(
            F.col("e.id").alias("alert_id"),
            F.col("e.alert.cause").alias("cause"),
            F.col("e.alert.effect").alias("effect"),
            F.col("e.alert.severity_level").alias("severity_level"),
            translated_text("e.alert.header_text").alias("header_text"),
            translated_text("e.alert.description_text").alias("description_text"),
            translated_text("e.alert.url").alias("url"),
            epoch_to_ts(F.expr("try_element_at(e.alert.active_period, 1).start")).alias("active_start_at"),
            epoch_to_ts(F.expr("try_element_at(e.alert.active_period, 1).end")).alias("active_end_at"),
            F.col("e.alert.informed_entity").alias("informed_entities"),
            F.col("feed_at"),
            F.col("fetched_at").alias("last_seen_at"),
        )
    )


dp.create_streaming_table(
    name=f"{CATALOG}.silver.service_alerts",
    comment="Service alert versions (SCD type 2). last_seen_at is the last poll that contained the alert.",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.service_alerts",
    source="service_alert_polls",
    keys=["alert_id"],
    sequence_by=F.col("feed_at"),
    stored_as_scd_type=2,
    track_history_column_list=TRACKED,
)


@dp.materialized_view(
    name=f"{CATALOG}.silver.service_alert_entities",
    comment="Routes, stops and trips affected by the current version of each alert",
    table_properties={"quality": "silver"},
)
def service_alert_entities():
    return (
        spark.read.table(f"{CATALOG}.silver.service_alerts")
        .where("__END_AT IS NULL")
        .select(
            "alert_id",
            "effect",
            "severity_level",
            "active_start_at",
            "active_end_at",
            "last_seen_at",
            F.explode("informed_entities").alias("ie"),
        )
        .select(
            "*",
            F.col("ie.route_id").alias("route_id"),
            F.col("ie.route_type").cast("int").alias("route_type"),
            F.col("ie.stop_id").alias("stop_id"),
            F.col("ie.trip.trip_id").alias("trip_id"),
            F.col("ie.trip.direction_id").cast("int").alias("direction_id"),
            F.to_date("ie.trip.start_date", "yyyyMMdd").alias("service_date"),
        )
        .drop("ie")
    )
