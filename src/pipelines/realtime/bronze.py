"""Bronze: incremental ingestion of GTFS-RT JSONL batches from the landing volume.

One row per poll of one feed. The nested `feed_message` is kept as landed; flattening happens in silver.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "metlink")
LANDING = f"/Volumes/{CATALOG}/bronze/raw/realtime"

FEEDS = {
    "rt_trip_updates": "tripupdates",
    "rt_vehicle_positions": "vehiclepositions",
    "rt_service_alerts": "servicealerts",
}


def define_bronze_table(table: str, feed: str) -> None:
    @dp.table(
        name=f"{CATALOG}.bronze.{table}",
        comment=f"Raw GTFS-RT {feed} polls, one row per poll",
        table_properties={"quality": "bronze"},
    )
    def _():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaHints", "header_timestamp BIGINT, entity_count INT")
            .option("cloudFiles.partitionColumns", "date")
            .load(f"{LANDING}/{feed}")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
        )


for table, feed in FEEDS.items():
    define_bronze_table(table, feed)
