# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Load static GTFS to bronze
# MAGIC Loads the GTFS CSVs uploaded by `python -m poller.static_gtfs` from
# MAGIC `/Volumes/<catalog>/bronze/raw/gtfs_static/<version>/`, writing one Delta table per file (`bronze.gtfs_<file>`).
# MAGIC
# MAGIC - Every column stays a string, exactly as published. Types are applied in silver.
# MAGIC - Each run overwrites the tables. Earlier feed versions stay available through Delta table history.
# MAGIC
# MAGIC | Parameter | Default | Notes |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `metlink` | |
# MAGIC | `version` | latest | Feed version folder (`YYYY-MM-DD`) |

# COMMAND ----------

dbutils.widgets.text("catalog", "metlink")
dbutils.widgets.text("version", "", "version (blank = latest)")
catalog = dbutils.widgets.get("catalog")
static_root = f"/Volumes/{catalog}/bronze/raw/gtfs_static"

versions = sorted(f.name.rstrip("/") for f in dbutils.fs.ls(static_root))
version = dbutils.widgets.get("version") or versions[-1]
source_dir = f"{static_root}/{version}"
print(f"available versions: {versions}\nloading: {source_dir}")

# COMMAND ----------

from pyspark.sql import functions as F

files = [f for f in dbutils.fs.ls(source_dir) if f.name.endswith(".txt")]

for f in files:
    table = f"{catalog}.bronze.gtfs_{f.name.removesuffix('.txt')}"
    df = (
        spark.read.option("header", True).option("inferSchema", False).csv(f.path)
        .withColumn("_feed_version", F.lit(version))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(table)
    print(f"{table:45} {spark.table(table).count():>10,} rows")

# COMMAND ----------

# stop_times is the largest table and is almost always joined on trip_id.
spark.sql(f"ALTER TABLE {catalog}.bronze.gtfs_stop_times CLUSTER BY (trip_id)")
display(spark.sql(f"OPTIMIZE {catalog}.bronze.gtfs_stop_times"))
