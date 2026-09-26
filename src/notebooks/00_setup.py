# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Unity Catalog setup
# MAGIC Creates the catalog, the medallion schemas and the landing volume that the collectors upload into.
# MAGIC Idempotent, so it's safe to re-run.
# MAGIC
# MAGIC | Parameter | Default | Notes |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `metlink` | Use `workspace` if the workspace does not allow creating catalogs, and change `METLINK_VOLUME_PATH` to match |

# COMMAND ----------

dbutils.widgets.text("catalog", "metlink")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

if catalog != "workspace":
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'Metlink (Wellington) public transport data'")

for schema, comment in [
    ("bronze", "Raw data as landed: static GTFS CSVs and GTFS-RT polls"),
    ("silver", "Cleaned, typed, deduplicated"),
    ("gold", "Business-level aggregates for BI and ML"),
    ("ml", "Feature tables and registered models"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT '{comment}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw COMMENT 'Landing zone for files uploaded by the collectors'")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
display(dbutils.fs.ls(f"/Volumes/{catalog}/bronze/raw"))
