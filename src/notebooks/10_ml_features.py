# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Arrival delay model: feature table
# MAGIC Builds `<catalog>.ml.route_delay_15min`, a time-series feature table of recent delay per route in 15-minute
# MAGIC windows. Training and scoring join it point-in-time (the latest window that ended at or before the observation),
# MAGIC so a prediction only uses information available when it would have been made.
# MAGIC
# MAGIC | Parameter | Default |
# MAGIC |---|---|
# MAGIC | `catalog` | `metlink` |

# COMMAND ----------

# MAGIC %pip install -q databricks-feature-engineering
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "metlink")
catalog = dbutils.widgets.get("catalog")
table = f"{catalog}.ml.route_delay_15min"

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()

features = (
    spark.table(f"{catalog}.silver.stop_arrivals")
    .where("is_final")
    .groupBy("route_id", F.window("arrival_at", "15 minutes"))
    .agg(
        F.avg("arrival_delay_s").alias("route_mean_delay_s"),
        F.expr("percentile_approx(arrival_delay_s, 0.9)").alias("route_p90_delay_s"),
        F.avg((F.col("arrival_delay_s") >= 300).cast("double")).alias("route_late_share"),
        F.count("*").alias("route_arrivals"),
    )
    .select("route_id", F.col("window.end").alias("window_end"), "route_mean_delay_s", "route_p90_delay_s",
            "route_late_share", "route_arrivals")
)

if spark.catalog.tableExists(table):
    fe.write_table(name=table, df=features, mode="merge")
else:
    fe.create_table(
        name=table,
        primary_keys=["route_id", "window_end"],
        timeseries_columns=["window_end"],
        df=features,
        description="Delay of each route's final stop arrivals in 15-minute windows, keyed by window end",
    )
print(f"{table}: {spark.table(table).count():,} rows")
