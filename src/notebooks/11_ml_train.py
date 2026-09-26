# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Arrival delay model: training
# MAGIC Predicts the arrival delay at a stop 1–15 stops ahead of a trip's current position.
# MAGIC
# MAGIC - **Examples:** pairs of (current stop, later stop) on the same trip, from final arrivals in `silver.stop_arrivals`.
# MAGIC - **Features:** current delay, stops and scheduled minutes ahead, time of day, weekday, mode, and point-in-time
# MAGIC   route delay from `ml.route_delay_15min`.
# MAGIC - **Baseline:** the current delay persists (the delay reported for the next stop carried forward).
# MAGIC - **Split:** by service date. The most recent `test_days` are held out.
# MAGIC
# MAGIC The model is registered as `<catalog>.ml.arrival_delay`. It is given the `champion` alias when it beats the
# MAGIC baseline and the current champion on held-out MAE.
# MAGIC
# MAGIC | Parameter | Default |
# MAGIC |---|---|
# MAGIC | `catalog` | `metlink` |
# MAGIC | `test_days` | `4` |
# MAGIC | `max_training_rows` | `400000` |

# COMMAND ----------

# MAGIC %pip install -q databricks-feature-engineering mlflow scikit-learn
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "metlink")
dbutils.widgets.text("test_days", "4")
dbutils.widgets.text("max_training_rows", "400000")
catalog = dbutils.widgets.get("catalog")
test_days = int(dbutils.widgets.get("test_days"))
max_rows = int(dbutils.widgets.get("max_training_rows"))

model_name = f"{catalog}.ml.arrival_delay"
feature_table = f"{catalog}.ml.route_delay_15min"
MAX_STOPS_AHEAD = 15
OBSERVATION_SAMPLE = 4  # use one in four stops as an observation point
MODES = ["bus", "school bus", "rail", "ferry", "cable car"]

# COMMAND ----------

from pyspark.sql import functions as F

arrivals = (
    spark.table(f"{catalog}.silver.stop_arrivals")
    .where("is_final")
    .select("trip_id", "service_date", "route_id", "mode", "stop_sequence", "arrival_delay_s", "arrival_at",
            "scheduled_arrival_at", "scheduled_arrival_at_local")
)
observations = arrivals.where(F.pmod(F.xxhash64("trip_id", "stop_sequence"), OBSERVATION_SAMPLE) == 0)
targets = arrivals.select(
    "trip_id", "service_date",
    F.col("stop_sequence").alias("target_stop_sequence"),
    F.col("arrival_delay_s").alias("target_delay_s"),
    F.col("scheduled_arrival_at").alias("target_scheduled_at"),
)
mode_code = F.array_position(F.array(*[F.lit(m) for m in MODES]), F.col("mode")).cast("int")

examples = (
    observations.join(targets, ["trip_id", "service_date"])
    .where(F.col("target_stop_sequence") > F.col("stop_sequence"))
    .where(F.col("target_stop_sequence") - F.col("stop_sequence") <= MAX_STOPS_AHEAD)
    .select(
        "trip_id",
        "service_date",
        "route_id",
        F.col("arrival_at").alias("observed_at"),
        F.col("arrival_delay_s").cast("double").alias("current_delay_s"),
        (F.col("target_stop_sequence") - F.col("stop_sequence")).cast("int").alias("stops_ahead"),
        ((F.unix_timestamp("target_scheduled_at") - F.unix_timestamp("scheduled_arrival_at")) / 60)
        .cast("double").alias("scheduled_minutes_ahead"),
        F.hour("scheduled_arrival_at_local").alias("hour"),
        F.dayofweek("scheduled_arrival_at_local").alias("day_of_week"),
        mode_code.alias("mode_code"),
        F.col("target_delay_s").cast("double").alias("target_delay_s"),
    )
)

dates = sorted(r.service_date for r in examples.select("service_date").distinct().collect())
if len(dates) <= test_days:
    dbutils.notebook.exit(f"Only {len(dates)} service dates available; need more than test_days={test_days}")
cutoff = dates[-test_days]
print(f"{len(dates)} service dates; training before {cutoff}, testing from {cutoff}")

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()
training_set = fe.create_training_set(
    df=examples,
    feature_lookups=[
        FeatureLookup(
            table_name=feature_table,
            lookup_key="route_id",
            timestamp_lookup_key="observed_at",
            feature_names=["route_mean_delay_s", "route_p90_delay_s", "route_late_share", "route_arrivals"],
        )
    ],
    label="target_delay_s",
    exclude_columns=["trip_id", "route_id", "observed_at"],
)
data = training_set.load_df()

train_df = data.where(F.col("service_date") < cutoff).drop("service_date")
test_df = data.where(F.col("service_date") >= cutoff).drop("service_date")
fraction = min(1.0, max_rows / max(train_df.count(), 1))
train = train_df.sample(fraction=fraction, seed=42).toPandas()
test = test_df.sample(fraction=min(1.0, max_rows / 4 / max(test_df.count(), 1)), seed=42).toPandas()
print(f"train {len(train):,} rows, test {len(test):,} rows")

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

FEATURES = ["current_delay_s", "stops_ahead", "scheduled_minutes_ahead", "hour", "day_of_week", "mode_code",
            "route_mean_delay_s", "route_p90_delay_s", "route_late_share", "route_arrivals"]
LABEL = "target_delay_s"

user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/metlink-arrival-delay")
mlflow.set_registry_uri("databricks-uc")

X_train, y_train = train[FEATURES], train[LABEL]
X_test, y_test = test[FEATURES], test[LABEL]

with mlflow.start_run(run_name=f"hgb-{catalog}") as run:
    params = {"max_iter": 300, "learning_rate": 0.08, "max_leaf_nodes": 63, "min_samples_leaf": 50,
              "l2_regularization": 1.0, "categorical_features": ["mode_code"]}
    model = HistGradientBoostingRegressor(**params, random_state=42).fit(X_train, y_train)

    pred = model.predict(X_test)
    baseline = X_test["current_delay_s"].to_numpy()
    mae, baseline_mae = mean_absolute_error(y_test, pred), mean_absolute_error(y_test, baseline)
    by_horizon = (
        pd.DataFrame({"stops_ahead": X_test["stops_ahead"], "model": np.abs(y_test - pred),
                      "baseline": np.abs(y_test - baseline)})
        .groupby("stops_ahead").mean().round(1)
    )
    mlflow.log_params({**{k: v for k, v in params.items() if k != "categorical_features"},
                       "catalog": catalog, "test_from": str(cutoff), "train_rows": len(train)})
    mlflow.log_metrics({"test_mae_s": mae, "baseline_mae_s": baseline_mae,
                        "mae_improvement_pct": 100 * (1 - mae / baseline_mae)})
    mlflow.log_table(by_horizon.reset_index(), "mae_by_stops_ahead.json")

    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=model_name,
        infer_input_example=True,
    )

print(f"test MAE {mae:.1f}s vs baseline {baseline_mae:.1f}s ({100 * (1 - mae / baseline_mae):.1f}% better)")
display(by_horizon.reset_index())

# COMMAND ----------

client = MlflowClient(registry_uri="databricks-uc")
version = max(int(v.version) for v in client.search_model_versions(f"name='{model_name}'")
              if v.run_id == run.info.run_id)

try:
    champion = client.get_model_version_by_alias(model_name, "champion")
    champion_mae = client.get_run(champion.run_id).data.metrics.get("test_mae_s", float("inf"))
except Exception:
    champion_mae = float("inf")

if mae < baseline_mae and mae <= champion_mae:
    client.set_registered_model_alias(model_name, "champion", version)
    print(f"version {version} is now @champion")
else:
    print(f"version {version} not promoted (MAE {mae:.1f}s; baseline {baseline_mae:.1f}s; champion {champion_mae:.1f}s)")
