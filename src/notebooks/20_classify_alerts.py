# Databricks notebook source
# MAGIC %md
# MAGIC # 20 · Service alert classification
# MAGIC Tags each service alert version with a cause category, an impact category and a short summary, using
# MAGIC Databricks AI Functions (`ai_classify`, `ai_summarize`). Incremental: only alert versions whose text has not been
# MAGIC classified before are sent to the model. Results are in `<catalog>.gold.alert_classifications`.
# MAGIC
# MAGIC | Parameter | Default |
# MAGIC |---|---|
# MAGIC | `catalog` | `metlink` |

# COMMAND ----------

dbutils.widgets.text("catalog", "metlink")
catalog = dbutils.widgets.get("catalog")
target = f"{catalog}.gold.alert_classifications"

CAUSES = [
    "roadworks or construction",
    "crash or incident",
    "weather",
    "event",
    "staff or vehicle shortage",
    "infrastructure or vehicle fault",
    "planned timetable change",
    "other",
]
IMPACTS = ["detour", "stop closed or moved", "delays", "cancellations", "reduced service", "information only"]


def sql_array(values: list[str]) -> str:
    return "ARRAY(" + ", ".join("'" + v.replace("'", "''") + "'" for v in values) + ")"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target} (
      alert_id STRING,
      text_hash STRING,
      header_text STRING,
      gtfs_cause STRING,
      gtfs_effect STRING,
      gtfs_severity STRING,
      cause_category STRING,
      impact_category STRING,
      summary STRING,
      classified_at TIMESTAMP
    )
    COMMENT 'AI-assigned cause and impact categories and a summary for each distinct service alert text'
""")

spark.sql(f"""
    MERGE INTO {target} t
    USING (
      WITH alerts AS (
        SELECT DISTINCT alert_id, header_text, description_text, cause, effect, severity_level,
               sha2(concat_ws('|', header_text, description_text), 256) AS text_hash
        FROM {catalog}.silver.service_alerts
      ),
      new AS (
        SELECT a.* FROM alerts a LEFT ANTI JOIN {target} c USING (alert_id, text_hash)
      )
      SELECT alert_id, text_hash, header_text, cause AS gtfs_cause, effect AS gtfs_effect,
             severity_level AS gtfs_severity,
             ai_classify(concat(header_text, '. ', coalesce(description_text, '')), {sql_array(CAUSES)}) AS cause_category,
             ai_classify(concat(header_text, '. ', coalesce(description_text, '')), {sql_array(IMPACTS)}) AS impact_category,
             ai_summarize(concat(header_text, '. ', coalesce(description_text, '')), 20) AS summary,
             current_timestamp() AS classified_at
      FROM new
    ) s
    ON t.alert_id = s.alert_id AND t.text_hash = s.text_hash
    WHEN NOT MATCHED THEN INSERT *
""")

display(spark.sql(f"SELECT cause_category, impact_category, count(*) AS alerts FROM {target} GROUP BY ALL ORDER BY alerts DESC"))
