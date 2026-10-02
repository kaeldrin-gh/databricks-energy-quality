# Databricks notebook source
# MAGIC %md
# MAGIC # Land published SMARD day-ahead prices in the landing volume
# MAGIC
# MAGIC Writes one JSON-lines file per run to
# MAGIC `/Volumes/<catalog>/<schema>/landing/smard/`. The pipeline ingests new
# MAGIC files with Auto Loader into `bronze_prices`, and AUTO CDC keeps the newest
# MAGIC revision per hour in `silver_prices_latest`, so re-running this task is always safe.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "energy_quality")
dbutils.widgets.text("weeks", "3")

# COMMAND ----------

from energy_quality.ingest import run_ingest  # noqa: E402

path = run_ingest(
    dbutils.widgets.get("catalog"),
    dbutils.widgets.get("schema"),
    weeks=int(dbutils.widgets.get("weeks")),
)
print(f"landed {path}")
