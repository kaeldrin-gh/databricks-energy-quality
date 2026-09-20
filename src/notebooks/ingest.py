# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest published SMARD day-ahead prices into bronze
# MAGIC
# MAGIC Append-only landing; revisions are deduplicated in the pipeline's
# MAGIC `silver_prices` table (newest `fetched_at` wins), so re-running this task
# MAGIC is always safe.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "energy_quality")
dbutils.widgets.text("weeks", "3")

# COMMAND ----------

from energy_quality.ingest import run_ingest  # noqa: E402

count = run_ingest(
    spark,
    dbutils.widgets.get("catalog"),
    dbutils.widgets.get("schema"),
    weeks=int(dbutils.widgets.get("weeks")),
)
print(f"ingested {count} published hours into bronze_prices")
