# Databricks notebook source
# MAGIC %md
# MAGIC # Quality report
# MAGIC
# MAGIC Runs the freshness, price-bounds, uniqueness and DST-aware daily checks
# MAGIC against the marts, appends them to `quality_report`, and fails the task
# MAGIC when any check fails so the workspace alert fires.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "energy_quality")

# COMMAND ----------

from energy_quality.report import run_quality_report  # noqa: E402

results = run_quality_report(spark, dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
for result in results:
    print(f"{result.status.upper():4} {result.name}: {result.detail}")
