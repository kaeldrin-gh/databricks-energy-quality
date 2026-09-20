# Databricks notebook source
# MAGIC %md
# MAGIC # Energy prices pipeline (Lakeflow Spark Declarative Pipelines)
# MAGIC
# MAGIC `bronze_prices` (landed by the ingest task) -> `silver_prices` (one row
# MAGIC per region and delivery hour, the newest `fetched_at` wins) -> `gold_daily`
# MAGIC (daily aggregates). Expectations drop rows without keys or prices and warn
# MAGIC on empty days.

# COMMAND ----------

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------


@dlt.table(
    name="silver_prices",
    comment="One row per (region, delivery_ts); the newest fetched_at wins.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("keys_present", "region IS NOT NULL AND delivery_ts IS NOT NULL")
@dlt.expect_or_drop("price_present", "price_eur_mwh IS NOT NULL")
def silver_prices():
    ranked = dlt.read("bronze_prices").withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("region", "delivery_ts").orderBy(F.col("fetched_at").desc())
        ),
    )
    return ranked.where("rn = 1").drop("rn")


# COMMAND ----------


@dlt.table(
    name="gold_daily",
    comment="Daily market aggregates from the deduplicated prices.",
    table_properties={"quality": "gold"},
)
@dlt.expect("has_hours", "hours > 0")
def gold_daily():
    silver = dlt.read("silver_prices")
    return silver.groupBy("region", F.to_date("delivery_ts").alias("day")).agg(
        F.count("*").cast("int").alias("hours"),
        F.round(F.avg("price_eur_mwh"), 2).alias("avg_price_eur_mwh"),
        F.round(F.min("price_eur_mwh"), 2).alias("min_price_eur_mwh"),
        F.round(F.max("price_eur_mwh"), 2).alias("max_price_eur_mwh"),
        F.sum(F.when(F.col("price_eur_mwh") < 0, 1).otherwise(0))
        .cast("int")
        .alias("negative_hours"),
    )
