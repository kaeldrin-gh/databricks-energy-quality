# Databricks notebook source
# MAGIC %md
# MAGIC # Energy prices pipeline (Lakeflow pipelines, formerly Delta Live Tables)
# MAGIC
# MAGIC Medallion: JSON files in the landing volume -> `bronze_prices` (streaming
# MAGIC table, Auto Loader) -> `silver_prices_latest` (AUTO CDC, SCD Type 1: one row per
# MAGIC region and delivery hour, the newest `fetched_at` wins) and
# MAGIC `silver_price_revisions` (AUTO CDC, SCD Type 2: every distinct published
# MAGIC price per hour) -> `gold_daily` (materialized view of daily aggregates).

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Set in resources/pipeline.yml: /Volumes/<catalog>/<schema>/landing/smard
LANDING_PATH = spark.conf.get("landing_path")

RAW_SCHEMA = (
    "region STRING, delivery_ts TIMESTAMP, price_eur_mwh DOUBLE, "
    "source STRING, fetched_at TIMESTAMP"
)
KEYS = ["region", "delivery_ts"]

# COMMAND ----------


@dp.table(
    name="bronze_prices",
    comment="Every landed SMARD batch, read incrementally from the landing volume by Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_prices():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(RAW_SCHEMA)
        .load(LANDING_PATH)
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


# COMMAND ----------


@dp.temporary_view(name="prices_clean")
@dp.expect_or_drop("keys_present", "region IS NOT NULL AND delivery_ts IS NOT NULL")
@dp.expect_or_drop("price_present", "price_eur_mwh IS NOT NULL")
@dp.expect_or_drop("fetched_at_present", "fetched_at IS NOT NULL")
def prices_clean():
    return spark.readStream.table("bronze_prices").drop("source_file")


# COMMAND ----------

dp.create_streaming_table(
    name="silver_prices_latest",
    comment="One row per (region, delivery_ts): AUTO CDC keeps the newest fetched_at (SCD Type 1).",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="silver_prices_latest",
    source="prices_clean",
    keys=KEYS,
    sequence_by="fetched_at",
    stored_as_scd_type=1,
)

# COMMAND ----------

dp.create_streaming_table(
    name="silver_price_revisions",
    comment=(
        "Every distinct published price per (region, delivery_ts), with __START_AT and "
        "__END_AT from fetched_at (SCD Type 2). A refetch of the same price adds no version."
    ),
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="silver_price_revisions",
    source="prices_clean",
    keys=KEYS,
    sequence_by="fetched_at",
    stored_as_scd_type=2,
    track_history_column_list=["price_eur_mwh"],
)

# COMMAND ----------


@dp.materialized_view(
    name="gold_daily",
    comment="Daily market aggregates from the deduplicated prices.",
    table_properties={"quality": "gold"},
)
@dp.expect("has_hours", "hours > 0")
def gold_daily():
    silver = spark.read.table("silver_prices_latest")
    return silver.groupBy("region", F.to_date("delivery_ts").alias("day")).agg(
        F.count("*").cast("int").alias("hours"),
        F.round(F.avg("price_eur_mwh"), 2).alias("avg_price_eur_mwh"),
        F.round(F.min("price_eur_mwh"), 2).alias("min_price_eur_mwh"),
        F.round(F.max("price_eur_mwh"), 2).alias("max_price_eur_mwh"),
        F.sum(F.when(F.col("price_eur_mwh") < 0, 1).otherwise(0))
        .cast("int")
        .alias("negative_hours"),
    )
