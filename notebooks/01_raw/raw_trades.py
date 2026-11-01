# Databricks notebook source
# MAGIC %md
# MAGIC # Raw: Binance trades (Auto Loader: JSON Lines to Delta)

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath(".."))   # makes notebooks/utils importable

from pyspark.sql import functions as F
from utils.schemas import TRADE_FIELDS

CATALOG = "dev"
SOURCE_PATH = f"/Volumes/{CATALOG}/raw/landing/trades"
CHECKPOINT = f"/Volumes/{CATALOG}/raw/checkpoints/raw_trades"
TARGET_TABLE = f"{CATALOG}.raw.binance_trades"

# COMMAND ----------

# Each line of each file is one raw JSON event, read as text.
raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .load(SOURCE_PATH)
)

# Parse into a string map so keys like "e"/"E" and "t"/"T" stay distinct.
parsed = F.from_json("value", "map<string,string>")

df = raw.select(
    *[parsed[code].alias(name) for code, name in TRADE_FIELDS.items()],
    F.col("value").alias("raw_payload"),
    F.current_timestamp().alias("ingestion_time"),
    F.current_date().alias("ingestion_date"),
    F.col("_metadata.file_path").alias("source_file"),
)

# COMMAND ----------

query = (
    df.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .partitionBy("ingestion_date")
    .trigger(availableNow=True)      # process everything new, then stop
    .toTable(TARGET_TABLE)
)
query.awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT symbol, COUNT(*) AS events FROM {TARGET_TABLE} GROUP BY symbol"))