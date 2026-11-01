# Databricks notebook source
# MAGIC %md
# MAGIC # Raw: Binance klines (Auto Loader: JSON Lines to Delta)

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath(".."))

from pyspark.sql import functions as F
from utils.schemas import KLINE_FIELDS, KLINE_K_FIELDS

CATALOG = "dev"
SOURCE_PATH = f"/Volumes/{CATALOG}/raw/landing/klines"
CHECKPOINT = f"/Volumes/{CATALOG}/raw/checkpoints/raw_klines"
TARGET_TABLE = f"{CATALOG}.raw.binance_klines"

# COMMAND ----------

raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .load(SOURCE_PATH)
)

top = F.from_json("value", "map<string,string>")
k = F.from_json(top["k"], "map<string,string>")   # nested candle object

df = raw.select(
    *[top[code].alias(name) for code, name in KLINE_FIELDS.items()],
    *[k[code].alias(name) for code, name in KLINE_K_FIELDS.items()],
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
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)
query.awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT symbol, is_closed, COUNT(*) AS events FROM {TARGET_TABLE} GROUP BY symbol, is_closed"))