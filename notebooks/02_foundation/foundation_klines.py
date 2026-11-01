# Databricks notebook source
# MAGIC %md
# MAGIC # Foundation: klines (typed, validated, latest version per candle)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = "dev"
SOURCE_TABLE = f"{CATALOG}.raw.binance_klines"
TARGET_TABLE = f"{CATALOG}.foundation.crypto_klines"
REJECTED_TABLE = f"{CATALOG}.foundation.crypto_klines_rejected"
CHECKPOINT = f"/Volumes/{CATALOG}/foundation/checkpoints/foundation_klines"

def ms_to_ts(col_name):
    return F.expr(f"timestamp_millis(cast({col_name} as bigint))")

KEY = ["symbol", "kline_interval", "kline_start_timestamp"]

# COMMAND ----------

def process_batch(batch_df, batch_id):
    typed = batch_df.select(
        F.col("symbol"),
        F.col("interval").alias("kline_interval"),
        ms_to_ts("kline_start_ms").alias("kline_start_timestamp"),
        ms_to_ts("kline_close_ms").alias("kline_close_timestamp"),
        F.col("open_price").cast("decimal(20,8)").alias("open_price"),
        F.col("high_price").cast("decimal(20,8)").alias("high_price"),
        F.col("low_price").cast("decimal(20,8)").alias("low_price"),
        F.col("close_price").cast("decimal(20,8)").alias("close_price"),
        F.col("volume").cast("decimal(30,12)").alias("volume"),
        F.col("quote_volume").cast("decimal(30,12)").alias("quote_volume"),
        F.col("number_of_trades").cast("bigint").alias("trade_count"),
        F.col("taker_buy_base_volume").cast("decimal(30,12)").alias("taker_buy_volume"),
        F.col("is_closed").cast("boolean").alias("is_closed"),
        ms_to_ts("event_time_ms").alias("event_timestamp"),
        F.col("ingestion_time").alias("ingestion_timestamp"),
        F.col("raw_payload"),
    )

    is_valid = F.coalesce(
        F.col("symbol").isNotNull()
        & F.col("kline_interval").isNotNull()
        & F.col("kline_start_timestamp").isNotNull()
        & (F.col("open_price") > 0) & (F.col("close_price") > 0)
        & (F.col("high_price") >= F.col("low_price"))
        & (F.col("volume") >= 0),
        F.lit(False),
    )

    (typed.filter(~is_valid)
        .withColumn("rejected_at", F.current_timestamp())
        .write.mode("append").saveAsTable(REJECTED_TABLE))

    latest = Window.partitionBy(*KEY).orderBy(F.col("event_timestamp").desc(), F.col("ingestion_timestamp").desc())

    good = (
        typed.filter(is_valid)
        .withColumn("taker_sell_volume", F.col("volume") - F.col("taker_buy_volume"))
        .withColumn("event_date", F.to_date("kline_start_timestamp"))
        .withColumn("rn", F.row_number().over(latest))
        .filter("rn = 1").drop("rn", "raw_payload")      # one row per candle in this batch
    )

    (DeltaTable.forName(spark, TARGET_TABLE).alias("t")
        .merge(good.alias("s"),
               "t.symbol = s.symbol AND t.kline_interval = s.kline_interval "
               "AND t.kline_start_timestamp = s.kline_start_timestamp")
        .whenMatchedUpdateAll(condition="s.event_timestamp > t.event_timestamp")   # newer version wins
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

query = (
    spark.readStream.table(SOURCE_TABLE)
    .writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT symbol, is_closed, COUNT(*) AS candles FROM {TARGET_TABLE} GROUP BY symbol, is_closed ORDER BY symbol"))