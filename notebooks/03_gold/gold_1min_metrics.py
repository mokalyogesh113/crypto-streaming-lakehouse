# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: 1-minute trade metrics (event-time windows with a watermark)

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "dev"
SOURCE_TABLE = f"{CATALOG}.foundation.crypto_trades"
TARGET_TABLE = f"{CATALOG}.gold.crypto_1min_metrics"
CHECKPOINT = f"/Volumes/{CATALOG}/gold/checkpoints/gold_1min_metrics"

# COMMAND ----------

trades = (
    spark.readStream
    .option("skipChangeCommits", "true")      # tolerate MERGE commits on the source table
    .table(SOURCE_TABLE)
)

order_key = F.struct("trade_timestamp", "trade_id")   # trade_id breaks timestamp ties

agg = (
    trades
    .withWatermark("trade_timestamp", "2 minutes")    # late data beyond 2 min is dropped
    .groupBy(F.window("trade_timestamp", "1 minute"), "symbol")
    .agg(
        F.min_by("price", order_key).alias("open_price"),
        F.max("price").alias("high_price"),
        F.min("price").alias("low_price"),
        F.max_by("price", order_key).alias("close_price"),
        F.sum("quantity").alias("total_volume"),
        F.sum(F.when(F.col("trade_side") == "BUY", F.col("quantity")).otherwise(0)).alias("buy_volume"),
        F.sum(F.when(F.col("trade_side") == "SELL", F.col("quantity")).otherwise(0)).alias("sell_volume"),
        F.count("*").alias("trade_count"),
        F.avg("price").alias("avg_trade_price"),
        F.sum("quote_quantity").alias("quote_volume"),
        F.stddev_samp("price").alias("price_stddev"),
    )
)

# COMMAND ----------

def upsert(batch_df, batch_id):
    out = batch_df.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("symbol"),
        F.col("open_price").cast("decimal(20,8)"),
        F.col("high_price").cast("decimal(20,8)"),
        F.col("low_price").cast("decimal(20,8)"),
        F.col("close_price").cast("decimal(20,8)"),
        F.col("total_volume").cast("decimal(30,8)"),
        F.col("buy_volume").cast("decimal(30,8)"),
        F.col("sell_volume").cast("decimal(30,8)"),
        F.col("trade_count"),
        F.col("avg_trade_price").cast("decimal(20,8)"),
        (F.col("close_price") - F.col("open_price")).cast("decimal(20,8)").alias("price_change"),
        ((F.col("close_price") - F.col("open_price")) / F.col("open_price") * 100).cast("decimal(12,6)").alias("price_change_pct"),
        F.col("price_stddev").cast("double").alias("volatility"),
        (F.col("quote_volume") / F.col("total_volume")).cast("decimal(20,8)").alias("vwap"),
        F.current_timestamp().alias("updated_at"),
        F.to_date(F.col("window.start")).alias("event_date"),
    )

    (DeltaTable.forName(spark, TARGET_TABLE).alias("t")
        .merge(out.alias("s"), "t.symbol = s.symbol AND t.window_start = s.window_start")
        .whenMatchedUpdateAll()        # a window gets re-emitted as more trades arrive
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

query = (
    agg.writeStream
    .outputMode("update")
    .foreachBatch(upsert)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

display(spark.sql(f"""
    SELECT window_start, symbol, open_price, high_price, low_price, close_price,
           total_volume, trade_count, vwap, price_change_pct
    FROM {TARGET_TABLE} ORDER BY window_start DESC, symbol LIMIT 12
"""))