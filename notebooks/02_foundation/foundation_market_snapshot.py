# Databricks notebook source
# MAGIC %md
# MAGIC # Foundation: market snapshot (best bid/ask with derived spread)

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "dev"
SOURCE_TABLE = f"{CATALOG}.raw.binance_book_ticker"
TARGET_TABLE = f"{CATALOG}.foundation.crypto_market_snapshot"
REJECTED_TABLE = f"{CATALOG}.foundation.crypto_market_snapshot_rejected"
CHECKPOINT = f"/Volumes/{CATALOG}/foundation/checkpoints/foundation_market_snapshot"

# COMMAND ----------

def process_batch(batch_df, batch_id):
    typed = batch_df.select(
        F.col("update_id").cast("bigint").alias("update_id"),
        F.col("symbol"),
        F.col("ingestion_time").alias("observed_timestamp"),   # bookTicker has no event time
        F.col("best_bid_price").cast("decimal(20,8)").alias("bid_price"),
        F.col("best_bid_qty").cast("decimal(20,8)").alias("bid_quantity"),
        F.col("best_ask_price").cast("decimal(20,8)").alias("ask_price"),
        F.col("best_ask_qty").cast("decimal(20,8)").alias("ask_quantity"),
        F.col("ingestion_time").alias("ingestion_timestamp"),
        F.col("raw_payload"),
    )

    is_valid = F.coalesce(
        F.col("update_id").isNotNull()
        & F.col("symbol").isNotNull()
        & (F.col("bid_price") > 0) & (F.col("ask_price") > 0)
        & (F.col("ask_price") >= F.col("bid_price")),
        F.lit(False),
    )

    (typed.filter(~is_valid)
        .withColumn("rejected_at", F.current_timestamp())
        .write.mode("append").saveAsTable(REJECTED_TABLE))

    good = (
        typed.filter(is_valid)
        .withColumn("mid_price", ((F.col("bid_price") + F.col("ask_price")) / 2).cast("decimal(20,8)"))
        .withColumn("spread", (F.col("ask_price") - F.col("bid_price")).cast("decimal(20,8)"))
        .withColumn("spread_percentage", (F.col("spread") / F.col("mid_price") * 100).cast("decimal(12,8)"))
        .withColumn("total_bid_value", (F.col("bid_price") * F.col("bid_quantity")).cast("decimal(38,13)"))
        .withColumn("total_ask_value", (F.col("ask_price") * F.col("ask_quantity")).cast("decimal(38,13)"))
        .withColumn("event_date", F.to_date("observed_timestamp"))
        .drop("raw_payload")
        .dropDuplicates(["symbol", "update_id"])
    )

    (DeltaTable.forName(spark, TARGET_TABLE).alias("t")
        .merge(good.alias("s"), "t.symbol = s.symbol AND t.update_id = s.update_id")
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

display(spark.sql(f"SELECT symbol, COUNT(*) AS snapshots, ROUND(AVG(spread_percentage), 6) AS avg_spread_pct FROM {TARGET_TABLE} GROUP BY symbol"))