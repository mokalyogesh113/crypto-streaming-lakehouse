# Databricks notebook source
# MAGIC %md
# MAGIC # Foundation: trades (typed, validated, deduplicated)

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "dev"
SOURCE_TABLE = f"{CATALOG}.raw.binance_trades"
TARGET_TABLE = f"{CATALOG}.foundation.crypto_trades"
REJECTED_TABLE = f"{CATALOG}.foundation.crypto_trades_rejected"
CHECKPOINT = f"/Volumes/{CATALOG}/foundation/checkpoints/foundation_trades"

# COMMAND ----------

def process_batch(batch_df, batch_id):
    typed = batch_df.select(
        F.col("trade_id").cast("bigint").alias("trade_id"),
        F.col("symbol"),
        F.expr("timestamp_millis(cast(event_time_ms as bigint))").alias("event_timestamp"),
        F.expr("timestamp_millis(cast(trade_time_ms as bigint))").alias("trade_timestamp"),
        F.col("price").cast("decimal(20,8)").alias("price"),
        F.col("quantity").cast("decimal(20,8)").alias("quantity"),
        F.col("is_market_maker").cast("boolean").alias("is_market_maker"),
        F.col("ingestion_time").alias("ingestion_timestamp"),
        F.col("raw_payload"),
    )

    is_valid = F.coalesce(
        F.col("trade_id").isNotNull()
        & F.col("symbol").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & (F.col("price") > 0)
        & (F.col("quantity") > 0),
        F.lit(False),
    )

    # Rejected rows are kept, with their original payload, instead of silently dropped.
    (typed.filter(~is_valid)
        .withColumn("rejected_at", F.current_timestamp())
        .write.mode("append").saveAsTable(REJECTED_TABLE))

    good = (
        typed.filter(is_valid)
        .withColumn("quote_quantity", (F.col("price") * F.col("quantity")).cast("decimal(38,13)"))
        # Binance: is_market_maker = true means the buyer was the maker, so the taker sold.
        .withColumn("trade_side", F.when(F.col("is_market_maker"), "SELL").otherwise("BUY"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .select("trade_id", "symbol", "event_timestamp", "trade_timestamp", "price", "quantity",
                "quote_quantity", "is_market_maker", "trade_side", "event_date", "ingestion_timestamp")
        .dropDuplicates(["symbol", "trade_id"])      # duplicates inside this batch
    )

    (DeltaTable.forName(spark, TARGET_TABLE).alias("t")
        .merge(good.alias("s"), "t.symbol = s.symbol AND t.trade_id = s.trade_id")
        .whenNotMatchedInsertAll()                   # duplicates across batches
        .execute())

# COMMAND ----------

query = (
    spark.readStream.table(SOURCE_TABLE)
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT symbol, trade_side, COUNT(*) AS trades FROM {TARGET_TABLE} GROUP BY symbol, trade_side ORDER BY symbol"))