# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: market summary (latest state per symbol, for the dashboard)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "dev"
METRICS = f"{CATALOG}.gold.crypto_1min_metrics"
SNAPSHOT = f"{CATALOG}.foundation.crypto_market_snapshot"
TARGET_TABLE = f"{CATALOG}.gold.crypto_market_summary"

# COMMAND ----------

m = spark.table(METRICS)
latest = m.groupBy("symbol").agg(F.max("window_start").alias("as_of"))
w = m.join(latest, "symbol")

def last_n_minutes(n):
    """Aggregate the last n one-minute windows up to each symbol's latest window."""
    sub = w.filter(F.col("window_start") >= F.expr(f"as_of - interval {n - 1} minutes"))
    return sub.groupBy("symbol").agg(
        F.min_by("open_price", "window_start").alias(f"open_{n}"),
        F.max_by("close_price", "window_start").alias(f"close_{n}"),
        F.sum("total_volume").alias(f"volume_{n}"),
        F.stddev_samp("price_change_pct").alias(f"volatility_{n}"),
        F.sum("buy_volume").alias(f"buy_{n}"),
        F.sum("sell_volume").alias(f"sell_{n}"),
    )

a1, a5, a60 = last_n_minutes(1), last_n_minutes(5), last_n_minutes(60)

# Spread and book imbalance over the last 5 minutes of snapshots
snap = (
    spark.table(SNAPSHOT)
    .join(latest, "symbol")
    .filter(
        (F.col("observed_timestamp") >= F.expr("as_of - interval 4 minutes"))
        & (F.col("observed_timestamp") < F.expr("as_of + interval 1 minute"))
    )
    .groupBy("symbol")
    .agg(
        F.avg("spread_percentage").cast("double").alias("spread_pct"),
        F.avg(((F.col("bid_quantity") - F.col("ask_quantity"))
               / (F.col("bid_quantity") + F.col("ask_quantity"))).cast("double")).alias("book_imbalance"),
    )
)

def pct(close_col, open_col):
    return ((F.col(close_col) - F.col(open_col)) / F.col(open_col) * 100).cast("double")

summary = (
    latest
    .join(a1, "symbol").join(a5, "symbol").join(a60, "symbol")
    .join(snap, "symbol", "left")
    .select(
        F.col("as_of").alias("as_of_window"),
        "symbol",
        F.col("close_1").alias("current_price"),
        pct("close_1", "open_1").alias("price_change_1m_pct"),
        pct("close_5", "open_5").alias("price_change_5m_pct"),
        pct("close_60", "open_60").alias("price_change_1h_pct"),
        F.col("volume_1").alias("volume_1m"),
        F.col("volume_5").alias("volume_5m"),
        F.col("volume_60").alias("volume_1h"),
        F.col("volatility_5").alias("volatility_5m"),
        F.col("volatility_60").alias("volatility_1h"),
        F.when(F.col("sell_5") > 0, (F.col("buy_5") / F.col("sell_5")).cast("double")).alias("buy_sell_ratio_5m"),
        "spread_pct",
        "book_imbalance",
        F.current_timestamp().alias("updated_at"),
    )
)

# COMMAND ----------

summary.createOrReplaceTempView("summary_staging")
spark.sql(f"CREATE OR REPLACE TABLE {TARGET_TABLE} AS SELECT * FROM summary_staging")

# COMMAND ----------

display(spark.table(TARGET_TABLE))