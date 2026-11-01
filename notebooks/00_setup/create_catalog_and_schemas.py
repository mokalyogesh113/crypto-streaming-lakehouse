# Databricks notebook source

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dev;
# MAGIC CREATE SCHEMA IF NOT EXISTS dev.raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS dev.foundation;
# MAGIC CREATE SCHEMA IF NOT EXISTS dev.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS dev.raw.landing;

# COMMAND ----------

for t in ["trades", "klines", "bookticker"]:
    dbutils.fs.mkdirs(f"/Volumes/dev/raw/landing/{t}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS dev.raw.checkpoints;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.foundation.crypto_trades (
# MAGIC   trade_id BIGINT,
# MAGIC   symbol STRING,
# MAGIC   event_timestamp TIMESTAMP,
# MAGIC   trade_timestamp TIMESTAMP,
# MAGIC   price DECIMAL(20,8),
# MAGIC   quantity DECIMAL(20,8),
# MAGIC   quote_quantity DECIMAL(38,13),
# MAGIC   is_market_maker BOOLEAN,
# MAGIC   trade_side STRING,
# MAGIC   event_date DATE,
# MAGIC   ingestion_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (event_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.foundation.crypto_klines (
# MAGIC   symbol STRING,
# MAGIC   kline_interval STRING,
# MAGIC   kline_start_timestamp TIMESTAMP,
# MAGIC   kline_close_timestamp TIMESTAMP,
# MAGIC   open_price DECIMAL(20,8),
# MAGIC   high_price DECIMAL(20,8),
# MAGIC   low_price DECIMAL(20,8),
# MAGIC   close_price DECIMAL(20,8),
# MAGIC   volume DECIMAL(30,12),
# MAGIC   quote_volume DECIMAL(30,12),
# MAGIC   trade_count BIGINT,
# MAGIC   taker_buy_volume DECIMAL(30,12),
# MAGIC   taker_sell_volume DECIMAL(30,12),
# MAGIC   is_closed BOOLEAN,
# MAGIC   event_timestamp TIMESTAMP,
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   event_date DATE
# MAGIC ) USING DELTA PARTITIONED BY (event_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.foundation.crypto_market_snapshot (
# MAGIC   update_id BIGINT,
# MAGIC   symbol STRING,
# MAGIC   observed_timestamp TIMESTAMP,
# MAGIC   bid_price DECIMAL(20,8),
# MAGIC   bid_quantity DECIMAL(20,8),
# MAGIC   ask_price DECIMAL(20,8),
# MAGIC   ask_quantity DECIMAL(20,8),
# MAGIC   mid_price DECIMAL(20,8),
# MAGIC   spread DECIMAL(20,8),
# MAGIC   spread_percentage DECIMAL(12,8),
# MAGIC   total_bid_value DECIMAL(38,13),
# MAGIC   total_ask_value DECIMAL(38,13),
# MAGIC   event_date DATE,
# MAGIC   ingestion_timestamp TIMESTAMP
# MAGIC ) USING DELTA PARTITIONED BY (event_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS dev.gold.checkpoints;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.gold.crypto_1min_metrics (
# MAGIC   window_start TIMESTAMP,
# MAGIC   window_end TIMESTAMP,
# MAGIC   symbol STRING,
# MAGIC   open_price DECIMAL(20,8),
# MAGIC   high_price DECIMAL(20,8),
# MAGIC   low_price DECIMAL(20,8),
# MAGIC   close_price DECIMAL(20,8),
# MAGIC   total_volume DECIMAL(30,8),
# MAGIC   buy_volume DECIMAL(30,8),
# MAGIC   sell_volume DECIMAL(30,8),
# MAGIC   trade_count BIGINT,
# MAGIC   avg_trade_price DECIMAL(20,8),
# MAGIC   price_change DECIMAL(20,8),
# MAGIC   price_change_pct DECIMAL(12,6),
# MAGIC   volatility DOUBLE,
# MAGIC   vwap DECIMAL(20,8),
# MAGIC   updated_at TIMESTAMP,
# MAGIC   event_date DATE
# MAGIC ) USING DELTA PARTITIONED BY (event_date);