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