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