# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: anomaly events (rule-based, z-score vs. trailing baseline)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = "dev"
METRICS = f"{CATALOG}.gold.crypto_1min_metrics"
TARGET_TABLE = f"{CATALOG}.gold.crypto_anomaly_events"

BASELINE_WINDOWS = 30     # trailing one-minute windows used as the baseline
MIN_HISTORY = 10          # skip windows without enough history
Z_THRESHOLD = 3.0
PRICE_MIN_ABS_PCT = 0.05  # ignore price moves smaller than 0.05%
VOLUME_MIN_RATIO = 3.0    # volume must be at least 3x the baseline average

# COMMAND ----------

# Only windows closed by more than the 2-minute watermark, so they can't still change.
m = spark.table(METRICS).filter(F.col("window_end") <= F.expr("current_timestamp() - interval 2 minutes"))
baseline = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-BASELINE_WINDOWS, -1)

def with_stats(metric_col):
    return (
        m.select("window_start", "symbol", F.col(metric_col).cast("double").alias("v"))
        .withColumn("mean", F.avg("v").over(baseline))
        .withColumn("std", F.stddev_samp("v").over(baseline))
        .withColumn("n", F.count("v").over(baseline))
        .filter((F.col("n") >= MIN_HISTORY) & (F.col("std") > 0))
        .withColumn("z", (F.col("v") - F.col("mean")) / F.col("std"))
    )

def to_events(df, anomaly_type, metric_name, rule_text):
    severity = (F.when(F.abs("z") >= 6, "HIGH").when(F.abs("z") >= 4, "MEDIUM").otherwise("LOW"))
    return df.select(
        F.sha2(F.concat_ws("|", F.col("symbol"), F.lit(anomaly_type), F.col("window_start").cast("string")), 256).alias("event_id"),
        F.col("window_start").alias("event_timestamp"),
        "symbol",
        F.lit(anomaly_type).alias("anomaly_type"),
        F.lit(metric_name).alias("metric_name"),
        F.col("v").alias("metric_value"),
        F.col("mean").alias("baseline_value"),
        F.when(F.abs("mean") > 0, (F.col("v") - F.col("mean")) / F.abs("mean") * 100).alias("deviation_pct"),
        F.col("z").alias("z_score"),
        severity.alias("severity"),
        F.lit(rule_text).alias("detection_rule"),
        F.current_timestamp().alias("created_at"),
        F.to_date("window_start").alias("event_date"),
    )

price = to_events(
    with_stats("price_change_pct").filter((F.abs("z") >= Z_THRESHOLD) & (F.abs("v") >= PRICE_MIN_ABS_PCT)),
    "PRICE_SPIKE", "price_change_pct",
    f"|z| >= {Z_THRESHOLD} over {BASELINE_WINDOWS} windows and |change| >= {PRICE_MIN_ABS_PCT}%",
)

volume = to_events(
    with_stats("total_volume").filter((F.col("z") >= Z_THRESHOLD) & (F.col("v") >= VOLUME_MIN_RATIO * F.col("mean"))),
    "VOLUME_SPIKE", "total_volume",
    f"z >= {Z_THRESHOLD} over {BASELINE_WINDOWS} windows and volume >= {VOLUME_MIN_RATIO}x baseline",
)

events = price.unionByName(volume)

# COMMAND ----------

(DeltaTable.forName(spark, TARGET_TABLE).alias("t")
    .merge(events.alias("s"), "t.event_id = s.event_id")
    .whenNotMatchedInsertAll()          # rerunning never creates duplicate events
    .execute())

# COMMAND ----------

display(spark.sql(f"""
    SELECT event_timestamp, symbol, anomaly_type, severity, metric_value, baseline_value, deviation_pct, z_score
    FROM {TARGET_TABLE} ORDER BY event_timestamp DESC LIMIT 20
"""))