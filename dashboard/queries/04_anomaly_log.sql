SELECT event_timestamp, symbol, anomaly_type, severity, metric_value, baseline_value,
       ROUND(deviation_pct, 1) AS deviation_pct, ROUND(z_score, 2) AS z_score
FROM dev.gold.crypto_anomaly_events
ORDER BY event_timestamp DESC
LIMIT 20;