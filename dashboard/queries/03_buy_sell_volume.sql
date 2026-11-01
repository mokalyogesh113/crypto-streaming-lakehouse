SELECT window_start, buy_volume, sell_volume
FROM dev.gold.crypto_1min_metrics
WHERE symbol = 'BTCUSDT'
  AND window_start >= (SELECT MAX(window_start) FROM dev.gold.crypto_1min_metrics) - INTERVAL 2 HOURS
ORDER BY window_start;