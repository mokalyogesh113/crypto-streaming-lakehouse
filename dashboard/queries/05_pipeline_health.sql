SELECT symbol, MAX(window_start) AS latest_window, COUNT(*) AS windows, SUM(trade_count) AS trades
FROM dev.gold.crypto_1min_metrics
GROUP BY symbol
ORDER BY symbol;