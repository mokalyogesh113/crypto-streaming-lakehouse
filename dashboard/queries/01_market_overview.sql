SELECT symbol, current_price, price_change_1m_pct, price_change_5m_pct, price_change_1h_pct,
       volume_5m, buy_sell_ratio_5m, spread_pct, book_imbalance, as_of_window
FROM dev.gold.crypto_market_summary
ORDER BY symbol;