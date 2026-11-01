# Maps Binance short field codes -> readable Raw column names.
# Values stay as strings in Raw; casting happens in Foundation.

TRADE_FIELDS = {
    "e": "event_type",
    "E": "event_time_ms",
    "s": "symbol",
    "t": "trade_id",
    "p": "price",
    "q": "quantity",
    "T": "trade_time_ms",
    "m": "is_market_maker",
}

KLINE_FIELDS = {          # top level of the event
    "e": "event_type",
    "E": "event_time_ms",
    "s": "symbol",
}

KLINE_K_FIELDS = {        # nested inside the "k" object
    "t": "kline_start_ms",
    "T": "kline_close_ms",
    "i": "interval",
    "o": "open_price",
    "h": "high_price",
    "l": "low_price",
    "c": "close_price",
    "v": "volume",
    "n": "number_of_trades",
    "q": "quote_volume",
    "V": "taker_buy_base_volume",
    "Q": "taker_buy_quote_volume",
    "x": "is_closed",
}

BOOKTICKER_FIELDS = {     # this stream has no "e" field
    "u": "update_id",
    "s": "symbol",
    "b": "best_bid_price",
    "B": "best_bid_qty",
    "a": "best_ask_price",
    "A": "best_ask_qty",
}

# Columns added by the pipeline to every Raw table
METADATA_COLUMNS = ["raw_payload", "ingestion_time", "ingestion_date", "source_file"]