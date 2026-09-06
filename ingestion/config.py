# Symbols to track
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]

# Binance combined stream URL
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"

# Build the combined stream query param, e.g.:
# btcusdt@trade/ethusdt@trade/btcusdt@kline_1m/...
def build_stream_url():
    streams = []
    for symbol in SYMBOLS:
        streams.append(f"{symbol}@trade")
        streams.append(f"{symbol}@kline_1m")
        streams.append(f"{symbol}@bookTicker")
    stream_param = "/".join(streams)
    return f"{BINANCE_WS_BASE}?streams={stream_param}"

# Map Binance event types to Kafka topics
EVENT_TOPIC_MAP = {
    "trade": "raw-trades",
    "kline": "raw-klines",
    "bookTicker": "raw-bookticker",
}

# Kafka / Redpanda connection
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Reconnect behavior
RECONNECT_DELAY_SECONDS = 5
MAX_RECONNECT_ATTEMPTS = 10

def get_topic_for_stream(stream_name: str) -> str:
    """Map a Binance stream name like 'btcusdt@bookTicker' to a Kafka topic."""
    if "@trade" in stream_name:
        return EVENT_TOPIC_MAP["trade"]
    elif "@kline" in stream_name:
        return EVENT_TOPIC_MAP["kline"]
    elif "@bookTicker" in stream_name:
        return EVENT_TOPIC_MAP["bookTicker"]
    else:
        return None