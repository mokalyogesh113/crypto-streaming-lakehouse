import asyncio
import json
import logging
import websockets

from config import build_stream_url, get_topic_for_stream, RECONNECT_DELAY_SECONDS, MAX_RECONNECT_ATTEMPTS
from kafka_producer import RedpandaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

producer = RedpandaProducer()


async def consume_binance_stream():
    url = build_stream_url()
    attempt = 0

    while attempt < MAX_RECONNECT_ATTEMPTS:
        try:
            logger.info(f"Connecting to Binance WebSocket: {url}")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logger.info("Connected. Listening for events...")
                attempt = 0

                async for raw_message in ws:
                    event = json.loads(raw_message)
                    handle_event(event)

        except (websockets.ConnectionClosed, OSError) as e:
            attempt += 1
            logger.warning(f"Connection lost ({e}). Reconnect attempt {attempt}/{MAX_RECONNECT_ATTEMPTS}")
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    producer.flush()
    logger.error("Max reconnect attempts reached. Exiting.")


def handle_event(event: dict):
    stream_name = event.get("stream", "")
    payload = event.get("data", {})

    topic = get_topic_for_stream(stream_name)
    if topic is None:
        logger.warning(f"Unrecognized stream: {stream_name}")
        return

    producer.publish(topic, payload)


if __name__ == "__main__":
    try:
        asyncio.run(consume_binance_stream())
    except KeyboardInterrupt:
        logger.info("Shutting down, flushing producer...")
        producer.flush()