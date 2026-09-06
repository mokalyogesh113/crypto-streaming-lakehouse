import json
import logging
from confluent_kafka import Producer

from config import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)


class RedpandaProducer:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "binance-ingestion-service",
        })

    def publish(self, topic: str, payload: dict):
        try:
            self.producer.produce(
                topic=topic,
                value=json.dumps(payload).encode("utf-8"),
                callback=self._delivery_report,
            )
            # Non-blocking; triggers callbacks for previously sent messages
            self.producer.poll(0)
        except BufferError:
            logger.warning("Producer queue full, waiting...")
            self.producer.poll(1)
            self.producer.produce(topic=topic, value=json.dumps(payload).encode("utf-8"))

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Delivery failed for {msg.topic()}: {err}")
        # else: silent on success to avoid flooding logs

    def flush(self):
        self.producer.flush()