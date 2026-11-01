import io
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer
from databricks.sdk import WorkspaceClient

from config import KAFKA_BOOTSTRAP_SERVERS

load_dotenv()  # reads DATABRICKS_HOST and DATABRICKS_TOKEN

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TOPIC_TO_FOLDER = {
    "raw-trades": "trades",
    "raw-klines": "klines",
    "raw-bookticker": "bookticker",
}
VOLUME_BASE = "/Volumes/dev/raw/landing"
FLUSH_SECONDS = 30
FLUSH_MAX_RECORDS = 5000

w = WorkspaceClient()


def flush(buffers: dict):
    """Upload each folder's buffered events as one JSON Lines file."""
    for folder in list(buffers.keys()):
        lines = buffers[folder]
        if not lines:
            del buffers[folder]
            continue
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        path = f"{VOLUME_BASE}/{folder}/{stamp}_{uuid.uuid4().hex[:8]}.jsonl"
        data = b"\n".join(lines) + b"\n"
        w.files.upload(path, io.BytesIO(data), overwrite=True)
        logger.info(f"Uploaded {len(lines)} events to {path}")
        del buffers[folder]  # only removed after a successful upload


def main():
    consumer = KafkaConsumer(
        *TOPIC_TO_FOLDER.keys(),
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="volume-sink",
        enable_auto_commit=False,   # commit only after files are uploaded
        auto_offset_reset="earliest",
    )
    buffers = defaultdict(list)
    last_flush = time.time()

    try:
        while True:
            batches = consumer.poll(timeout_ms=1000, max_records=1000)
            for tp, messages in batches.items():
                folder = TOPIC_TO_FOLDER[tp.topic]
                for msg in messages:
                    buffers[folder].append(msg.value)  # raw JSON bytes, untouched

            total = sum(len(v) for v in buffers.values())
            if total >= FLUSH_MAX_RECORDS or (total > 0 and time.time() - last_flush >= FLUSH_SECONDS):
                flush(buffers)
                consumer.commit()
                last_flush = time.time()
    except KeyboardInterrupt:
        logger.info("Stopping, flushing remaining events...")
        flush(buffers)
        consumer.commit()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()