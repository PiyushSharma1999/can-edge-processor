"""
kafka_producer.py — Kafka Producer (Edge Processor)

WHY KAFKA OVER SQS:
  Kafka preserves message ordering within a partition.
  SQS does not guarantee ordering (standard queues).
  CAN data is time-series — order matters for signal reconstruction.

PRIORITY → TOPIC MAPPING (mirrors CAN bus arbitration):
  0x0C8 (engine)  → can.decoded.high_priority
  0x1A4 (vehicle) → can.decoded.high_priority
  0x2B0 (trans)   → can.decoded.medium_priority
  0x3C0 (body)    → can.decoded.low_priority

MESSAGE KEY = arbitration_id:
  Kafka routes messages with the same key to the same partition.
  This guarantees all ENGINE_STATUS frames stay in order.
"""

import json
import logging
import os
import sys
from dataclasses import asdict

log = logging.getLogger(__name__)

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Priority topic mapping
PRIORITY_TOPICS = {
    0x0C8: "can.decoded.high_priority",
    0x1A4: "can.decoded.high_priority",
    0x2B0: "can.decoded.medium_priority",
    0x3C0: "can.decoded.low_priority",
}
DEFAULT_TOPIC = "can.decoded.unknown"


class CANKafkaProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self._dry_run = not KAFKA_AVAILABLE
        self.stats    = {"sent": 0, "errors": 0, "by_topic": {}}

        if KAFKA_AVAILABLE:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: str(k).encode("utf-8"),
                    retries=3,
                    acks="all",
                    linger_ms=1,
                )
                log.info(f"Kafka connected: {bootstrap_servers}")
            except Exception as e:
                log.error(f"Kafka connect failed: {e}")
                self._dry_run = True
        else:
            log.info("kafka-python not installed — dry-run mode")

    def _get_topic(self, frame) -> str:
        return PRIORITY_TOPICS.get(frame.arbitration_id, DEFAULT_TOPIC)

    def send_frame(self, frame) -> bool:
        topic   = self._get_topic(frame)
        payload = asdict(frame)

        if self._dry_run:
            log.debug(f"[dry-run] → {topic} | {frame.message_name} "
                      f"t={frame.timestamp:.3f}s")
            self.stats["sent"] += 1
            self.stats["by_topic"][topic] = self.stats["by_topic"].get(topic, 0) + 1
            return True

        try:
            self.producer.send(topic=topic, key=frame.arbitration_id, value=payload)
            self.stats["sent"] += 1
            self.stats["by_topic"][topic] = self.stats["by_topic"].get(topic, 0) + 1
            return True
        except KafkaError as e:
            log.error(f"Kafka error: {e}")
            self.stats["errors"] += 1
            return False

    def send_frames(self, frames: list) -> int:
        sent = sum(1 for f in frames if self.send_frame(f))
        if not self._dry_run and hasattr(self, 'producer'):
            self.producer.flush()
        log.info(f"Kafka: sent {sent}/{len(frames)}")
        return sent

    def print_stats(self):
        print(f"\n── Kafka Stats ───────────────────────────────")
        print(f"  Sent   : {self.stats['sent']}")
        print(f"  Errors : {self.stats['errors']}")
        for topic, count in sorted(self.stats["by_topic"].items()):
            print(f"  {topic:<40} : {count}")
        print(f"──────────────────────────────────────────────\n")

    def close(self):
        if hasattr(self, 'producer') and self.producer:
            self.producer.close()