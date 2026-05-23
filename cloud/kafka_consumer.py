"""
kafka_consumer.py — Kafka → InfluxDB (Cloud EC2)

InfluxDB line protocol:
  <measurement>,<tag>=<value> <field>=<value> <timestamp_ns>
  ENGINE_STATUS,signal=EngineRPM value=2000.0 1700000000000000000
"""

import json
import logging
import os

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [cloud] %(levelname)s %(message)s")

try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False

TOPICS        = ["can.decoded.high_priority",
                 "can.decoded.medium_priority",
                 "can.decoded.low_priority"]
INFLUX_URL    = os.getenv("INFLUX_URL",    "http://localhost:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "can-pipeline-token")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "can-pipeline")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "can_data")
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")


class InfluxWriter:
    def __init__(self):
        if INFLUX_AVAILABLE:
            self.client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN,
                                            org=INFLUX_ORG)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        else:
            self.write_api = None

    def write_frame(self, frame: dict):
        if not frame.get("signals"):
            return
        msg_name = frame.get("message_name", "UNKNOWN")
        arb_id   = frame.get("arbitration_id_hex", "0x000")
        ts_ns    = int(frame["timestamp"] * 1e9)

        points = []
        for sig_name, value in frame["signals"].items():
            if isinstance(value, (int, float)):
                points.append(
                    Point(msg_name)
                    .tag("signal", sig_name)
                    .tag("arbitration_id", arb_id)
                    .field("value", float(value))
                    .time(ts_ns, WritePrecision.NANOSECONDS)
                )

        if INFLUX_AVAILABLE and self.write_api:
            self.write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        else:
            for p in points:
                log.debug(f"[dry-run] {p.to_line_protocol()}")


def run_consumer():
    if not KAFKA_AVAILABLE:
        log.error("pip install kafka-python")
        return

    writer   = InfluxWriter()
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="can-influx-writer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    log.info(f"Consuming topics: {TOPICS}")
    count = 0
    for msg in consumer:
        writer.write_frame(msg.value)
        count += 1
        if count % 1000 == 0:
            log.info(f"Written {count} frames to InfluxDB")


if __name__ == "__main__":
    run_consumer()