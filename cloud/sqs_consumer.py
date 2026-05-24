"""
sqs_consumer.py — SQS Consumer -> InfluxDB (Cloud EC2)

Polls three SQS queues for decoded CAN frames and writes
them to InfluxDB as time-series data points.
"""

import json
import logging
import os
import time

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [sqs-consumer] %(levelname)s %(message)s")

try:
    import boto3
    from botocore.exceptions import ClientError
    SQS_AVAILABLE = True
except ImportError:
    SQS_AVAILABLE = False
    log.error("boto3 not installed: pip install boto3")

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False
    log.warning("influxdb-client not installed — dry-run mode")

SQS_HIGH_URL   = os.getenv("SQS_HIGH_URL",   "")
SQS_MEDIUM_URL = os.getenv("SQS_MEDIUM_URL", "")
SQS_LOW_URL    = os.getenv("SQS_LOW_URL",    "")
QUEUE_URLS     = [SQS_HIGH_URL, SQS_MEDIUM_URL, SQS_LOW_URL]
QUEUE_NAMES    = ["high", "medium", "low"]

INFLUX_URL    = os.getenv("INFLUX_URL",    "http://localhost:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "can-pipeline-token")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "can-pipeline")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "can_data")


class InfluxWriter:
    def __init__(self):
        if INFLUX_AVAILABLE:
            try:
                self.client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN,
                                                org=INFLUX_ORG)
                self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
                log.info(f"InfluxDB connected: {INFLUX_URL}")
            except Exception as e:
                log.warning(f"InfluxDB failed: {e} — dry-run mode")
                self.write_api = None
        else:
            self.write_api = None

    def write_frame(self, frame: dict):
        if not frame.get("signals"):
            return

        msg_name = frame.get("message_name", "UNKNOWN")
        arb_id   = frame.get("arbitration_id_hex", "0x000")
        ts_ns    = int(frame.get("timestamp", 0) * 1e9)

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

        if not points:
            return

        if self.write_api:
            try:
                self.write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG,
                                     record=points)
            except Exception as e:
                log.error(f"InfluxDB write error: {e}")
        else:
            for p in points:
                log.debug(f"[dry-run] {p.to_line_protocol()}")


def poll_queue(sqs_client, queue_url: str, queue_name: str,
               writer: InfluxWriter) -> int:
    received = 0
    while True:
        try:
            resp = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
                MessageAttributeNames=["All"]
            )
        except ClientError as e:
            log.error(f"SQS receive error ({queue_name}): {e}")
            break

        messages = resp.get("Messages", [])
        if not messages:
            break

        for msg in messages:
            try:
                frame = json.loads(msg["Body"])
                writer.write_frame(frame)
                received += 1
                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=msg["ReceiptHandle"]
                )
            except Exception as e:
                log.error(f"Error processing message: {e}")

    return received


def run_consumer():
    if not SQS_AVAILABLE:
        log.error("pip install boto3")
        return

    sqs    = boto3.client("sqs", region_name="us-east-1")
    writer = InfluxWriter()

    log.info("SQS consumer started — polling:")
    for name, url in zip(QUEUE_NAMES, QUEUE_URLS):
        log.info(f"  {name}: {url or 'NOT SET'}")

    total = 0
    while True:
        batch = 0
        for url, name in zip(QUEUE_URLS, QUEUE_NAMES):
            if url:
                batch += poll_queue(sqs, url, name, writer)
        total += batch
        if batch > 0:
            log.info(f"Processed {batch} frames (total: {total})")
        time.sleep(1)


if __name__ == "__main__":
    run_consumer()