"""
sqs_producer.py — SQS Producer (replaces Kafka)

Priority queue mapping mirrors CAN bus arbitration:
  0x0C8, 0x1A4 → can-high-priority
  0x2B0        → can-medium-priority
  0x3C0        → can-low-priority
"""

import json
import logging
import os
from dataclasses import asdict

log = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError
    SQS_AVAILABLE = True
except ImportError:
    SQS_AVAILABLE = False
    log.warning("boto3 not installed — dry-run mode")

QUEUE_URLS = {
    "high":   os.getenv("SQS_HIGH_URL",   ""),
    "medium": os.getenv("SQS_MEDIUM_URL", ""),
    "low":    os.getenv("SQS_LOW_URL",    ""),
}

PRIORITY_MAP = {
    0x0C8: "high",
    0x1A4: "high",
    0x2B0: "medium",
    0x3C0: "low",
}


class CANSQSProducer:
    def __init__(self):
        self._dry_run = not SQS_AVAILABLE
        self.stats = {"sent": 0, "errors": 0, "by_queue": {}}

        if SQS_AVAILABLE:
            try:
                self.sqs = boto3.client("sqs", region_name="us-east-1")
                log.info("SQS client initialized")
            except Exception as e:
                log.warning(f"SQS init failed: {e} — dry-run mode")
                self._dry_run = True
        else:
            log.info("boto3 not installed — dry-run mode")

    def _get_queue_url(self, frame) -> str:
        priority = PRIORITY_MAP.get(frame.arbitration_id, "low")
        return QUEUE_URLS.get(priority, ""), priority

    def send_frame(self, frame) -> bool:
        queue_url, priority = self._get_queue_url(frame)
        payload = json.dumps(asdict(frame))

        if self._dry_run or not queue_url:
            log.debug(f"[dry-run] → {priority} | {frame.message_name}")
            self.stats["sent"] += 1
            self.stats["by_queue"][priority] = self.stats["by_queue"].get(priority, 0) + 1
            return True

        try:
            self.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=payload,
                MessageAttributes={
                    "arbitration_id": {
                        "StringValue": frame.arbitration_id_hex,
                        "DataType": "String"
                    },
                    "message_name": {
                        "StringValue": frame.message_name,
                        "DataType": "String"
                    }
                }
            )
            self.stats["sent"] += 1
            self.stats["by_queue"][priority] = self.stats["by_queue"].get(priority, 0) + 1
            return True

        except ClientError as e:
            log.error(f"SQS send error: {e}")
            self.stats["errors"] += 1
            return False

    def send_frames(self, frames: list) -> int:
        # SQS supports batch send of up to 10 messages at a time
        sent = 0
        batch = []

        for frame in frames:
            queue_url, priority = self._get_queue_url(frame)

            if self._dry_run or not queue_url:
                self.send_frame(frame)
                sent += 1
                continue

            batch.append((frame, queue_url, priority))

            # Flush batch of 10
            if len(batch) >= 10:
                sent += self._flush_batch(batch)
                batch = []

        # Flush remaining
        if batch:
            sent += self._flush_batch(batch)

        log.info(f"SQS: sent {sent}/{len(frames)}")
        return sent

    def _flush_batch(self, batch: list) -> int:
        # Group by queue URL
        by_queue: dict = {}
        for frame, queue_url, priority in batch:
            by_queue.setdefault(queue_url, []).append((frame, priority))

        sent = 0
        for queue_url, items in by_queue.items():
            entries = [
                {
                    "Id": str(i),
                    "MessageBody": json.dumps(asdict(frame)),
                }
                for i, (frame, _) in enumerate(items)
            ]
            try:
                resp = self.sqs.send_message_batch(
                    QueueUrl=queue_url,
                    Entries=entries
                )
                sent += len(resp.get("Successful", []))
                priority = items[0][1]
                self.stats["by_queue"][priority] = \
                    self.stats["by_queue"].get(priority, 0) + len(resp.get("Successful", []))
                self.stats["sent"] += len(resp.get("Successful", []))
            except ClientError as e:
                log.error(f"SQS batch error: {e}")
                self.stats["errors"] += len(entries)

        return sent

    def print_stats(self):
        print(f"\n── SQS Stats ─────────────────────────────────")
        print(f"  Sent   : {self.stats['sent']}")
        print(f"  Errors : {self.stats['errors']}")
        for queue, count in sorted(self.stats["by_queue"].items()):
            print(f"  {queue:<40} : {count}")
        print(f"──────────────────────────────────────────────\n")