"""
sqs_producer.py — SQS Producer (Core EC2 Edge Layer)

Priority queue mapping mirrors CAN bus arbitration:
  0x0C8, 0x1A4 (engine, vehicle) -> can-high-priority
  0x2B0        (transmission)    -> can-medium-priority
  0x3C0        (body)            -> can-low-priority
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
        self.sqs = None

        if SQS_AVAILABLE:
            try:
                self.sqs = boto3.client("sqs", region_name="us-east-1")
                log.info("SQS client initialized")
            except Exception as e:
                log.warning(f"SQS init failed: {e} — dry-run mode")
                self._dry_run = True
        else:
            log.info("boto3 not installed — dry-run mode")

    def _get_priority(self, frame) -> str:
        return PRIORITY_MAP.get(frame.arbitration_id, "low")

    def send_frames(self, frames: list) -> int:
        if not frames:
            return 0

        by_priority = {"high": [], "medium": [], "low": []}
        for frame in frames:
            by_priority[self._get_priority(frame)].append(frame)

        total_sent = 0
        for priority, pframes in by_priority.items():
            if not pframes:
                continue
            queue_url = QUEUE_URLS.get(priority, "")
            total_sent += self._send_batch(pframes, queue_url, priority)

        log.info(f"SQS: sent {total_sent}/{len(frames)}")
        return total_sent

    def _send_batch(self, frames: list, queue_url: str, priority: str) -> int:
        sent = 0
        for i in range(0, len(frames), 10):
            chunk = frames[i:i + 10]

            if self._dry_run or not queue_url:
                for frame in chunk:
                    log.debug(f"[dry-run] -> {priority} | {frame.message_name}")
                    self.stats["sent"] += 1
                    self.stats["by_queue"][priority] = \
                        self.stats["by_queue"].get(priority, 0) + 1
                sent += len(chunk)
                continue

            try:
                entries = [
                    {
                        "Id": str(idx),
                        "MessageBody": json.dumps(asdict(frame)),
                        "MessageAttributes": {
                            "message_name": {
                                "StringValue": frame.message_name,
                                "DataType": "String"
                            },
                            "arbitration_id": {
                                "StringValue": frame.arbitration_id_hex,
                                "DataType": "String"
                            }
                        }
                    }
                    for idx, frame in enumerate(chunk)
                ]
                resp = self.sqs.send_message_batch(
                    QueueUrl=queue_url, Entries=entries)

                ok  = len(resp.get("Successful", []))
                err = len(resp.get("Failed", []))
                sent += ok
                self.stats["sent"]   += ok
                self.stats["errors"] += err
                self.stats["by_queue"][priority] = \
                    self.stats["by_queue"].get(priority, 0) + ok

                if err:
                    log.warning(f"SQS: {err} failed in {priority} queue")

            except ClientError as e:
                log.error(f"SQS batch error ({priority}): {e}")
                self.stats["errors"] += len(chunk)

        return sent

    def print_stats(self):
        print(f"\n── SQS Stats ─────────────────────────────────")
        print(f"  Sent   : {self.stats['sent']}")
        print(f"  Errors : {self.stats['errors']}")
        for queue, count in sorted(self.stats["by_queue"].items()):
            print(f"  {queue:<40} : {count}")
        print(f"──────────────────────────────────────────────\n")