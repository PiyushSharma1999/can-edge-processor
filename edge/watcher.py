"""
watcher.py — Orchestrator (Core EC2 / Greengrass Component)

Three trigger modes:
  file   — process a single .asc file directly (dev/testing)
  folder — watch a directory for new .asc files
  mqtt   — receive file-ready notifications over MQTT from client device
"""

import json
import logging
import os
import sys
import time
import argparse

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [watcher] %(levelname)s %(message)s")

# FIX: use abspath so imports work regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parser import CANParser
from filter import CANFilter
from sqs_producer import CANSQSProducer

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False

try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False


class ASCPipeline:
    # FIX: removed kafka_servers arg — no longer needed
    def __init__(self, dbc_path: str):
        self.parser    = CANParser(dbc_path)
        self.flt       = CANFilter(drop_error_frames=True, validate_ranges=True)
        self.producer  = CANSQSProducer()
        self.processed = set()

    def run(self, asc_path: str):
        if asc_path in self.processed:
            log.info(f"Already processed: {asc_path} — skipping")
            return
        self.processed.add(asc_path)
        log.info(f"━━━ Processing: {asc_path} ━━━")
        try:
            frames = self.parser.parse_asc_file(asc_path)
            clean  = self.flt.filter_frames(frames)
            sent   = self.producer.send_frames(clean)
            self.flt.print_stats()
            self.producer.print_stats()
            # FIX: was "sent to Kafka"
            log.info(f"━━━ Done: {sent} frames sent to SQS ━━━\n")
        except Exception as e:
            log.error(f"Pipeline error: {e}", exc_info=True)


class FolderWatcher:
    def __init__(self, watch_dir: str, pipeline: ASCPipeline):
        self.watch_dir = watch_dir
        self.pipeline  = pipeline

    def start(self):
        if not WATCHDOG_AVAILABLE:
            log.error("pip install watchdog")
            return

        class Handler(FileSystemEventHandler):
            def __init__(self, pipeline):
                self.pipeline = pipeline
            def on_created(self, event):
                if not event.is_directory and event.src_path.endswith(".asc"):
                    log.info(f"New file detected: {event.src_path}")
                    time.sleep(0.5)
                    self.pipeline.run(event.src_path)

        observer = Observer()
        observer.schedule(Handler(self.pipeline), self.watch_dir, recursive=False)
        observer.start()
        log.info(f"Watching: {self.watch_dir}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()


class MQTTWatcher:
    def __init__(self, broker: str, port: int, topic: str, pipeline: ASCPipeline):
        self.broker   = broker
        self.port     = port
        self.topic    = topic
        self.pipeline = pipeline

    def start(self):
        if not MQTT_AVAILABLE:
            log.error("pip install paho-mqtt")
            return

        def on_connect(client, userdata, flags, rc):
            client.subscribe(self.topic, qos=1)
            log.info(f"MQTT subscribed: {self.topic}")

        def on_message(client, userdata, msg):
            try:
                payload  = json.loads(msg.payload.decode())
                asc_path = payload.get("asc_file")
                if asc_path:
                    self.pipeline.run(asc_path)
            except Exception as e:
                log.error(f"MQTT error: {e}")

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,
                             client_id="can-edge-processor")
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(self.broker, self.port)
        client.loop_forever()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dbc",         default="dbc/vehicle.dbc")
    ap.add_argument("--mode",        choices=["file", "folder", "mqtt"], default="folder")
    ap.add_argument("--watch-dir",   default="/tmp/can-data/")
    ap.add_argument("--mqtt-broker", default="localhost")
    ap.add_argument("--mqtt-port",   type=int, default=1883)
    ap.add_argument("--mqtt-topic",  default="can/new-file")
    ap.add_argument("--asc",         default=None)
    args = ap.parse_args()

    # FIX: ASCPipeline takes only dbc_path now
    pipeline = ASCPipeline(args.dbc)

    if args.mode == "file":
        if not args.asc:
            ap.error("--asc required for mode=file")
        pipeline.run(args.asc)
    elif args.mode == "folder":
        os.makedirs(args.watch_dir, exist_ok=True)
        FolderWatcher(args.watch_dir, pipeline).start()
    elif args.mode == "mqtt":
        MQTTWatcher(args.mqtt_broker, args.mqtt_port,
                    args.mqtt_topic, pipeline).start()