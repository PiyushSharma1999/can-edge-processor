"""
app.py — Flask API (Cloud EC2)
"""

import os
from flask import Flask, jsonify, request

app = Flask(__name__)

SQS_HIGH_URL   = os.getenv("SQS_HIGH_URL",   "https://sqs.us-east-1.amazonaws.com/295690253730/can-high-priority")
SQS_MEDIUM_URL = os.getenv("SQS_MEDIUM_URL", "https://sqs.us-east-1.amazonaws.com/295690253730/can-medium-priority")
SQS_LOW_URL    = os.getenv("SQS_LOW_URL",    "https://sqs.us-east-1.amazonaws.com/295690253730/can-low-priority")


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/signals")
def list_signals():
    return jsonify({
        "ENGINE_STATUS":      {"arb_id": "0x0C8", "priority": "HIGH",
            "sqs_queue": "can-high-priority",
            "signals": ["EngineRPM","CoolantTemp","ThrottlePos","EngineLoad","FuelPressure"]},
        "VEHICLE_DYNAMICS":   {"arb_id": "0x1A4", "priority": "HIGH",
            "sqs_queue": "can-high-priority",
            "signals": ["VehicleSpeed","BrakePressure","LateralAccel","LongitudinalAccel","ABSActive"]},
        "TRANSMISSION_STATUS":{"arb_id": "0x2B0", "priority": "MEDIUM",
            "sqs_queue": "can-medium-priority",
            "signals": ["GearPosition","TorqueConverterSlip","TransmissionTemp"]},
        "BODY_CONTROL":       {"arb_id": "0x3C0", "priority": "LOW",
            "sqs_queue": "can-low-priority",
            "signals": ["HeadlightsOn","AmbientTemp","WipersActive"]},
    })


@app.route("/api/status")
def status():
    return jsonify({
        "queues": {
            "high":   SQS_HIGH_URL   or "not configured",
            "medium": SQS_MEDIUM_URL or "not configured",
            "low":    SQS_LOW_URL    or "not configured",
        },
        "influxdb": os.getenv("INFLUX_URL", "http://localhost:8086"),
        "grafana":  "http://localhost:3000",
    })


@app.route("/api/queue/stats")
def queue_stats():
    try:
        import boto3
        sqs   = boto3.client("sqs", region_name="us-east-1")
        stats = {}
        for name, url in [("high",   SQS_HIGH_URL),
                          ("medium", SQS_MEDIUM_URL),
                          ("low",    SQS_LOW_URL)]:
            if not url:
                stats[name] = "not configured"
                continue
            resp  = sqs.get_queue_attributes(
                QueueUrl=url,
                AttributeNames=["ApproximateNumberOfMessages",
                                 "ApproximateNumberOfMessagesNotVisible"])
            attrs = resp.get("Attributes", {})
            stats[name] = {
                "available": attrs.get("ApproximateNumberOfMessages", "?"),
                "in_flight": attrs.get("ApproximateNumberOfMessagesNotVisible", "?")
            }
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)