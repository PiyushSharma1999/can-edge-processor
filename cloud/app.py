"""
app.py — Flask API (Cloud EC2)
Mirrors Tesla's Query Engine / API layer from Image 1.
"""

import json
import os
import sys
from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/signals")
def list_signals():
    return jsonify({
        "ENGINE_STATUS":      {"arb_id": "0x0C8", "priority": "HIGH",
            "signals": ["EngineRPM","CoolantTemp","ThrottlePos","EngineLoad","FuelPressure"]},
        "VEHICLE_DYNAMICS":   {"arb_id": "0x1A4", "priority": "HIGH",
            "signals": ["VehicleSpeed","BrakePressure","LateralAccel","LongitudinalAccel","ABSActive"]},
        "TRANSMISSION_STATUS":{"arb_id": "0x2B0", "priority": "MEDIUM",
            "signals": ["GearPosition","TorqueConverterSlip","TransmissionTemp"]},
        "BODY_CONTROL":       {"arb_id": "0x3C0", "priority": "LOW",
            "signals": ["HeadlightsOn","AmbientTemp","WipersActive"]},
    })


@app.route("/api/parse", methods=["POST"])
def trigger_parse():
    body     = request.json or {}
    asc_path = body.get("asc_path")
    dbc_path = body.get("dbc_path", "dbc/vehicle.dbc")
    if not asc_path:
        return jsonify({"error": "asc_path required"}), 400

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "edge"))
    try:
        from edge.parser import CANParser
        from edge.filter import CANFilter
        frames = CANParser(dbc_path).parse_asc_file(asc_path)
        clean  = CANFilter().filter_frames(frames)
        return jsonify({"total_frames": len(frames), "clean_frames": len(clean)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/status")
def status():
    return jsonify({
        "kafka":    "localhost:9092",
        "influxdb": "localhost:8086",
        "grafana":  "localhost:3000",
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)