import can
import math
import os
import random
import struct
import time
import json
import argparse
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False

# Arbitration IDs — lower = higher priority
MSG_ENGINE       = 0x0C8   # 200 decimal — highest priority
MSG_VEHICLE_DYN  = 0x1A4   # 420 decimal
MSG_TRANSMISSION = 0x2B0   # 688 decimal
MSG_BODY_CONTROL = 0x3C0   # 960 decimal — lowest priority

class VehicleState:
    """Simulate accelaration -> cruise -> brake cycle."""
    def __init__(self):
        self.time = 0.0
        self.speed_kmh = 0.0
        self.rpm = 800.0
        self.throttle_pct = 0.0
        self.coolant_temp = 20.0
        self.gear = 0
        self.brake_pressure = 0.0
        self.abs_active = False
        self.lateral_accel = 0.0
        self.long_accel = 0.0
        self.headlights = False
        self.ambient_temp = 22.0
    
    def update(self, dt):
        self.time += dt
        cycle_time = self.time % 60.0

        if cycle_time < 20:
            # Accelerating
            self.throttle_pct = min(80.0, self.throttle_pct + dt * 4)
            self.speed_kmh = min(120.0, self.speed_kmh + dt * 3.0)
            self.gear = max(1, min(5, int(self.speed_kmh / 25) + 1))
            self.long_accel = 2.5
            self.brake_pressure = 0.0
            self.abs_active = False
        elif cycle_time < 40:
            # Cruising
            self.throttle_pct = 35.0
            self.speed_kmh = 120.0 + math.sin(cycle_time * 0.5) * 2
            self.gear = 5
            self.long_accel = 0.0
            self.lateral_accel = math.sin(cycle_time * 0.3) * 3.0
        else:
            # Braking
            self.throttle_pct = max(0.0, self.throttle_pct - dt * 8)
            self.speed_kmh = max(0.0, self.speed_kmh - dt * 4.0)
            self.brake_pressure = 8.0
            self.long_accel = -3.5
            self.abs_active = self.speed_kmh > 10 and random.random() < 0.3
            self.gear = max(1, min(5, int(self.speed_kmh / 25) + 1))
            if self.speed_kmh < 1.0:
                self.gear = 0

        self.rpm = 800 + self.throttle_pct * 55 + self.speed_kmh * 10
        self.rpm = min(6500, self.rpm) + random.gauss(0, 20)

        if self.coolant_temp < 90:
            self.coolant_temp += dt * 0.3
        else:
            self.coolant_temp = 90 + math.sin(self.time * 0.1) * 2

        self.headlights = self.time > 30
    

class CANFrameBuilder:
    """
    Encodes engineering calues -> raw CAN bytes
    Formula: raw = (physical - offset) / scale
    """

    @staticmethod
    def encode_engine(state):
        """
        ENGINER_STATUS - 0x0C8
        Bytes: [0-1]=RPM, [2]=CoolantTemp, [3]=Throttle,
               [4]=EngineLoad, [5]=FuelPressure, [6]=CheckEngineLight
        """
        rpm_raw     = int(max(0, min(65535, state.rpm)))
        coolant_raw = int(max(0, min(255, state.coolant_temp + 40)))
        throttle_raw = int(max(0, min(100, state.throttle_pct)))
        load_raw    = int(max(0, min(255, state.throttle_pct / 0.392)))
        fuel_raw    = int(max(0, min(255, 350 / 3.0)))
        cel_byte    = 0x01 if state.coolant_temp > 110 else 0x00

        data = struct.pack('<H', rpm_raw) + bytes([
            coolant_raw, throttle_raw, load_raw, fuel_raw, cel_byte, 0x00
        ])
        return can.Message(arbitration_id=MSG_ENGINE, data=data, is_extended_id=False, timestamp=state.time)
    
    @staticmethod
    def encode_vehicle_dynamics(state):
        """
        VEHICLE_DYNAMICS - 0x1A4
        Bytes: [0-1]=Speed, [2]=BrakePressure,
               [3]=LateralAccel(signed), [4]=LongAccel(signed), [5]=ABS
        """
        speed_raw = int(max(0, min(65535, state.speed_kmh / 0.005)))
        brake_raw = int(max(0, min(255, state.brake_pressure / 0.1)))
        lat_raw   = int(max(-127, min(127, state.lateral_accel / 0.1)))
        lon_raw   = int(max(-127, min(127, state.long_accel / 0.1)))
        abs_byte  = 0x01 if state.abs_active else 0x00

        data = struct.pack('<H', speed_raw) + bytes([
                    brake_raw & 0xFF,
                    lat_raw & 0xFF,
                    lon_raw & 0xFF,
                    abs_byte,
                    0x00,
                    0x00   # padding to reach DLC=8
                ])
        return can.Message(arbitration_id=MSG_VEHICLE_DYN, data=data,
                           is_extended_id=False, timestamp=state.time)
    
    @staticmethod
    def encode_transmission(state):
        """
        TRANSMISSION_STATUS — 0x2B0, DLC=4 (not all messages use 8 bytes)
        """
        slip_raw      = max(0, min(100, int(random.gauss(20, 5))))
        trans_temp_raw = int(max(0, min(255, state.coolant_temp - 5 + 40)))
        gear_byte     = state.gear & 0x0F

        data = bytes([gear_byte, slip_raw, trans_temp_raw, 0x00])
        return can.Message(arbitration_id=MSG_TRANSMISSION, data=data,
                           is_extended_id=False, timestamp=state.time)
    
    @staticmethod
    def encode_body_control(state):
        """
        BODY_CONTROL — 0x3C0, DLC=2
        Shows bit-packing: 6 boolean flags in one byte.
        """
        flags = 0
        flags |= (int(state.headlights) << 2)  # HeadlightsOn at bit 2
        ambient_raw = int((state.ambient_temp - (-40)) / 0.5) & 0xFF

        data = bytes([flags, ambient_raw])
        return can.Message(arbitration_id=MSG_BODY_CONTROL, data=data,
                           is_extended_id=False, timestamp=state.time)
    
    @staticmethod
    def make_error_frame_line(timestamp):
        return f"   {timestamp:.6f}  1  ErrorFrame\n"
    

def generate_asc_file(output_path, duration_seconds, error_rate):
    """
    Generate realistic .asc log file.

    Transmission rates (matches real ECU behavior):
        Engine:         100 Hz - safety critical
        Vehicle dyn:    100 Hz - ABS need fast data
        Transmission:   50 Hz
        Body:           10 Hz - nobody needs headlight state at 100 Hz
    """
    state = VehicleState()
    builder = CANFrameBuilder()
    rates  = {'engine': 0.010, 'vehicle_dyn': 0.010,
               'transmission': 0.020, 'body': 0.100}
    next_tx = {k: 0.0 for k in rates}

    messages, error_lines = [], []
    dt, t = 0.001, 0.0

    steps = int(duration_seconds / dt)
    for i in range(steps):
        t = i * dt
        state.time = t
        state.update(dt)

        if i % 10 == 0:    # every 10ms = 100Hz — engine + vehicle
            messages.append(builder.encode_engine(state))
            messages.append(builder.encode_vehicle_dynamics(state))

        if i % 20 == 0:    # every 20ms = 50Hz — transmission
            messages.append(builder.encode_transmission(state))

        if i % 100 == 0:   # every 100ms = 10Hz — body
            messages.append(builder.encode_body_control(state))

        if random.random() < error_rate * dt:
            error_lines.append(builder.make_error_frame_line(t))
    
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    with can.ASCWriter(output_path) as writer:
        for msg in sorted(messages, key=lambda m : m.timestamp):
            writer.on_message_received(msg)
    
    if error_lines:
        with open(output_path, 'a') as f:
            f.writelines(error_lines)

    print(f"[simulator] {len(messages)} frames + {len(error_lines)} error frames → {output_path}")
    return output_path

def publish_via_mqtt(asc_path, broker, port, topic):
    if not MQTT_AVAILABLE:
        return
    client = mqtt.Client(client_id="can-simulator")
    client.connect(broker, port)
    payload = json.dumps({"asc_file": asc_path,
                          "generated_at": datetime.utcnow().isoformat()})
    client.publish(topic, payload, qos=1).wait_for_publish()
    client.disconnect()
    print(f"[simulator] Published MQTT -> {topic}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--output",      default="data/simulated.asc")
    ap.add_argument("--duration",    type=float, default=10.0)
    ap.add_argument("--errors",      type=float, default=0.02)
    ap.add_argument("--mqtt-broker", default="localhost")
    ap.add_argument("--mqtt-port",   type=int, default=1883)
    ap.add_argument("--mqtt-topic",  default="can/new-file")
    ap.add_argument("--no-mqtt",     action="store_true")
    args = ap.parse_args()

    path = generate_asc_file(args.output, args.duration, args.errors)
    if not args.no_mqtt:
        publish_via_mqtt(path, args.mqtt_broker, args.mqtt_port, args.mqtt_topic)
