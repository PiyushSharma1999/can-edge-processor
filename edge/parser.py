"""
parser.py — CAN Message Parser (Core EC2)

DECODING FORMULA:
  physical = (raw_value x scale) + offset

EXAMPLE — EngineRPM from bytes [0xE8, 0x03]:
  little-endian uint16: 0x03E8 = 1000
  physical = (1000 x 1.0) + 0.0 = 1000 RPM

EXAMPLE — CoolantTemp from byte [0x82]:
  uint8: 0x82 = 130
  physical = (130 x 1.0) + (-40) = 90 degC

EXAMPLE — LateralAccel (SIGNED) from byte [0xCE]:
  int8: 0xCE = -50 (two's complement)
  physical = (-50 x 0.1) + 0.0 = -5.0 m/s2
"""

import can
import cantools
import json
import logging
import os
from dataclasses import dataclass, field, asdict
from typing import Optional

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [parser] %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@dataclass
class DecodedFrame:
    timestamp: float
    arbitration_id: int
    arbitration_id_hex: str
    message_name: str
    dlc: int
    raw_data_hex: str
    signals: dict = field(default_factory=dict)
    is_error_frame: bool = False
    decode_error: Optional[str] = None

    def to_json(self):
        return json.dumps(asdict(self))


class CANParser:
    def __init__(self, dbc_path: str):
        log.info(f"Loading DBC: {dbc_path}")
        self.db = cantools.database.load_file(dbc_path)
        self.message_map = {msg.frame_id: msg for msg in self.db.messages}

        log.info(f"DBC loaded: {len(self.db.messages)} messages, "
                 f"{sum(len(m.signals) for m in self.db.messages)} signals")

        for msg in self.db.messages:
            log.info(f"  0x{msg.frame_id:03X} {msg.name:<25} "
                     f"DLC={msg.length} signals={[s.name for s in msg.signals]}")

    def decode_message(self, msg: can.Message) -> DecodedFrame:
        frame = DecodedFrame(
            timestamp=msg.timestamp,
            arbitration_id=msg.arbitration_id,
            arbitration_id_hex=f"0x{msg.arbitration_id:03X}",
            message_name="UNKNOWN",
            dlc=msg.dlc,
            raw_data_hex=" ".join(f"{b:02X}" for b in msg.data),
            is_error_frame=msg.is_error_frame,
        )

        if msg.is_error_frame:
            frame.message_name = "ERROR_FRAME"
            return frame

        if msg.arbitration_id not in self.message_map:
            frame.decode_error = f"No DBC entry for ID 0x{msg.arbitration_id:03X}"
            return frame

        frame.message_name = self.message_map[msg.arbitration_id].name

        try:
            decoded = self.db.decode_message(msg.arbitration_id, msg.data,
                                             decode_choices=True)
            # FIX: original code had `vars` typo, should be `v`
            frame.signals = {k: float(v) if not isinstance(v, str) else v
                             for k, v in decoded.items()}
        except Exception as e:
            frame.decode_error = str(e)

        return frame

    def parse_asc_file(self, asc_path: str) -> list:
        if not os.path.exists(asc_path):
            raise FileNotFoundError(f"ASC file not found: {asc_path}")

        log.info(f"Parsing: {asc_path}")
        frames, total, decoded_ok, error_frames = [], 0, 0, 0

        with can.ASCReader(asc_path) as reader:
            for msg in reader:
                total += 1
                frame = self.decode_message(msg)
                frames.append(frame)
                if frame.is_error_frame:
                    error_frames += 1
                elif frame.decode_error is None:
                    decoded_ok += 1

        log.info(f"Total={total} | Decoded={decoded_ok} | Errors={error_frames}")
        return frames

    def print_signal_summary(self, frames: list):
        signal_data = {}
        for frame in frames:
            if frame.is_error_frame or frame.decode_error:
                continue
            for sig_name, value in frame.signals.items():
                key = f"{frame.message_name}.{sig_name}"
                if isinstance(value, (int, float)):
                    signal_data.setdefault(key, []).append(value)

        unit_map = {}
        for msg in self.db.messages:
            for sig in msg.signals:
                unit_map[f"{msg.name}.{sig.name}"] = sig.unit or ""

        print("\n" + "─" * 70)
        print(f"{'Signal':<40} {'Min':>8} {'Max':>8} {'Last':>8} Unit")
        print("─" * 70)
        for key in sorted(signal_data):
            v = signal_data[key]
            print(f"{key:<40} {min(v):>8.2f} {max(v):>8.2f} "
                  f"{v[-1]:>8.2f} {unit_map.get(key, '')}")
        print("─" * 70)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--asc",     required=True)
    ap.add_argument("--dbc",     default="dbc/vehicle.dbc")
    ap.add_argument("--out",     default=None)
    ap.add_argument("--summary", action="store_true")
    args = ap.parse_args()

    parser = CANParser(args.dbc)
    frames = parser.parse_asc_file(args.asc)

    if args.summary:
        parser.print_signal_summary(frames)

    if args.out:
        with open(args.out, 'w') as f:
            json.dump([asdict(fr) for fr in frames], f, indent=2)