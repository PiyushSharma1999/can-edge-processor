"""
filter.py - CAN Frame Filter (Edge Processor)

ERROR FRAMES - WHY THEY EXISTS:
    Any CAN node can signal an error by transmitting 6 dominant bits in a row
    (violates bit-stuffing rule). Causes:
    - Bit stuffing violation (corrunpted wire data)
    - Form errors (bad frame delimiters)
    - ACK errors (nobody acknowledged)
    - Physical issues (bad wirinhm EMI, no termination resistor)

    Error counter state machine:
        TEC/REC 0-127 -> Error Active (normal, can transmit error flags)
        TEC/REC 128+  -> Error Passive (limited error signaling)
        TEC 256+      -> Bus Off (node stops transmitting entirely)
    
    In .asc files: 0.123456 1 ErrorFrame
"""

import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

@dataclass
class FilterResult:
    frame: object
    kept: bool
    rejection_reason: Optional[str] = None

class CANFilter:
    def __init__(self, drop_error_frames=True, drop_unknown_ids=False,
                 validate_ranges=True, range_tolerance_pct=5.0):
        self.drop_error_frames = drop_error_frames
        self.drop_unknown_ids  = drop_unknown_ids
        self.validate_ranges   = validate_ranges
        self.tolerance         = range_tolerance_pct / 100.0
        
        # signal_name → (min, max, unit)
        self.signal_ranges = {
            "EngineRPM":           (0,     6500,    "rpm"),
            "CoolantTemp":         (-40,   215,     "degC"),
            "ThrottlePos":         (0,     100,     "%"),
            "EngineLoad":          (0,     100,     "%"),
            "FuelPressure":        (0,     765,     "kPa"),
            "VehicleSpeed":        (0,     327.67,  "km/h"),
            "BrakePressure":       (0,     25.5,    "MPa"),
            "LateralAccel":        (-12.7, 12.7,    "m/s2"),
            "LongitudinalAccel":   (-12.7, 12.7,    "m/s2"),
            "GearPosition":        (0,     8,       ""),
            "TorqueConverterSlip": (0,     100,     "%"),
            "TransmissionTemp":    (-40,   215,     "degC"),
            "AmbientTemp":         (-40,   87.5,    "degC"),
            "IsoTp_PCI":          (0, 255, ""),
            "UDS_ServiceID":      (0, 255, ""),
            "ResponseByte1":      (0, 255, ""),
            "ResponseByte2":      (0, 255, ""),
            "ResponseByte3":      (0, 255, ""),
            "ResponseByte4":      (0, 255, ""),
            "Padding0":           (0, 255, ""),
            "Padding1":           (0, 255, ""),
            "Request_ServiceID":  (0, 255, ""),
            "Request_SubFunction":(0, 255, ""),
            "RequestByte1":       (0, 255, ""),
            "RequestByte2":       (0, 255, ""),
        }

        self.stats = {"total": 0, "kept": 0,
                      "dropped_error_frame": 0,
                      "dropped_unknown_id": 0,
                      "dropped_range_violation": 0}
    
    def filter_frame(self, frame) -> FilterResult:
        self.stats["total"] += 1

        # Stage 1: error frames
        if frame.is_error_frame:
            if self.drop_error_frames:
                self.stats["dropped_error_frame"] += 1
                return FilterResult(frame, kept=False,
                    rejection_reason="error_frame: physical layer error")
            return FilterResult(frame, kept=True)

        # Stage 2: unknown IDs
        if frame.decode_error and "No DBC entry" in frame.decode_error:
            if self.drop_unknown_ids:
                self.stats["dropped_unknown_id"] += 1
                return FilterResult(frame, kept=False,
                    rejection_reason=f"unknown_id: {frame.arbitration_id_hex}")
            return FilterResult(frame, kept=True)

        # Stage 3: range validation
        if self.validate_ranges and frame.signals:
            for sig_name, value in frame.signals.items():
                if sig_name not in self.signal_ranges:
                    continue
                if isinstance(value, str):
                    continue
                min_v, max_v, unit = self.signal_ranges[sig_name]
                tol = (max_v - min_v) * self.tolerance
                if value < (min_v - tol) or value > (max_v + tol):
                    self.stats["dropped_range_violation"] += 1
                    return FilterResult(frame, kept=False,
                        rejection_reason=f"range_violation: {sig_name}={value:.2f} "
                                         f"outside [{min_v},{max_v}] {unit}")

        self.stats["kept"] += 1
        return FilterResult(frame, kept=True)

    def filter_frames(self, frames: list) -> list:
        results = [self.filter_frame(f) for f in frames]
        kept    = [r.frame for r in results if r.kept]
        dropped = [r for r in results if not r.kept]
        log.info(f"[filter] kept={len(kept)} dropped={len(dropped)}")
        return kept

    def print_stats(self):
        s = self.stats
        print(f"\n── Filter Stats ──────────────────────────────")
        print(f"  Total   : {s['total']}")
        print(f"  Kept    : {s['kept']}")
        print(f"  Errors  : {s['dropped_error_frame']}")
        print(f"  Unknown : {s['dropped_unknown_id']}")
        print(f"  Range   : {s['dropped_range_violation']}")
        print(f"──────────────────────────────────────────────\n")