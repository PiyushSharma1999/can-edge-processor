# CAN Edge Pipeline

A working edge-computing pipeline that ingests CAN bus data from a simulated vehicle, processes it on an edge device (AWS IoT Greengrass), ships it through priority-tiered queues, and visualizes it as time-series telemetry.

This README is a learning guide. It explains the **CAN bus protocol**, **MQTT messaging**, and how each piece of this project fits together.

---

## 1. CAN Bus — what it is and why it exists

**Controller Area Network (CAN)** is a serial bus designed by Bosch in 1986 for in-vehicle communication. Before CAN, every ECU (engine control unit, ABS, transmission, body control, etc.) was wired point-to-point — a wiring nightmare. CAN replaced that with a **two-wire differential bus** that every ECU shares.

### Why CAN, not Ethernet?

| Property | CAN | Ethernet |
|---|---|---|
| Wires | 2 (CAN_H, CAN_L) | 4–8 |
| Determinism | Priority-based arbitration, no collisions | Best-effort, possible collisions |
| Error detection | CRC + bit-stuffing + ack-bit + form-check | CRC only |
| Typical speed | 500 kbps (HS-CAN), 1 Mbps (CAN FD) | 100 Mbps – 10 Gbps |
| Cost per node | Cents | Dollars |
| Designed for | Real-time control under EMI | Bulk data |

A misfiring spark plug needs a response in microseconds, not milliseconds. CAN guarantees the highest-priority message gets through first, every time.

### The frame structure

Every CAN message has the same skeleton:

```
┌────────────────┬─────┬──────────────────┬─────┬─────┐
│ Arbitration ID │ DLC │   Data (0–8 B)   │ CRC │ ACK │
└────────────────┴─────┴──────────────────┴─────┴─────┘
```

- **Arbitration ID** (11 or 29 bits) — identifies the *message*, not the sender. Also sets priority: **lower number = higher priority**.
- **DLC** (Data Length Code) — 0 to 8 bytes of payload (CAN FD allows 64).
- **Data** — the actual bytes, packed however the system designer chose.
- **CRC + ACK** — error detection and acknowledgment.

### Priority by arbitration

When two ECUs transmit at the same time, the one with the lower ID "wins" — it keeps transmitting, the other backs off and retries. No collisions, no leader election, just physics (dominant 0 beats recessive 1 on the wire).

This project's IDs (see [client/simulator.py:19-22](client/simulator.py#L19-L22)):

| ID | Message | Why this priority |
|---|---|---|
| `0x0C8` | Engine status | Safety-critical, 100 Hz |
| `0x1A4` | Vehicle dynamics (speed, brake, ABS) | Safety-critical, 100 Hz |
| `0x2B0` | Transmission | Important, 50 Hz |
| `0x3C0` | Body control (headlights, ambient temp) | Comfort, 10 Hz |

Notice we mirror this priority in **SQS queue tiers** later. Engine data shouldn't queue behind headlight state.

### Encoding: raw bytes ↔ engineering values

CAN data is just bytes. To turn `[0xE8, 0x03]` into "1000 RPM" you need a **decoder spec**. That spec lives in a **DBC file** — a text format from Vector that maps signals onto bytes:

```
BO_ 200 ENGINE_STATUS: 8 ECU_ENGINE
 SG_ EngineRPM       : 0|16@1+ (1,0)     [0|6500]  "rpm"
 SG_ CoolantTemp     : 16|8@1+ (1,-40)   [-40|215] "degC"
```

Reading it:
- `BO_ 200` → message with ID 200 (= `0x0C8`)
- `EngineRPM : 0|16@1+ (1,0)` → bits 0–15, little-endian, unsigned, scale × 1, offset 0
- `CoolantTemp : 16|8@1+ (1,-40)` → bits 16–23, unsigned byte, scale × 1, **offset −40** (so byte value 130 → 90 °C)

**Decoding formula**: `physical = (raw × scale) + offset`
**Encoding formula** (the inverse): `raw = (physical − offset) / scale`

The encoder side lives in [client/simulator.py:81-155](client/simulator.py#L81-L155) (`CANFrameBuilder`). The decoder uses `cantools.database.decode_message()` in [edge/parser.py:46-76](edge/parser.py#L46-L76).

### Why the offset trick

Coolant temp ranges from −40 °C (winter start) to 215 °C (overheat). That's 255 values, fits in one byte if you shift: store `temp + 40` as a `uint8`. Same trick for accelerations (signed: −12.7 to +12.7 m/s² stored as int8 × 0.1). Bytes are precious on a 500 kbps bus shared by 50 ECUs.

### Error frames

If a node detects a bit-stuffing violation, bad CRC, or form error, it transmits an **error frame** — 6+ dominant bits in a row, which violates the framing rule and tells every node to discard the in-flight message. Healthy CAN traffic has ~0 error frames; persistent errors mean a wiring or termination problem.

In `.asc` log files (Vector's ASCII CAN log format), an error frame looks like:
```
   1.234567 1 ErrorFrame
```

The filter in [edge/filter.py:99-105](edge/filter.py#L99-L105) drops these by default. The simulator injects them at a configurable rate ([client/simulator.py:197-198](client/simulator.py#L197-L198)) so you can see the filter working.

### The .asc file format

This project uses Vector's `.asc` text format as the wire format between simulator and edge:

```
date Mon Jan 1 00:00:00 2024
base hex  timestamps absolute
internal events logged
// version 8.0.0
Begin Triggerblock
   0.001000 1  0C8             Rx   d 8 E8 03 82 32 7F 75 00 00
   0.002000 1  1A4             Rx   d 8 80 7D 00 00 00 00 00 00
```

Each line: `timestamp channel arbitration-id direction "d" DLC data-bytes`. Library-agnostic, human-readable, and `python-can` parses it via `can.ASCReader`.

---

## 2. MQTT — what it is and why this project uses it

**MQTT (Message Queuing Telemetry Transport)** is a lightweight pub/sub messaging protocol designed in 1999 for oil pipeline monitoring over satellite links. It's now the de facto IoT protocol.

### The model

Three actors:
- **Publishers** send messages to **topics** (strings like `clients/IoT-CAN-Client`).
- **Subscribers** subscribe to topics and get pushed messages.
- A **broker** sits in the middle, routes messages from publishers to subscribers. Publishers and subscribers don't know about each other.

```
   ┌─────────────┐   publish    ┌────────┐   push   ┌─────────────┐
   │  Publisher  │ ───────────► │ Broker │ ───────► │ Subscriber  │
   └─────────────┘   "topic/x"  └────────┘          └─────────────┘
                                    ▲
                                    │ subscribe "topic/x"
                                    │
```

### Why pub/sub for IoT?

- **Decoupling** — the simulator doesn't need to know who consumes its data. Add a second consumer (e.g., a recorder) without touching the publisher.
- **Tiny client footprint** — the protocol is 12 control packet types and runs on microcontrollers with kilobytes of RAM.
- **Persistent connection** — one TCP socket stays open; the broker pushes when there's news. No polling.
- **QoS levels** — 0 (fire and forget), 1 (at least once, with PUBACK), 2 (exactly once, four-way handshake). Trade reliability vs. overhead.
- **Wildcards** — `clients/+/data` matches one segment; `clients/#` matches any depth. Powerful for hierarchical topic schemes.

### How this project uses MQTT

Two hops in the message path:

```
                                  bridge (LocalMqtt → Pubsub)
   Simulator ──mTLS/8883──►  Moquette  ─────────────────────►  IPC ──► Edge component
   (client EC2)             (broker on core EC2)                       (watcher.py)
```

1. **Client → Core broker (real MQTT)**
   The simulator publishes a .asc file (base64-encoded JSON) to topic `clients/IoT-CAN-Client` over **mTLS on port 8883**. Authentication is via X.509 device certificates — the Auth component on the core checks the cert maps to a thing it has a policy for.

2. **Core broker → Edge component (Greengrass IPC)**
   The **MQTT Bridge** component watches for messages on `clients/IoT-CAN-Client` from `LocalMqtt` and re-publishes them onto Greengrass IPC PubSub. The edge component (`watcher.py`) subscribes via IPC and never touches MQTT directly. This separation means the edge code doesn't need TLS certs or a broker client — Greengrass handles it.

### Why mTLS for IoT MQTT?

Username/password is fine for an internal service. For devices in the field, **mutual TLS** is the standard:
- Server proves identity with a TLS cert (you trust the AWS root CA).
- Client proves identity with its own cert (the broker checks the client cert against its allow-list).
- The cert itself *is* the identity. Revoke the cert → device is locked out instantly.

In this project, `aws iot create-keys-and-certificate` generated `device.pem.crt` + `private.pem.key`, the client EC2 holds the private key, and the IoT thing policy (`GreengrassV2IoTThingPolicy`) authorizes that thing to connect.

---

## 3. Architecture

```
┌──────────────────────────────────┐         ┌─────────────────────────────────────┐
│      Client EC2 (Ubuntu)         │         │   Core EC2 — AWS IoT Greengrass     │
│   "IoT-CAN-Client" thing          │         │   "IoT-CAN-Core" thing              │
│                                  │         │                                     │
│  ┌────────────────────────────┐  │  mTLS   │  ┌──────────────────────────────┐  │
│  │  simulator.py              │  │  8883   │  │  Moquette (MQTT broker)      │  │
│  │  - VehicleState model      │  │ ──────► │  │  + Client Device Auth        │  │
│  │  - CANFrameBuilder         │  │         │  │  + MQTT Bridge → Pubsub      │  │
│  │  - generate .asc file      │  │         │  └────────────┬─────────────────┘  │
│  │  - publisher.py (discovery │  │         │               │ Greengrass IPC      │
│  │    + mTLS publish)         │  │         │  ┌────────────▼─────────────────┐  │
│  └────────────────────────────┘  │         │  │  com.canpipeline.CANProcessor│  │
└──────────────────────────────────┘         │  │  (custom Greengrass         │  │
                                              │  │   component, watcher.py)    │  │
                                              │  │                              │  │
                                              │  │  watcher → parser → filter   │  │
                                              │  │           → sqs_producer     │  │
                                              │  └────────────┬─────────────────┘  │
                                              └───────────────┼────────────────────┘
                                                              │ boto3 send_message_batch
                                                              ▼
                                              ┌───────────────────────────────────┐
                                              │  AWS SQS (3 priority queues)      │
                                              │  ┌─────────┐ ┌─────────┐ ┌──────┐ │
                                              │  │  HIGH   │ │ MEDIUM  │ │ LOW  │ │
                                              │  │ engine  │ │  trans  │ │ body │ │
                                              │  │ vehicle │ │         │ │      │ │
                                              │  └────┬────┘ └────┬────┘ └──┬───┘ │
                                              └───────┼───────────┼─────────┼─────┘
                                                      │           │         │
                                                      └───────────┼─────────┘
                                                                  ▼
                                              ┌───────────────────────────────────┐
                                              │     Cloud EC2                     │
                                              │                                   │
                                              │  ┌────────────────────────────┐   │
                                              │  │  sqs_consumer.py           │   │
                                              │  │  - long-poll all 3 queues  │   │
                                              │  │  - convert to Influx Point │   │
                                              │  └─────────────┬──────────────┘   │
                                              │                ▼                  │
                                              │  ┌────────────────────────────┐   │
                                              │  │  InfluxDB 2.x (Docker)     │   │
                                              │  │  bucket = can_data         │   │
                                              │  └─────────────┬──────────────┘   │
                                              │                ▼                  │
                                              │  ┌────────────────────────────┐   │
                                              │  │  Grafana (Docker, :3000)   │   │
                                              │  └────────────────────────────┘   │
                                              └───────────────────────────────────┘
```

### Why each piece exists

#### Client side ([client/](client/))

- **`simulator.py`** — stands in for a real ECU. Runs a 60-second cycle of accelerate → cruise → brake, encodes engineering values (RPM, km/h, °C) into raw CAN bytes at correct refresh rates (100 Hz engine, 10 Hz body), and writes a `.asc` file.
- **`publisher.py`** — uses **Greengrass discovery** (calls AWS IoT to find the core's IP and CA cert) then opens an **mTLS** connection to the core's Moquette broker and publishes the base64'd `.asc` as a JSON payload to `clients/IoT-CAN-Client`.

Why send the whole file instead of streaming individual frames? Two reasons:
1. **Batching** — 2600 frames sent as one MQTT message is much cheaper than 2600 publishes.
2. **Atomic processing** — the edge sees a full data window and can compute stats over it.

#### Core side (this repo's [edge/](edge/) directory)

- **Moquette** (Greengrass-managed MQTT broker on port 8883) — accepts the client's mTLS connection.
- **Client Device Auth** — checks the client's cert against a policy (only `IoT-CAN-Client` can publish to `clients/IoT-CAN-Client`).
- **MQTT Bridge** — forwards messages from `LocalMqtt` (the broker) to `Pubsub` (Greengrass IPC, an internal channel between components on the same core).
- **`watcher.py`** — the custom Greengrass component. Subscribes via IPC, decodes the base64 payload back to a `.asc` file on disk, then runs the pipeline:
  - **`parser.py`** — uses `cantools` + DBC to decode raw bytes into named signals (`{EngineRPM: 1234.5, CoolantTemp: 90.2, ...}`).
  - **`filter.py`** — drops error frames, range-checks signals (e.g., RPM in 0–6500), drops bad rows.
  - **`sqs_producer.py`** — batches signals by priority and pushes to one of three SQS queues using `send_message_batch`.

#### Cloud side ([cloud/](cloud/))

- **`sqs_consumer.py`** — long-polls all three queues, converts JSON frames into **InfluxDB Points** (one point per signal, tagged with message name and arbitration ID), writes them.
- **InfluxDB** — time-series database; cheap inserts, efficient range queries by time + tag.
- **Grafana** — queries Influx via Flux, renders panels.

### Why three SQS queues?

We mirror CAN bus priority in our cloud transport. If we used one queue:
- A burst of low-priority body messages could clog up an autoscaling consumer, delaying engine telemetry.
- We couldn't apply different SLOs or different consumer-fleet sizes per priority.

Three queues = three independent FIFOs we can scale/throttle/route separately. The mapping lives in [edge/sqs_producer.py:33-38](edge/sqs_producer.py#L33-L38).

---

## 4. End-to-end message journey

A single 10-second simulation produces ~2600 CAN frames. Here's what happens to one of them:

1. **Generation** — `simulator.py` ticks `VehicleState` forward 1 ms at a time. At t=0.010 s it emits an Engine frame: speed 30 km/h, RPM 1234, throttle 12%. `CANFrameBuilder.encode_engine()` packs this into 8 bytes:
   ```
   ID=0x0C8  data=[D2 04 7E 0C 1F 75 00 00]
                   ─┬─── ─── ─── ─── ───
                    │    │   │   │   └── padding
                    │    │   │   └────── fuel pressure raw
                    │    │   └────────── engine load raw
                    │    │   throttle %
                    │    coolant_temp + 40
                    RPM (little-endian uint16: 0x04D2 = 1234)
   ```

2. **Serialization** — frames written to `data/simulated.asc` via `can.ASCWriter`.

3. **Base64 + JSON** — entire file (~150 KB) read, base64-encoded (~200 KB), wrapped:
   ```json
   {"encoded": "ZGF0ZSBNb24g...", "filename": "simulated.asc", "request_id": "uuid"}
   ```

4. **mTLS publish** — `publisher.py` discovers the core, opens TLS on `<core-ip>:8883` with the device cert, publishes QoS 1. Broker sends PUBACK once persisted.

5. **Bridge → IPC** — Moquette delivers to the bridge subscription; bridge re-publishes on Greengrass IPC topic `clients/IoT-CAN-Client`.

6. **Edge processing** — `watcher.py`'s IPC stream handler fires. It writes the `.asc` to `/tmp/can-data/simulated.asc`, then:
   ```
   parser.parse_asc_file() → 2600 DecodedFrame objects
   filter.filter_frames()  → 2600 kept (no errors, all in range)
   sqs_producer.send_frames() → batched by priority, send_message_batch
   ```

7. **SQS** — our engine frame lands in `can-high-priority`. The message body is the full DecodedFrame JSON; message attributes carry `message_name` and `arbitration_id` for cheap filtering.

8. **Cloud consume** — `sqs_consumer.py` long-polls, receives, deletes. For each signal in the frame, it creates an Influx Point:
   ```
   ENGINE_STATUS,signal=EngineRPM,arbitration_id=0x0C8 value=1234.0 1716595260000000000
   ```
   That's the **line protocol** Influx natively speaks.

9. **Grafana** — Flux query pulls the last hour of `ENGINE_STATUS` measurements, plots EngineRPM and VehicleSpeed against each other.

---

## 5. What "edge computing" buys us here

If we sent every raw CAN frame to the cloud, we'd:
- Pay egress on 100 Hz × 6 signals × 24 h = millions of frames per vehicle per day.
- Process raw bytes in the cloud, where we then need the DBC, which is IP we may not want to ship.
- See ~250 ms latency end-to-end (round-trip to AWS region).

By doing **decode + filter on the edge**, we:
- Drop error frames and out-of-range garbage *before* paying to transmit them.
- Send decoded engineering values (cheap, small JSON) instead of raw byte blobs.
- Keep the DBC + decoding logic on a single trusted device.
- Could compute alerts locally (e.g., overspeed) without cloud round-trip — the cloud just stores history.

This is the same architectural reason Tesla, Waymo, and modern fleet platforms put substantial compute next to the bus.

---

## 6. Repo layout

```
client/
  simulator.py      ← generate .asc file (VehicleState + CANFrameBuilder)
  publisher.py      ← mTLS publish over Greengrass discovery

edge/
  watcher.py        ← Greengrass component entrypoint (IPC subscriber + pipeline)
  parser.py         ← DBC-based decode (cantools)
  filter.py         ← error/range/unknown-id filtering
  sqs_producer.py   ← priority routing to 3 SQS queues

cloud/
  sqs_consumer.py   ← long-poll → InfluxDB Points
  app.py            ← optional Flask status API

dbc/
  vehicle.dbc       ← signal definitions (the encode/decode contract)

docker-compose.yml  ← InfluxDB + Grafana for the cloud box
```

---

## 7. Glossary

| Term | Meaning |
|---|---|
| **ECU** | Electronic Control Unit — a microcontroller in a vehicle (engine, ABS, etc.) |
| **DBC** | Vector's database format mapping CAN signals to bytes |
| **Arbitration ID** | The "address" of a CAN message; also sets bus priority |
| **DLC** | Data Length Code — payload byte count (0–8 for classic CAN) |
| **MTU** | Maximum Transmission Unit — classic CAN = 8 bytes data |
| **MQTT** | Pub/sub messaging protocol over TCP |
| **QoS** | MQTT Quality of Service: 0 fire-and-forget, 1 at-least-once, 2 exactly-once |
| **mTLS** | Mutual TLS — both client and server present certs |
| **Greengrass** | AWS's edge runtime — runs IoT components on local hardware |
| **Component** | A unit of code Greengrass deploys (recipe + artifacts) |
| **IPC** | Greengrass's local inter-process channel between components |
| **TES** | Token Exchange Service — Greengrass's way of giving components AWS creds |
| **SQS** | Simple Queue Service — managed message queue |
| **InfluxDB** | Time-series database |
| **Flux** | InfluxDB 2.x query language |
