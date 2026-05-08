# Smart Vault Security Monitor

An IoT pipeline that monitors a secure enclosure for unauthorized entry and physical tampering. The system uses an Arduino Mega 2560 with a photoresistor and tilt sensor to detect light changes and movement, processes the data through a multi-stage pipeline, stores it in InfluxDB, and visualizes it in Grafana with machine learning anomaly detection.

---

## Architecture

```
[Arduino Mega 2560]
  Photoresistor (light level 0-1023)
  Tilt Switch   (movement 0/1)
  RGB LED       (local status feedback)
  Active Buzzer (local alert)
        |
     USB Serial (9600 baud)
        |
[scripts/gateway.py  -  Edge Gateway]
  - Reads JSON from Arduino every second
  - Adds UTC timestamp
  - Validates data (6 rules)
  - Simulates failures (duplicate, delay, out-of-range)
  - Publishes to MQTT broker via QoS 1
        |
      MQTT (eclipse-mosquitto, port 1883)
        |
[scripts/subscriber.py  -  MQTT Subscriber]
  - Subscribes to all vault topics
  - Validates and deduplicates records
  - Writes to InfluxDB using correct tag/field schema
        |
[InfluxDB 2.7  -  Time Series Database]
  Measurement : vault_readings
  Tags        : device, location, status
  Fields      : light, tilt, sev, uptime, loop_ms
        |
   +--------------------+
   |                    |
[Grafana]          [scripts/ml_anomaly.py]
  4 dashboards       Isolation Forest model
  Live light panel   Writes vault_anomalies
  Status pie chart   back to InfluxDB
  Severity timeline
  Anomaly scores
```

### Why each component was chosen

**Protocol - MQTT over REST or CoAP:**
MQTT uses a publish/subscribe model which is ideal here because the Arduino does not need to know who is consuming its data. It publishes once and any number of subscribers can receive it. The lightweight binary framing and QoS 1 delivery guarantee make it suitable for a constrained device sending data at 1 Hz over a local network. REST would require the device to initiate HTTP connections on every reading, adding latency and complexity. CoAP is designed for lossy networks which is not a concern here.

**Processing layer - Python gateway on laptop:**
The Arduino Mega has no WiFi capability so the laptop acts as an edge gateway. This is the correct IoT architecture pattern: constrained device handles only sensing and local feedback, while a more capable edge node handles protocol translation, validation, and failure handling.

**Database - InfluxDB 2.7:**
InfluxDB is a time series database purpose-built for append-heavy workloads with timestamp indexing. Every record in this project has a timestamp as its primary dimension. Standard relational databases are not optimized for time-range queries across millions of timestamped rows. InfluxDB also natively supports the tag/field schema which maps directly to IoT concepts: tags are low-cardinality metadata used for filtering (device, location, status), fields are the actual measurements (light, tilt, sev).

**ML model - Isolation Forest:**
Isolation Forest is unsupervised, meaning it does not require labelled training data. Since we cannot pre-label every anomaly in a real vault monitoring system, this is the correct choice. It works by building random trees and measuring how many splits are needed to isolate a data point. Anomalies require fewer splits because they are statistically different from the majority. A score below 0 indicates an anomaly.

**Visualization - Grafana:**
Grafana connects directly to InfluxDB and supports Flux queries natively. It provides live auto-refreshing panels which satisfy the real-time visualization requirement. It is open source and runs in Docker alongside the rest of the stack.

---

## Project Structure

```
smart-vault/
├── scripts/
│   ├── gateway.py          edge gateway - serial to MQTT
│   ├── subscriber.py       MQTT to InfluxDB writer
│   └── ml_anomaly.py       anomaly detection model
├── config/
│   └── config.yaml         central configuration
├── arduino/
│   └── smart_vault/
│       └── smart_vault.ino Arduino sketch
├── .env
├── .gitignore
└── README.md
```

---

## Setup

### Prerequisites

- Python 3.9+
- Docker Desktop
- Arduino IDE
- Arduino Mega 2560 connected via USB

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/smart-vault.git
cd smart-vault
```

### 2. Create virtual environment and install dependencies

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Set up environment variables

```bash
copy .env.example .env
```

Open `.env` and paste your InfluxDB token:
```
INFLUXDB_TOKEN=your_actual_token_here
```

### 4. Start Docker services

Create the shared network first:
```bash
docker network create smartvault-net
```

Start Mosquitto:
```bash
docker run -d --name mosquitto --network smartvault-net -p 1883:1883 -v "%CD%\mosquitto\config\mosquitto.conf:/mosquitto/config/mosquitto.conf" eclipse-mosquitto
```

Start InfluxDB:
```bash
docker run -d --name influxdb --network smartvault-net -p 8086:8086 -v "%CD%\influxdb:/var/lib/influxdb2" influxdb:2.7
```

Start Grafana:
```bash
docker run -d --name grafana --network smartvault-net -p 3000:3000 grafana/grafana
```

### 5. Configure InfluxDB

1. Open `http://localhost:8086`
2. Create account: org = `rowan-iot`, bucket = `smartvault`
3. Copy the API token into your `.env` file

### 6. Configure Grafana

1. Open `http://localhost:3000` (login: admin / admin)
2. Add data source: InfluxDB, Flux query language
3. URL = `http://influxdb:8086`
4. Enter org, token, and default bucket

### 7. Upload Arduino sketch

1. Open `arduino/smart_vault/smart_vault.ino` in Arduino IDE
2. Tools > Board > Arduino Mega or Mega 2560
3. Tools > Port > select your COM port
4. Click Upload

### 8. Run the pipeline

Open three terminals:

**Terminal 1:**
```bash
python scripts/gateway.py
```

**Terminal 2:**
```bash
python scripts/subscriber.py
```

**Terminal 3 (run once after data is collected):**
```bash
python scripts/ml_anomaly.py
```

### 9. Generate historical data (optional)

```bash
python scripts/generate_data.py
```

---

## Wiring

| Component | Arduino Pin |
|---|---|
| Photoresistor signal | A0 |
| Tilt switch | Pin 2 |
| RGB LED Red | Pin 3 via 220 ohm |
| RGB LED Green | Pin 5 via 220 ohm |
| RGB LED Blue | Pin 6 via 330 ohm |
| Active Buzzer (+) | Pin 8 |

Photoresistor: `5V -> Photoresistor -> A0 -> 10K resistor -> GND`

Tilt switch: `5V -> Tilt switch -> Pin 2 -> 10K resistor -> GND`

---

## Data Schema

### vault_readings

| Type | Key | Description |
|---|---|---|
| Tag | device | device ID e.g. vault-node-01 |
| Tag | location | physical location e.g. server-room-a |
| Tag | status | current state e.g. NORMAL, DARK_ALERT |
| Field | light | ADC value 0-1023 |
| Field | tilt | 0=stable 1=tilted |
| Field | sev | 0=normal 1=alert 2=critical |
| Field | uptime | seconds since Arduino boot |
| Field | loop_ms | actual sample interval ms |

Tags are indexed in InfluxDB for fast GROUP BY and WHERE filtering in Grafana. Fields hold the numeric measurements we aggregate and plot.

### vault_anomalies (ML output)

| Type | Key | Description |
|---|---|---|
| Tag | is_anomaly | 1=anomaly 0=normal |
| Field | anomaly_score | Isolation Forest score, negative = anomaly |
| Field | light | light value at time of reading |
| Field | sev | severity at time of reading |

---

## Failure Scenarios

| Scenario | How triggered | How handled |
|---|---|---|
| Communication failure | USB cable unplugged | SerialException caught, reconnect attempted, GATEWAY_OFFLINE published to MQTT |
| Sensor dropout | Photoresistor wire pulled | light=0 detected as SENSOR_FAULT, blue LED on device |
| Delayed transmission | Every 31st record | 3 second sleep injected, burst flush on resume |
| Duplicate message | Every 47th record | Duplicate ID detected by subscriber, flagged not written twice |
| Out-of-range value | Every 23rd record | light=9999 fails ADC range check, rejected before reaching DB |

---

## ML Model

**Model:** Isolation Forest (scikit-learn)

**Input features:** light, tilt, sev, loop_ms

**Why Isolation Forest:** Unsupervised so no labelled data is needed. Anomalies are statistically isolated faster than normal points in random decision trees. This is appropriate for IoT sensor streams where anomalies cannot be pre-labelled in advance.

**Output:**
- `anomaly_score` - continuous score, more negative = more anomalous
- `is_anomaly` - 1 if anomaly, 0 if normal (decision boundary at score = 0)

**Decision-making:** Operations team investigates when `is_anomaly=1` AND `sev>=1`. This combines ML output with rule-based severity for more reliable alerting than either method alone. TILT_DETECTED and SPIKE_AND_TILT both show 100% anomaly rates confirming the model correctly identifies physical tamper events as the most unusual readings.
