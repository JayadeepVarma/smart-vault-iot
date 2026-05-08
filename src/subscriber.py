# Smart Vault - MQTT Subscriber + InfluxDB Writer
# Subscribes to all vault MQTT topics, validates incoming records,
# deduplicates them, and writes to InfluxDB.
#
# InfluxDB schema:
#   Measurement: vault_readings
#   Tags:   device, location, status  (used for filtering in Grafana)
#   Fields: light, tilt, sev, uptime, loop_ms, reading_id
#   Time:   UTC timestamp from gateway
#
# Run: python src/subscriber.py

import json
import logging
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import yaml
from dotenv import load_dotenv


ROOT_DIR    = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config" / "config.yaml"
LOG_DIR     = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

VALID_STATUSES = {
    "NORMAL", "DARK_ALERT", "DARK_AND_TILT",
    "LIGHT_SPIKE", "SPIKE_AND_TILT", "TILT_DETECTED",
    "SENSOR_FAULT", "SENSOR_FAULT_AND_TILT",
}


def load_config(path: Path) -> dict:
    load_dotenv(path.parent / ".." / ".env")
    if not path.exists():
        print(f"[FATAL] Config not found: {path}")
        sys.exit(1)
    with open(path, encoding="utf-8") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(f"[FATAL] Bad config: {e}")
            sys.exit(1)
    if "influxdb" in cfg:
        cfg["influxdb"]["token"] = os.path.expandvars(cfg["influxdb"]["token"])
    return cfg


def setup_logging() -> logging.Logger:
    log_file = LOG_DIR / "subscriber.log"
    logger   = logging.getLogger("subscriber")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    fh = RotatingFileHandler(
        log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


class InfluxWriter:
    # Tags are indexed in InfluxDB for fast GROUP BY and WHERE queries.
    # Fields hold numeric measurements we want to aggregate and plot.
    # This separation is what makes time-range queries fast in Grafana.

    def __init__(self, cfg: dict, logger: logging.Logger):
        self.icfg   = cfg["influxdb"]
        self.logger = logger
        self.client    = None
        self.write_api = None
        self.seen_ids  = set()
        self.stats = {"written": 0, "failed": 0, "duplicates_skipped": 0}

    def connect(self) -> bool:
        try:
            self.client = InfluxDBClient(
                url=self.icfg["url"],
                token=self.icfg["token"],
                org=self.icfg["org"],
            )
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            health = self.client.health()
            if health.status == "pass":
                self.logger.info(
                    f"InfluxDB connected: {self.icfg['url']} "
                    f"| org={self.icfg['org']} | bucket={self.icfg['bucket']}"
                )
                return True
            else:
                self.logger.error(f"InfluxDB health check failed: {health}")
                return False
        except Exception as e:
            self.logger.error(f"InfluxDB connection error: {e}")
            return False

    def write(self, record: dict) -> bool:
        rec_id = record.get("id")

        # Skip if we already wrote this ID
        if rec_id in self.seen_ids:
            self.stats["duplicates_skipped"] += 1
            self.logger.warning(f"[DEDUP] Skipped duplicate id={rec_id}")
            return True

        self.seen_ids.add(rec_id)
        if len(self.seen_ids) > 500:
            for _ in range(len(self.seen_ids) - 500):
                self.seen_ids.pop()

        try:
            ts_str = record.get("timestamp")
            ts = (datetime.fromisoformat(ts_str)
                  if ts_str else datetime.now(timezone.utc))

            point = (
                Point("vault_readings")
                .tag("device",   record.get("device",   "unknown"))
                .tag("location", record.get("location", "unknown"))
                .tag("status",   record.get("status",   "UNKNOWN"))
                .field("light",      int(record.get("light",      0)))
                .field("tilt",       int(record.get("tilt",       0)))
                .field("sev",        int(record.get("sev",        0)))
                .field("uptime",     int(record.get("uptime",     0)))
                .field("loop_ms",    int(record.get("loop_ms",    0)))
                .field("reading_id", int(rec_id or 0))
                .time(ts, WritePrecision.NS)
            )

            self.write_api.write(
                bucket=self.icfg["bucket"],
                org=self.icfg["org"],
                record=point,
            )
            self.stats["written"] += 1
            return True

        except Exception as e:
            self.stats["failed"] += 1
            self.logger.error(f"InfluxDB write failed for id={rec_id}: {e}")
            return False

    def close(self):
        if self.client:
            self.client.close()
        self.logger.info(
            f"InfluxDB closed | written={self.stats['written']} "
            f"failed={self.stats['failed']} "
            f"dupes_skipped={self.stats['duplicates_skipped']}"
        )


class VaultSubscriber:

    def __init__(self, cfg: dict, influx: InfluxWriter, logger: logging.Logger):
        self.cfg    = cfg["mqtt"]
        self.influx = influx
        self.logger = logger
        self.stats  = {
            "received": 0, "written": 0,
            "rejected": 0, "parse_errors": 0, "alerts": 0,
        }

        self.client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id="vault-subscriber-01",
        )
        self.client.on_connect    = self._on_connect
        self.client.on_message    = self._on_message
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, connect_flags, reason_code, properties):
        if reason_code == 0:
            self.logger.info(
                f"MQTT connected to {self.cfg['broker']}:{self.cfg['port']}"
            )
            client.subscribe(self.cfg["topic_data"],   qos=1)
            client.subscribe(self.cfg["topic_status"], qos=1)
            client.subscribe(self.cfg["topic_alert"],   qos=1)
            self.logger.info(
                f"Subscribed to: {self.cfg['topic_data']}, "
                f"{self.cfg['topic_status']}, {self.cfg['topic_alert']}"
            )
        else:
            self.logger.error(f"MQTT connect failed: reason={reason_code}")

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        self.logger.warning(f"MQTT disconnected: {reason_code}")

    def _on_message(self, client, userdata, msg):
        self.stats["received"] += 1
        topic   = msg.topic
        payload = msg.payload.decode("utf-8", errors="replace").strip()

        try:
            record = json.loads(payload)
        except json.JSONDecodeError as e:
            self.stats["parse_errors"] += 1
            self.logger.warning(f"[BAD DATA] JSON parse error on {topic}: {e}")
            return

        if topic == self.cfg["topic_data"]:
            self._handle_data(record)
        elif topic == self.cfg["topic_status"]:
            self._handle_status(record)
        elif topic == self.cfg["topic_alert"]:
            self.stats["alerts"] += 1
            self.logger.warning(f"[ALERT TOPIC] {record}")

    def _handle_data(self, record: dict):
        if record.get("_injected") == "OUT_OF_RANGE":
            self.stats["rejected"] += 1
            self.logger.warning(
                f"[BAD DATA] OUT_OF_RANGE record rejected "
                f"id={record.get('id','?')} light={record.get('light','?')}"
            )
            return

        if not self._validate(record):
            self.stats["rejected"] += 1
            return

        if self.influx.write(record):
            self.stats["written"] += 1
            dup_flag = " [DUPLICATE]" if record.get("_duplicate") else ""
            sev_flag = " !! ALERT !!" if record.get("sev", 0) >= 2 else ""
            self.logger.info(
                f"[DB WRITE] "
                f"id={record['id']:<6} "
                f"light={record['light']:<5} "
                f"tilt={record['tilt']} "
                f"status={record['status']:<25} "
                f"sev={record['sev']}"
                f"{dup_flag}{sev_flag}"
            )
            if self.stats["written"] % 100 == 0:
                self.logger.info(f"[MILESTONE] {self.stats['written']} records written")
        else:
            self.stats["rejected"] += 1

    def _handle_status(self, record: dict):
        event = record.get("event", "")
        if event == "GATEWAY_OFFLINE":
            self.logger.warning(
                f"[GATEWAY OFFLINE] Reconnect attempt "
                f"{record.get('reconnect_attempt', '?')}"
            )
        elif "_rejection_reason" in record:
            self.logger.warning(
                f"[BAD DATA] Gateway pre-rejected: "
                f"{record['_rejection_reason']} | id={record.get('id', '?')}"
            )
        else:
            self.logger.info(f"[STATUS] {record}")

    def _validate(self, record: dict) -> bool:
        required = ["id", "light", "tilt", "status", "sev", "timestamp",
                    "device", "location"]
        for field in required:
            if field not in record:
                self.logger.warning(
                    f"[BAD DATA] Missing '{field}' in id={record.get('id','?')}"
                )
                return False

        if not (0 <= int(record["light"]) <= 1023):
            self.logger.warning(
                f"[BAD DATA] light={record['light']} out of range | id={record['id']}"
            )
            return False

        if record["tilt"] not in (0, 1):
            self.logger.warning(
                f"[BAD DATA] tilt={record['tilt']} invalid | id={record['id']}"
            )
            return False

        if record["status"] not in VALID_STATUSES:
            self.logger.warning(
                f"[BAD DATA] unknown status={record['status']} | id={record['id']}"
            )
            return False

        if record["sev"] not in (0, 1, 2):
            self.logger.warning(
                f"[BAD DATA] sev={record['sev']} invalid | id={record['id']}"
            )
            return False

        return True

    def start(self):
        try:
            self.client.connect(self.cfg["broker"], self.cfg["port"], keepalive=60)
        except Exception as e:
            self.logger.error(f"Cannot connect to MQTT broker: {e}")
            sys.exit(1)

        self.logger.info("Waiting for data from gateway.py ...")
        self.client.loop_forever()

    def stop(self):
        self.client.disconnect()
        self.logger.info("Session summary:")
        self.logger.info(f"  Received      : {self.stats['received']}")
        self.logger.info(f"  Written to DB : {self.stats['written']}")
        self.logger.info(f"  Rejected      : {self.stats['rejected']}")
        self.logger.info(f"  Parse errors  : {self.stats['parse_errors']}")
        self.logger.info(f"  Alerts        : {self.stats['alerts']}")


def run():
    cfg    = load_config(CONFIG_PATH)
    logger = setup_logging()

    logger.info("Smart Vault Subscriber starting")

    influx = InfluxWriter(cfg, logger)
    if not influx.connect():
        logger.error("Cannot connect to InfluxDB. Check: docker ps | findstr influxdb")
        sys.exit(1)

    subscriber = VaultSubscriber(cfg, influx, logger)
    try:
        subscriber.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        subscriber.stop()
        influx.close()
        logger.info("Subscriber stopped.")


if __name__ == "__main__":
    run()
