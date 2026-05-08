# Smart Vault - Edge Gateway
# Reads sensor data from Arduino over serial, validates it,
# simulates failures, and publishes to MQTT broker.
#
# Run: python src/gateway.py

import json
import logging
import os
import sys
import time
import copy
from datetime import datetime, timezone
from pathlib import Path

import serial
import serial.tools.list_ports
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import os
import yaml
from dotenv import load_dotenv


ROOT_DIR    = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config" / "config.yaml"
LOG_DIR     = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


def load_config(path: Path) -> dict:
    load_dotenv(path.parent / ".." / ".env")
    if not path.exists():
        print(f"[FATAL] Config file not found: {path}")
        sys.exit(1)
    with open(path, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(f"[FATAL] Invalid YAML: {e}")
            sys.exit(1)
    if "influxdb" in cfg:
        cfg["influxdb"]["token"] = os.path.expandvars(cfg["influxdb"]["token"])
    return cfg


def setup_logging(cfg: dict) -> logging.Logger:
    log_level = getattr(logging, cfg["logging"]["level"].upper(), logging.DEBUG)
    log_file  = ROOT_DIR / cfg["logging"]["file"]

    logger = logging.getLogger("gateway")
    logger.setLevel(log_level)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"))

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


VALID_STATUSES = {
    "NORMAL", "DARK_ALERT", "DARK_AND_TILT",
    "LIGHT_SPIKE", "SPIKE_AND_TILT", "TILT_DETECTED",
    "SENSOR_FAULT", "SENSOR_FAULT_AND_TILT"
}


def validate_record(record: dict, logger: logging.Logger) -> tuple[bool, str]:
    required = ["id", "device", "location", "light", "tilt",
                "status", "sev", "uptime", "loop_ms"]

    for field in required:
        if field not in record:
            return False, f"missing field '{field}'"

    light = record["light"]
    if not isinstance(light, (int, float)):
        return False, f"light is not numeric: {light}"
    if not (0 <= int(light) <= 1023):
        return False, f"light out of ADC range: {light}"

    if record["tilt"] not in (0, 1):
        return False, f"tilt invalid value: {record['tilt']}"

    if record["sev"] not in (0, 1, 2):
        return False, f"severity invalid: {record['sev']}"

    if record["status"] not in VALID_STATUSES:
        return False, f"unknown status: {record['status']}"

    if not isinstance(record["id"], int) or record["id"] < 1:
        return False, f"id invalid: {record['id']}"

    if record["uptime"] < 0:
        return False, f"negative uptime: {record['uptime']}"

    return True, "ok"


class FailureSimulator:
    # Injects duplicate messages, transmission delays, and out-of-range
    # values to test how the pipeline handles bad data.

    def __init__(self, cfg: dict, logger: logging.Logger):
        sim = cfg["failure_simulation"]
        self.enabled      = sim["enabled"]
        self.dup_every    = sim["duplicate_every_n"]
        self.delay_every  = sim["delay_every_n"]
        self.delay_secs   = sim["delay_seconds"]
        self.oor_every    = sim["out_of_range_every_n"]
        self.logger       = logger
        self.record_count = 0

    def process(self, record: dict) -> list[dict]:
        if not self.enabled:
            return [record]

        self.record_count += 1
        results = [record]

        if self.record_count % self.oor_every == 0:
            bad = copy.deepcopy(record)
            bad["light"] = 9999
            bad["_injected"] = "OUT_OF_RANGE"
            self.logger.warning(
                f"[SIMULATE] OUT_OF_RANGE injected at record {self.record_count} "
                f"- light=9999 (pipeline must catch this)"
            )
            return [bad]

        if self.record_count % self.delay_every == 0:
            self.logger.warning(
                f"[SIMULATE] DELAYED TRANSMISSION - sleeping {self.delay_secs}s "
                f"at record {self.record_count}"
            )
            time.sleep(self.delay_secs)

        if self.record_count % self.dup_every == 0:
            dup = copy.deepcopy(record)
            dup["_injected"] = "DUPLICATE"
            self.logger.warning(
                f"[SIMULATE] DUPLICATE MESSAGE at record {self.record_count} "
                f"- id={record['id']} sent twice"
            )
            results.append(dup)

        return results


class MQTTPublisher:

    def __init__(self, cfg: dict, logger: logging.Logger):
        self.cfg    = cfg["mqtt"]
        self.logger = logger
        self.client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=self.cfg["client_id"]
        )
        self.client.on_connect    = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.connected = False

    def _on_connect(self, client, userdata, connect_flags, reason_code, properties):
        if reason_code == 0:
            self.connected = True
            self.logger.info(f"MQTT connected to {self.cfg['broker']}:{self.cfg['port']}")
        else:
            self.logger.error(f"MQTT connection failed - reason={reason_code}")

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        self.connected = False
        self.logger.warning(f"MQTT disconnected - reason={reason_code}")

    def connect(self) -> bool:
        try:
            self.client.connect(self.cfg["broker"], self.cfg["port"], keepalive=60)
            self.client.loop_start()
            time.sleep(1)
            return self.connected
        except Exception as e:
            self.logger.error(f"MQTT connect error: {e}")
            return False

    def publish(self, topic: str, payload: dict) -> bool:
        if not self.connected:
            self.logger.warning("MQTT not connected - attempting reconnect")
            if not self.connect():
                self.logger.error("MQTT reconnect failed - record lost")
                return False
        try:
            msg = json.dumps(payload, separators=(",", ":"))
            result = self.client.publish(topic, msg, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.logger.debug(f"MQTT published to {topic}: {msg}")
                return True
            else:
                self.logger.error(f"MQTT publish failed - rc={result.rc}")
                return False
        except Exception as e:
            self.logger.error(f"MQTT publish exception: {e}")
            return False

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()


class SerialReader:

    def __init__(self, cfg: dict, logger: logging.Logger):
        self.port    = cfg["device"]["port"]
        self.baud    = cfg["device"]["baud_rate"]
        self.timeout = cfg["device"]["timeout"]
        self.logger  = logger
        self.ser     = None

    def connect(self) -> bool:
        try:
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baud,
                timeout=self.timeout
            )
            time.sleep(2)
            self.logger.info(f"Serial connected: {self.port} at {self.baud} baud")
            return True
        except serial.SerialException as e:
            self.logger.error(f"Serial connect failed on {self.port}: {e}")
            self.logger.info(
                f"Available ports: {[p.device for p in serial.tools.list_ports.comports()]}"
            )
            return False

    def readline(self) -> str | None:
        if self.ser is None or not self.ser.is_open:
            return None
        try:
            raw = self.ser.readline()
            if not raw:
                return None
            return raw.decode("utf-8", errors="replace").strip()
        except serial.SerialException as e:
            self.logger.error(f"[FAILURE] Serial port lost: {e}")
            self.logger.warning("Communication failure - will attempt to reconnect")
            self.ser = None
            return None
        except UnicodeDecodeError as e:
            self.logger.warning(f"Serial decode error: {e}")
            return None

    def disconnect(self):
        if self.ser and self.ser.is_open:
            self.ser.close()
            self.logger.info("Serial port closed")


def run():
    cfg    = load_config(CONFIG_PATH)
    logger = setup_logging(cfg)

    logger.info("Smart Vault Edge Gateway starting")
    logger.info(f"Config: {CONFIG_PATH}")

    serial_reader = SerialReader(cfg, logger)
    mqtt_pub      = MQTTPublisher(cfg, logger)
    simulator     = FailureSimulator(cfg, logger)

    logger.info("Connecting to MQTT broker...")
    if not mqtt_pub.connect():
        logger.error("Cannot reach MQTT broker. Is Docker running?")
        sys.exit(1)

    logger.info(f"Connecting to Arduino on {cfg['device']['port']}...")
    retry_count = 0
    while not serial_reader.connect():
        retry_count += 1
        if retry_count > 5:
            logger.error("Could not connect to Arduino after 5 attempts.")
            sys.exit(1)
        logger.warning(f"Retrying serial ({retry_count}/5) in 3s...")
        time.sleep(3)

    stats = {
        "received": 0, "published": 0, "rejected": 0,
        "parse_errors": 0, "serial_failures": 0,
    }

    last_ids  = set()
    last_stat = time.time()

    logger.info("Pipeline running. Press Ctrl+C to stop.")

    try:
        while True:
            if serial_reader.ser is None:
                stats["serial_failures"] += 1
                logger.warning(
                    f"[FAILURE] Serial offline. "
                    f"Reconnect attempt {stats['serial_failures']}..."
                )
                mqtt_pub.publish(
                    cfg["mqtt"]["topic_status"],
                    {
                        "event": "GATEWAY_OFFLINE",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "reconnect_attempt": stats["serial_failures"],
                    }
                )
                time.sleep(5)
                serial_reader.connect()
                continue

            line = serial_reader.readline()
            if not line:
                continue

            if '"event"' in line:
                logger.info(f"Arduino event: {line}")
                continue

            stats["received"] += 1

            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                stats["parse_errors"] += 1
                logger.warning(f"[BAD DATA] JSON parse error: {e} | raw: {line}")
                continue

            record["timestamp"] = datetime.now(timezone.utc).isoformat()

            records_to_publish = simulator.process(record)

            for rec in records_to_publish:
                is_valid, reason = validate_record(rec, logger)
                if not is_valid:
                    stats["rejected"] += 1
                    logger.warning(
                        f"[BAD DATA] Record rejected - {reason} "
                        f"| id={rec.get('id','?')} | light={rec.get('light','?')}"
                    )
                    rec["_rejection_reason"] = reason
                    mqtt_pub.publish(cfg["mqtt"]["topic_status"], rec)
                    continue

                rec_id = rec["id"]
                if rec_id in last_ids:
                    logger.warning(f"[BAD DATA] Duplicate id={rec_id} detected")
                    rec["_duplicate"] = True
                last_ids.add(rec_id)
                if len(last_ids) > 100:
                    last_ids.pop()

                topic = cfg["mqtt"]["topic_data"]
                if mqtt_pub.publish(topic, rec):
                    stats["published"] += 1
                    logger.info(
                        f"[{stats['published']:>6}] "
                        f"id={rec['id']:<6} "
                        f"light={rec['light']:<5} "
                        f"tilt={rec['tilt']} "
                        f"status={rec['status']:<25} "
                        f"sev={rec['sev']}"
                    )

                    if rec.get("sev", 0) == 2 and serial_reader.ser:
                        try:
                            serial_reader.ser.write(b"ALERT\n")
                            logger.info("[CMD] Sent ALERT to Arduino - sev=2 detected")
                        except serial.SerialException:
                            pass

            if time.time() - last_stat >= 60:
                last_stat = time.time()
                logger.info(
                    f"[STATS] received={stats['received']} "
                    f"published={stats['published']} "
                    f"rejected={stats['rejected']} "
                    f"errors={stats['parse_errors']}"
                )

    except KeyboardInterrupt:
        logger.info("Shutdown requested")

    finally:
        logger.info("Final stats:")
        for key, val in stats.items():
            logger.info(f"  {key} = {val}")
        serial_reader.disconnect()
        mqtt_pub.disconnect()
        logger.info("Gateway stopped.")


if __name__ == "__main__":
    run()