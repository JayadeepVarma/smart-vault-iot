# Smart Vault - ML Anomaly Detection
# Uses Isolation Forest to detect unusual sensor readings.
#
# Why Isolation Forest?
#   No labelled data needed. It works by isolating data points using
#   random trees. Anomalies are easier to isolate so they get shorter
#   paths. Score below 0 = anomaly, above 0 = normal.
#
# Features used:
#   light   - sudden brightness/darkness changes are anomalous
#   tilt    - any movement of the vault is suspicious
#   sev     - captures combined alert state
#   loop_ms - timing drift can indicate pipeline problems
#
# Results are written back to InfluxDB as vault_anomalies so
# Grafana can display them in the dashboard.
#
# Run: python src/ml_anomaly.py

import logging
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from logging.handlers import RotatingFileHandler

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import os
import yaml
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

warnings.filterwarnings("ignore")

ROOT_DIR    = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config" / "config.yaml"
LOG_DIR     = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

CONTAMINATION      = 0.05   # expect ~5% of readings to be anomalous
RANDOM_STATE       = 42
MIN_RECORDS_NEEDED = 100
FEATURES           = ["light", "tilt", "sev", "loop_ms"]


def setup_logging() -> logging.Logger:
    log_file = LOG_DIR / "ml_anomaly.log"
    logger   = logging.getLogger("ml_anomaly")
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


def load_config() -> dict:
    load_dotenv(CONFIG_PATH.parent / ".." / ".env")
    if not CONFIG_PATH.exists():
        print(f"[FATAL] Config not found: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if "influxdb" in cfg:
        cfg["influxdb"]["token"] = os.path.expandvars(cfg["influxdb"]["token"])
    return cfg


class InfluxReader:

    def __init__(self, cfg: dict, logger: logging.Logger):
        self.icfg   = cfg["influxdb"]
        self.logger = logger
        self.client = InfluxDBClient(
            url=self.icfg["url"],
            token=self.icfg["token"],
            org=self.icfg["org"],
        )
        self.query_api = self.client.query_api()

    def fetch(self, hours: int = 24) -> pd.DataFrame:
        self.logger.info(f"Fetching last {hours}h of data from InfluxDB...")

        query = f"""
from(bucket: "{self.icfg['bucket']}")
  |> range(start: -{hours}h)
  |> filter(fn: (r) => r._measurement == "vault_readings")
  |> filter(fn: (r) =>
      r._field == "light" or
      r._field == "tilt"  or
      r._field == "sev"   or
      r._field == "loop_ms" or
      r._field == "reading_id"
  )
  |> pivot(
      rowKey: ["_time"],
      columnKey: ["_field"],
      valueColumn: "_value"
  )
  |> keep(columns: [
      "_time", "device", "location", "status",
      "light", "tilt", "sev", "loop_ms", "reading_id"
  ])
"""
        try:
            tables  = self.query_api.query(query, org=self.icfg["org"])
            records = []
            for table in tables:
                for record in table.records:
                    records.append(record.values)

            if not records:
                self.logger.warning("No records returned from InfluxDB")
                return pd.DataFrame()

            df = pd.DataFrame(records)
            keep = ["_time", "device", "location", "status",
                    "light", "tilt", "sev", "loop_ms", "reading_id"]
            df = df[[c for c in keep if c in df.columns]]

            for col in ["light", "tilt", "sev", "loop_ms", "reading_id"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            df = df.dropna(subset=["light", "tilt", "sev"])
            df = df.sort_values("_time").reset_index(drop=True)

            self.logger.info(
                f"Fetched {len(df)} records | "
                f"range: {df['_time'].min()} to {df['_time'].max()}"
            )
            return df

        except Exception as e:
            self.logger.error(f"InfluxDB fetch failed: {e}")
            return pd.DataFrame()

    def close(self):
        self.client.close()


class InfluxWriter:

    def __init__(self, cfg: dict, logger: logging.Logger):
        self.icfg   = cfg["influxdb"]
        self.logger = logger
        self.client = InfluxDBClient(
            url=self.icfg["url"],
            token=self.icfg["token"],
            org=self.icfg["org"],
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def write_results(self, df: pd.DataFrame) -> int:
        written = 0
        errors  = 0
        self.logger.info(f"Writing {len(df)} anomaly results to InfluxDB...")

        for _, row in df.iterrows():
            try:
                point = (
                    Point("vault_anomalies")
                    .tag("device",     str(row.get("device",     "unknown")))
                    .tag("location",   str(row.get("location",   "unknown")))
                    .tag("status",     str(row.get("status",     "UNKNOWN")))
                    .tag("is_anomaly", str(int(row["is_anomaly"])))
                    .field("anomaly_score", float(row["anomaly_score"]))
                    .field("is_anomaly",    int(row["is_anomaly"]))
                    .field("light",         int(row["light"]))
                    .field("tilt",          int(row["tilt"]))
                    .field("sev",           int(row["sev"]))
                    .time(row["_time"], WritePrecision.NS)
                )
                self.write_api.write(
                    bucket=self.icfg["bucket"],
                    org=self.icfg["org"],
                    record=point,
                )
                written += 1
            except Exception as e:
                errors += 1
                self.logger.error(f"Write failed for row {row.get('reading_id','?')}: {e}")

        self.logger.info(f"Write complete: {written} written, {errors} failed")
        return written

    def close(self):
        self.client.close()


class VaultAnomalyDetector:

    def __init__(self, logger: logging.Logger):
        self.logger  = logger
        self.model   = IsolationForest(
            contamination=CONTAMINATION,
            random_state=RANDOM_STATE,
            n_estimators=100,
            max_samples="auto",
        )
        self.scaler  = StandardScaler()
        self.trained = False

    def train(self, df: pd.DataFrame) -> bool:
        if len(df) < MIN_RECORDS_NEEDED:
            self.logger.error(
                f"Not enough data: {len(df)} records. "
                f"Need at least {MIN_RECORDS_NEEDED}."
            )
            return False

        available = [f for f in FEATURES if f in df.columns]
        if len(available) < 2:
            self.logger.error(f"Not enough feature columns: {available}")
            return False

        X = df[available].values

        self.logger.info(
            f"Training on {len(X)} records with features: {available}"
        )
        for i, feat in enumerate(available):
            col = X[:, i]
            self.logger.info(
                f"  {feat:<10} mean={col.mean():.2f} std={col.std():.2f} "
                f"min={col.min():.0f} max={col.max():.0f}"
            )

        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)
        self.trained  = True
        self.features = available
        self.logger.info("Model training complete")
        return True

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.trained:
            self.logger.error("Model not trained yet")
            return df

        X        = df[self.features].values
        X_scaled = self.scaler.transform(X)
        scores   = self.model.decision_function(X_scaled)
        preds    = self.model.predict(X_scaled)

        df = df.copy()
        df["anomaly_score"] = scores
        df["is_anomaly"]    = (preds == -1).astype(int)

        n_anomalies = df["is_anomaly"].sum()
        pct         = (n_anomalies / len(df)) * 100
        self.logger.info(
            f"Prediction complete: {n_anomalies} anomalies out of "
            f"{len(df)} records ({pct:.1f}%)"
        )

        self.logger.info("Breakdown by status:")
        breakdown = df.groupby("status")["is_anomaly"].agg(["sum", "count"])
        breakdown.columns = ["anomalies", "total"]
        breakdown["pct"] = (breakdown["anomalies"] / breakdown["total"] * 100).round(1)
        for status, row in breakdown.iterrows():
            self.logger.info(
                f"  {status:<25} "
                f"{int(row['anomalies'])}/{int(row['total'])} "
                f"= {row['pct']}%"
            )
        return df


def run():
    logger = setup_logging()
    cfg    = load_config()

    logger.info("Smart Vault - ML Anomaly Detection")
    logger.info(f"Model: Isolation Forest | Features: {FEATURES}")
    logger.info(f"Contamination: {CONTAMINATION*100:.0f}%")

    reader = InfluxReader(cfg, logger)
    df     = reader.fetch(hours=24)
    reader.close()

    if df.empty:
        logger.error(
            "No data fetched. Make sure gateway.py and subscriber.py "
            "are running and writing data to InfluxDB."
        )
        sys.exit(1)

    logger.info(f"Dataset: {len(df)} records loaded")

    detector = VaultAnomalyDetector(logger)
    if not detector.train(df):
        logger.error("Training failed")
        sys.exit(1)

    df_results = detector.predict(df)

    logger.info("Sample anomalies:")
    anomalies = df_results[df_results["is_anomaly"] == 1].head(10)
    if anomalies.empty:
        logger.info("  No anomalies found in sample")
    else:
        for _, row in anomalies.iterrows():
            logger.info(
                f"  time={row['_time']} light={int(row['light'])} "
                f"tilt={int(row['tilt'])} status={row.get('status','?')} "
                f"score={row['anomaly_score']:.4f}"
            )

    writer  = InfluxWriter(cfg, logger)
    written = writer.write_results(df_results)
    writer.close()

    logger.info("ML Anomaly Detection complete")
    logger.info(f"  Records processed : {len(df_results)}")
    logger.info(f"  Anomalies found   : {int(df_results['is_anomaly'].sum())}")
    logger.info(f"  Normal readings   : {int((df_results['is_anomaly']==0).sum())}")
    logger.info(f"  Written to DB     : {written}")
    logger.info("  Measurement       : vault_anomalies")


if __name__ == "__main__":
    run()