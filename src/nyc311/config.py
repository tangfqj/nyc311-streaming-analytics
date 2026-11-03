import json
import os
from pathlib import Path
from datetime import datetime, timezone
import yaml

ROOT = Path(__file__).resolve().parents[2]

def load_config(path=None):
    with open(path or ROOT / "config/default.yaml") as f:
        return yaml.safe_load(f)

def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n")

def log_event(component, **fields):
    event = {"timestamp": datetime.now(timezone.utc).isoformat(), "component": component, **fields}
    path = ROOT / "artifacts/logs/events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(event, default=str, allow_nan=False) + "\n")
    return event

def spark_session(app="nyc311", master=None):
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.master(master or os.getenv("SPARK_MASTER", "local[2]"))
        .appName(app).config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
