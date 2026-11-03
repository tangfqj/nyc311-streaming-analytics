"""Batch Spark JSONL/CSV -> validated, deduplicated, partitioned Parquet."""
from __future__ import annotations

import time
from pathlib import Path

from pyspark.sql import Window, functions as F

from nyc311.config import log_event, spark_session, write_json
from nyc311.etl.clean import clean_rows
from nyc311.etl.quality import assert_reconciliation
from nyc311.etl.schema import raw_schema


def _input_files(path):
    path = Path(path)
    if path.is_file():
        return [str(path)]
    files = sorted(str(p) for p in path.rglob("*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No JSONL data files found in {path}")
    return files


def build_silver(input_path, output_path, metrics_path, spark=None):
    """Return persisted DQ metrics; latest observed_at wins among valid rows.

    Accepted means one silver row per key after validation and snapshot
    deduplication. Rejected rows go to ``<output_path>_quarantine``. Ties in
    observed_at resolve by content hash, not input partition order. Metadata
    changes with download time, but same-input silver content is idempotent.
    """
    started = time.monotonic()
    own_spark = spark is None
    metrics = {"status": "Running", "input_path": str(input_path), "output_path": str(output_path),
               "input_files": 0, "source_timezone": "America/New_York", "silver_timezone": "UTC",
               "dst_policy": "Earlier offset for ambiguous autumn wall times; nonexistent spring times quarantined",
               "deduplication": "Valid row with latest observed_at; deterministic SHA256 content tie-breaker"}
    cleaned = None
    silver = None
    try:
        files = _input_files(input_path)
        metrics["input_files"] = len(files)
        spark = spark or spark_session("nyc311-etl")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        spark.conf.set("spark.sql.ansi.enabled", "false")
        if len(files) == 1 and files[0].lower().endswith(".csv"):
            csv = spark.read.option("header", True).option("inferSchema", False).csv(files)
            raw = csv.select(*[F.col(field.name).cast(field.dataType).alias(field.name) if field.name in csv.columns
                               else F.lit(None).cast(field.dataType).alias(field.name) for field in raw_schema().fields])
        else:
            raw = spark.read.schema(raw_schema()).option("mode", "PERMISSIVE").json(files)
        checked_fields = ["unique_key", "created_date", "closed_date", "agency", "complaint_type", "borough", "status"]
        missing = raw.agg(*[F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
                            for c in checked_fields]).first()
        metrics["missing_fields"] = {c: missing[c] or 0 for c in checked_fields}
        cleaned = clean_rows(raw).cache()
        metrics["input_rows"] = cleaned.count()
        rejected = cleaned.filter(F.col("reject_reason").isNotNull())
        metrics["rejection_reasons"] = {r["reject_reason"]: r["count"] for r in rejected.groupBy("reject_reason").count().collect()}
        metrics["rejected_rows"] = sum(metrics["rejection_reasons"].values())
        metrics["invalid_borough_rows"] = cleaned.filter("_invalid_borough").count()
        metrics["missing_observation_rows"] = cleaned.filter("_observation_missing").count()
        metrics["invalid_observation_timestamp_rows"] = cleaned.filter("_observation_invalid").count()
        valid = cleaned.filter(F.col("reject_reason").isNull())
        hash_columns = sorted(name for name in valid.columns if not name.startswith("_"))
        valid = valid.withColumn("_content_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in hash_columns])), 256))
        order = Window.partitionBy("unique_key").orderBy(F.col("observed_at").desc(), F.col("_content_hash").desc())
        silver = (valid.withColumn("_row_number", F.row_number().over(order)).filter("_row_number = 1")
                  .select("unique_key", "created_date", "closed_date", "agency", "complaint_type", "descriptor",
                          "borough", "status", "incident_zip", "latitude", "longitude",
                          "resolution_description", "resolution_action_updated_date", "due_date",
                          "open_data_channel_type", "bbl", "community_board", "source_kind", "observed_at")
                  .withColumn("created_year", F.year(F.from_utc_timestamp("created_date", "America/New_York")))
                  .withColumn("created_month", F.month(F.from_utc_timestamp("created_date", "America/New_York"))).cache())
        metrics["accepted_rows"] = silver.count()
        metrics["output_rows"] = metrics["accepted_rows"]
        metrics["deduplicated_rows"] = metrics["input_rows"] - metrics["rejected_rows"] - metrics["accepted_rows"]
        metrics["duplicate_silver_keys"] = silver.groupBy("unique_key").count().filter("count > 1").count()
        metrics["closed_rows"] = silver.filter("closed_date IS NOT NULL").count()
        metrics["open_rows"] = metrics["accepted_rows"] - metrics["closed_rows"]
        metrics["status_closed_date_conflicts"] = silver.filter(
            (F.upper("status").isin("CLOSED") & F.col("closed_date").isNull()) |
            (~F.upper("status").isin("CLOSED") & F.col("closed_date").isNotNull())).count()
        metrics["source_kinds"] = {r["source_kind"]: r["count"] for r in silver.groupBy("source_kind").count().collect()}
        bounds = silver.agg(F.min("created_date").alias("min"), F.max("created_date").alias("max")).first()
        metrics["created_date_min_utc"] = str(bounds["min"]) if bounds["min"] is not None else None
        metrics["created_date_max_utc"] = str(bounds["max"]) if bounds["max"] is not None else None
        metrics["counts_by_borough"] = {r["borough"]: r["count"] for r in silver.groupBy("borough").count().collect()}
        metrics["counts_by_agency"] = {r["agency"]: r["count"] for r in silver.groupBy("agency").count().collect()}
        metrics["counts_by_complaint_type"] = {r["complaint_type"]: r["count"] for r in silver.groupBy("complaint_type").count().collect()}
        assert_reconciliation(metrics)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        silver.write.mode("overwrite").partitionBy("created_year", "created_month").parquet(str(output_path))
        rejected.write.mode("overwrite").parquet(str(output_path) + "_quarantine")
        metrics.update(status="Success", elapsed_seconds=time.monotonic() - started,
                       spark_version=spark.version, quarantine_path=str(output_path) + "_quarantine")
        write_json(metrics_path, metrics)
        log_event("build_silver", **metrics)
        return metrics
    except Exception as exc:
        metrics.update(status="Failed", failure_reason=str(exc), elapsed_seconds=time.monotonic() - started)
        write_json(metrics_path, metrics)
        log_event("build_silver", **metrics)
        raise
    finally:
        if silver is not None:
            silver.unpersist()
        if cleaned is not None:
            cleaned.unpersist()
        if own_spark and spark is not None:
            spark.stop()
