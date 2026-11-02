"""Spark transformations, independent of file IO and profiling actions."""
from pyspark.sql import functions as F

BOROUGHS = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]


def _timestamp(name):
    raw = F.col(name)
    parsed = F.to_timestamp(raw)
    aware = raw.rlike(r"(Z|[+-][0-9]{2}:?[0-9]{2})$")
    return F.when(aware, parsed).otherwise(F.to_utc_timestamp(parsed, "America/New_York"))


def clean_rows(raw):
    """Retain raw times for quarantine, normalize text and attach reject reason.

    Offset-free times use America/New_York. Spark resolves ambiguous autumn
    wall times to its earlier offset; nonexistent spring wall times are rejected
    via local-time roundtrip validation. Explicit offsets are honored.
    Missing observation timestamps use a fixed epoch and deterministic hash
    ordering, never a fabricated source modification timestamp.
    """
    df = raw
    for name in raw.columns:
        df = df.withColumn(name, F.when(F.trim(F.col(name)) == "", F.lit(None)).otherwise(F.trim(F.col(name))))
    df = (df.withColumn("created_date_raw", F.col("created_date"))
          .withColumn("closed_date_raw", F.col("closed_date"))
          .withColumn("created_date", _timestamp("created_date"))
          .withColumn("closed_date", _timestamp("closed_date"))
          .withColumn("_invalid_borough", ~F.coalesce(F.upper("borough").isin(BOROUGHS), F.lit(False)))
          .withColumn("borough", F.when(F.upper("borough").isin(BOROUGHS), F.upper("borough")).otherwise("UNKNOWN"))
          .withColumn("agency", F.coalesce(F.upper("agency"), F.lit("UNKNOWN")))
          .withColumn("complaint_type", F.coalesce(F.col("complaint_type"), F.lit("UNKNOWN")))
          .withColumn("status", F.coalesce(F.col("status"), F.lit("UNKNOWN")))
          .withColumn("source_kind", F.coalesce("_source_kind", "source_kind", F.lit("unknown")))
          .withColumn("source_kind", F.when(F.col("source_kind") == "official_nyc311_snapshot", "official_snapshot")
                      .otherwise(F.col("source_kind")))
          .withColumn("_observation_missing", F.coalesce("_observed_at", "observed_at").isNull())
          .withColumn("_observation_invalid", F.coalesce("_observed_at", "observed_at").isNotNull() &
                      F.to_timestamp(F.coalesce("_observed_at", "observed_at")).isNull())
          .withColumn("observed_at", F.coalesce(F.to_timestamp(F.coalesce("_observed_at", "observed_at")),
                                                 F.lit("1970-01-01T00:00:00Z").cast("timestamp"))))
    naive_created = ~F.col("created_date_raw").rlike(r"(Z|[+-][0-9]{2}:?[0-9]{2})$")
    nonexistent = naive_created & (F.from_utc_timestamp("created_date", "America/New_York") != F.to_timestamp("created_date_raw"))
    naive_closed = ~F.col("closed_date_raw").rlike(r"(Z|[+-][0-9]{2}:?[0-9]{2})$")
    nonexistent_closed = naive_closed & (F.from_utc_timestamp("closed_date", "America/New_York") != F.to_timestamp("closed_date_raw"))
    reason = (F.when(F.col("_corrupt_record").isNotNull(), "malformed_json")
              .when(F.col("unique_key").isNull(), "missing_unique_key")
              .when(F.col("created_date").isNull(), "invalid_created_date")
              .when(F.col("_observation_invalid"), "invalid_observed_at")
              .when(nonexistent, "nonexistent_local_created_time")
              .when(F.col("closed_date_raw").isNotNull() & F.col("closed_date").isNull(), "invalid_closed_date")
              .when(nonexistent_closed, "nonexistent_local_closed_time")
              .when(F.col("closed_date") < F.col("created_date"), "closed_before_created"))
    return df.withColumn("reject_reason", reason)
