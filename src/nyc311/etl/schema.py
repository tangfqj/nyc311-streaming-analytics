"""Explicit raw schema; Socrata floating timestamps represent NYC local time."""
from pyspark.sql.types import StringType, StructField, StructType

RAW_FIELDS = ["unique_key", "created_date", "closed_date", "agency", "complaint_type",
              "descriptor", "borough", "status", "incident_zip", "latitude", "longitude",
              "resolution_description", "resolution_action_updated_date", "due_date",
              "open_data_channel_type", "bbl", "community_board",
              "_source_kind", "_observed_at", "source_kind", "observed_at", "_corrupt_record"]


def raw_schema():
    return StructType([StructField(field, StringType(), True) for field in RAW_FIELDS])
