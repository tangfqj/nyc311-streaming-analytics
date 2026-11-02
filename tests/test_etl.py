import json
from pathlib import Path

import pytest

from nyc311.config import ROOT, spark_session
from nyc311.etl.build_silver import build_silver
from nyc311.etl.quality import assert_reconciliation


@pytest.fixture(scope="module")
def spark():
    session = spark_session("test-nyc311-etl", "local[2]")
    yield session
    session.stop()


@pytest.mark.spark
def test_fixture_reconciliation_latest_snapshot_timezone_and_repeat(tmp_path, spark):
    fixture = ROOT / "tests/fixtures/nyc311_synthetic.jsonl"
    output, metrics = tmp_path / "silver", tmp_path / "quality.json"
    result = build_silver(fixture, output, metrics, spark=spark)
    assert result["input_rows"] == 188
    assert result["accepted_rows"] == 181
    assert result["rejected_rows"] == 5
    assert result["deduplicated_rows"] == 2
    assert result["rejection_reasons"]["nonexistent_local_created_time"] == 1
    assert result["invalid_borough_rows"] == 1
    assert result["source_kinds"] == {"synthetic_fixture": 181}
    assert assert_reconciliation(result)
    frame = spark.read.parquet(str(output))
    first = frame.filter("unique_key = 'FIXTURE-000000'").first()
    assert str(first["created_date"]) == "2025-01-01 13:00:00"
    latest = frame.filter("unique_key = 'FIXTURE-000001'").first()
    assert latest["status"] == "Closed"
    assert frame.filter("unique_key = 'UNKNOWN_BOROUGH'").first()["borough"] == "UNKNOWN"
    original = {r["unique_key"]: r.asDict() for r in frame.collect()}
    repeated = build_silver(fixture, output, metrics, spark=spark)
    after = {r["unique_key"]: r.asDict() for r in spark.read.parquet(str(output)).collect()}
    assert original == after
    assert repeated["accepted_rows"] == result["accepted_rows"]
    assert json.loads(metrics.read_text())["status"] == "Success"


@pytest.mark.spark
def test_explicit_offsets_not_double_shifted_and_bad_closed_quarantined(tmp_path, spark):
    rows = [dict(unique_key="A", created_date="2025-07-01T12:00:00Z", closed_date=None),
            dict(unique_key="B", created_date="2025-07-01T12:00:00-04:00", closed_date=None),
            dict(unique_key="C", created_date="2025-07-01T12:00:00", closed_date="bad"),
            dict(unique_key="D", created_date="2025-07-01T12:00:00", closed_date=None, _observed_at="bad"),
            dict(unique_key="E", created_date="2025-03-09T00:30:00", closed_date="2025-03-09T02:30:00")]
    path = tmp_path / "input.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    result = build_silver(path, tmp_path / "silver", tmp_path / "dq.json", spark=spark)
    actual = {r["unique_key"]: str(r["created_date"]) for r in spark.read.parquet(str(tmp_path / "silver")).collect()}
    assert actual == {"A": "2025-07-01 12:00:00", "B": "2025-07-01 16:00:00"}
    assert result["rejected_rows"] == 3
    assert result["rejection_reasons"]["nonexistent_local_closed_time"] == 1
    assert result["missing_observation_rows"] == 4
    assert result["invalid_observation_timestamp_rows"] == 1


@pytest.mark.spark
def test_csv_headers_are_matched_by_name(tmp_path, spark):
    path = tmp_path / "reordered.csv"
    path.write_text("agency,borough,created_date,unique_key\nHPD,BRONX,2025-01-01T00:00:00,CSV-A\n")
    result = build_silver(path, tmp_path / "silver", tmp_path / "dq.json", spark=spark)
    row = spark.read.parquet(str(tmp_path / "silver")).first()
    assert row["unique_key"] == "CSV-A"
    assert row["agency"] == "HPD"
    assert result["missing_fields"]["closed_date"] == 1


def test_missing_input_emits_machine_readable_failure(tmp_path):
    with pytest.raises(FileNotFoundError):
        build_silver(tmp_path / "missing", tmp_path / "silver", tmp_path / "dq.json")
    assert json.loads((tmp_path / "dq.json").read_text())["status"] == "Failed"


def test_reconciliation_cannot_silently_drop_rows():
    with pytest.raises(AssertionError, match="reconciliation"):
        assert_reconciliation(dict(input_rows=10, accepted_rows=7, rejected_rows=1,
                                   deduplicated_rows=1, duplicate_silver_keys=0))
