import json

import pytest
import requests

from nyc311.ingest.download_311 import REQUIRED_FIELDS, _get_json, checksum, download_311, validate_schema


class Response:
    def __init__(self, payload, status=200):
        self.payload, self.status_code = payload, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self):
        return self.payload


class Session:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None, **kwargs):
        self.calls.append((url, params, kwargs))
        if "/api/views/" in url:
            return Response({"columns": [{"fieldName": f, "dataTypeName": "text"} for f in REQUIRED_FIELDS]})
        keys = ["10017", "10117", "10217"]
        if "unique_key > '10117'" in params["$where"]:
            keys = ["10217"]
        return Response([dict(unique_key=k, created_date="2025-01-01T12:00:00", agency="HPD",
                              complaint_type="HEAT/HOT WATER", borough="BRONX") for k in keys[:params["$limit"]]])


def test_paginated_manifest_and_checksum_resume(tmp_path):
    session = Session()
    result = download_311(tmp_path, "2025-01-01", "2025-01-02", page_size=2,
                          max_rows=20, session=session, pause=0, app_token="private-token")
    assert result["status"] == "Success"
    assert result["rows"] == 3
    window = result["windows"][0]
    assert checksum(tmp_path / window["file"]) == window["sha256"]
    rows = [json.loads(line) for line in (tmp_path / window["file"]).read_text().splitlines()]
    assert len({r["unique_key"] for r in rows}) == 3
    assert all(r["_source_kind"] == "official_nyc311_snapshot" for r in rows)
    assert "private-token" not in (tmp_path / "manifest.json").read_text()
    assert "complaint_type" in result["schema"]["selected_fields"]
    before = len([c for c in session.calls if "/resource/" in c[0]])
    download_311(tmp_path, "2025-01-01", "2025-01-02", page_size=2,
                 max_rows=20, session=session, pause=0)
    assert len([c for c in session.calls if "/resource/" in c[0]]) == before


def test_download_cap_is_explicitly_truncated(tmp_path):
    result = download_311(tmp_path, "2025-01-01", "2025-01-02", max_rows=2,
                          page_size=2, session=Session(), pause=0)
    assert result["rows"] == 2
    assert result["status"] == result["windows"][0]["status"] == "Truncated"
    assert result["sampling"]["population_complete"] is False


def test_truncated_unsampled_download_is_not_population_complete(tmp_path):
    result = download_311(tmp_path, "2025-01-01", "2025-01-02", max_rows=2,
                          page_size=2, session=Session(), pause=0, sample_suffix=None)
    assert result["status"] == "Truncated"
    assert result["sampling"]["population_complete"] is False
    assert result["coverage"]["complete_date_windows"] is False


def test_configuration_change_does_not_reuse_wrong_snapshot(tmp_path):
    download_311(tmp_path, "2025-01-01", "2025-01-02", page_size=2,
                 max_rows=2, session=Session(), pause=0)
    with pytest.raises(ValueError, match="different configuration"):
        download_311(tmp_path, "2025-01-01", "2025-01-02", page_size=2,
                     max_rows=2, session=Session(), sample_suffix="18", pause=0)


def test_noncritical_schema_drift_allowed_but_required_field_missing_fails():
    assert "complaint_type" in validate_schema(Session(), pause=0)["columns"]
    class Missing(Session):
        def get(self, *args, **kwargs):
            return Response({"columns": [{"fieldName": "created_date"}]})
    with pytest.raises(ValueError, match="missing required"):
        validate_schema(Missing(), pause=0)


def test_retry_transient_failure(monkeypatch):
    monkeypatch.setattr("nyc311.ingest.download_311.time.sleep", lambda _: None)
    class Flaky:
        count = 0
        def get(self, *args, **kwargs):
            self.count += 1
            return Response([], 503 if self.count == 1 else 200)
    session = Flaky()
    assert _get_json(session, "https://example.test", pause=0) == []
    assert session.count == 2


def test_suffix_rejects_query_injection(tmp_path):
    with pytest.raises(ValueError, match="decimal digits"):
        download_311(tmp_path, "2025-01-01", "2025-01-02", sample_suffix="';drop")


def test_official_fixture_has_verifiable_provenance():
    from nyc311.config import ROOT
    fixture = ROOT / "tests/fixtures/nyc311_official_sample.jsonl"
    provenance = json.loads(fixture.with_suffix(".provenance.json").read_text())
    assert checksum(fixture) == provenance["sha256"]
    rows = [json.loads(line) for line in fixture.read_text().splitlines()]
    assert len(rows) == provenance["rows"] == 12
    assert all(row["_source_kind"] == "official_nyc311_snapshot" for row in rows)
