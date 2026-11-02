import json

import numpy as np
import pandas as pd
import requests

from nyc311.analysis.seasonality import ALL, add_historical_baseline, run_seasonality
from nyc311.ingest.download_weather import download_weather


def test_weekday_baseline_excludes_current_and_future():
    frame = pd.DataFrame({"date": pd.date_range("2025-01-06", periods=4, freq="7D"),
        "complaint_type": "HEAT/HOT WATER", "borough": "BRONX",
        "count": [10., 10., 100., 9999.], "coverage_complete": True})
    result = add_historical_baseline(frame, min_history=2, min_count=20, min_excess=5)
    assert np.isnan(result.iloc[1].baseline_mean)
    assert result.iloc[2].baseline_mean == 10
    assert result.iloc[2].is_surge
    assert np.isnan(result.iloc[2].z_score)
    changed = frame.copy()
    changed.loc[3, "count"] = 1
    assert add_historical_baseline(changed, min_history=2).iloc[2].baseline_mean == result.iloc[2].baseline_mean


def test_complete_calendar_distinguishes_zero_from_failed_window(tmp_path):
    silver = tmp_path / "silver.parquet"
    pd.DataFrame({"unique_key": ["1", "2"], "created_date": ["2025-01-01T12:00:00Z", "2025-01-03T12:00:00Z"],
                  "closed_date": [None, None], "agency": "HPD", "borough": "BRONX", "complaint_type": "HEAT/HOT WATER",
                  "source_kind": "official_snapshot"}).to_parquet(silver)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"query_config": {"start_date": "2025-01-01", "end_date": "2025-01-04"},
        "sampling": {"population_complete": False, "suffix": "17"}, "windows": [
            {"start_date": "2025-01-01", "end_date": "2025-01-03", "status": "Success"},
            {"start_date": "2025-01-03", "end_date": "2025-01-04", "status": "Failed"}]}))
    summary = run_seasonality(silver, tmp_path / "out", config={"analysis": {"as_of": "2025-07-01T00:00:00Z"},
        "source_timezone": "America/New_York", "seasonality": {"coverage_manifest": str(manifest)}})
    daily = pd.read_csv(tmp_path / "out/metrics/daily_volume.csv")
    total = daily[daily.complaint_type == ALL].set_index("date")
    assert total.loc["2025-01-02", "count"] == 0
    assert pd.isna(total.loc["2025-01-03", "count"])
    assert total.loc["2025-01-03", "observed_rows"] == 1
    assert summary["covered_days"] == 2
    assert summary["weather"]["status"] == "Not Run"
    assert not total.population_complete.any()


def test_weather_downloader_persists_exact_dates_and_provenance(monkeypatch, tmp_path):
    class Response:
        status_code = 200
        def raise_for_status(self):
            return None
        def json(self):
            return {"daily": {"time": ["2025-01-01", "2025-01-02"], "temperature_2m_min": [-1, 0],
                "temperature_2m_max": [5, 6], "precipitation_sum": [0, 1]},
                "daily_units": {"temperature_2m_min": "°C", "precipitation_sum": "mm"}}
    monkeypatch.setattr(requests, "get", lambda *a, **k: Response())
    result = download_weather(tmp_path, "2025-01-01", "2025-01-02")
    assert result["status"] == "Success"
    assert result["rows"] == 2
    assert result["parameters"]["models"] == "era5"
    assert result["parameters"]["timezone"] == "America/New_York"
    assert "reanalysis" in result["weather_semantics"]
    assert (tmp_path / "weather.csv").exists()


def test_weather_failure_is_artifact_without_fake_values(monkeypatch, tmp_path):
    def fail(*args, **kwargs):
        raise requests.Timeout("offline test")
    monkeypatch.setattr(requests, "get", fail)
    result = download_weather(tmp_path, "2025-01-01", "2025-01-02", config={"weather": {"retries": 0}})
    assert result["status"] == "Failed"
    assert "offline test" in result["reason"]
    assert not (tmp_path / "weather.csv").exists()
    assert json.loads((tmp_path / "manifest.json").read_text())["status"] == "Failed"


def test_benchmark_source_kind_cannot_enter_seasonality(tmp_path):
    import pytest
    silver = tmp_path / "silver.parquet"
    pd.DataFrame({"unique_key": ["r1"], "created_date": ["2025-01-01T12:00:00Z"], "closed_date": [None],
                  "agency": "HPD", "borough": "BRONX", "complaint_type": "HEAT/HOT WATER",
                  "source_kind": "synthetic_scale_replica"}).to_parquet(silver)
    with pytest.raises(ValueError, match="provenance"):
        run_seasonality(silver, tmp_path / "out", config={"analysis": {"as_of": "2025-07-01T00:00:00Z"}})
