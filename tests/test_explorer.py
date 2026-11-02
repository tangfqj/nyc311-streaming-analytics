"""Interactive cohort tests use labelled synthetic fixtures, never official claims."""

import json

import numpy as np
import pandas as pd
import pytest

from nyc311.analysis.explorer import (
    aggregate_daily,
    available_dates,
    filter_requests,
    grouped_km,
    load_explorer_inputs,
    summarize_selection,
    weather_selection,
)


def cohort(start="2025-01-01", daily_counts=(2,) * 31, agency="A"):
    rows = []
    for day, count in zip(pd.date_range(start, periods=len(daily_counts)), daily_counts):
        for item in range(count):
            created = (day + pd.Timedelta(hours=12)).tz_localize("America/New_York").tz_convert("UTC")
            rows.append({"unique_key": f"{agency}-{day.date()}-{item}", "created_date": created,
                         "closed_date": created + pd.Timedelta(days=1), "agency": agency,
                         "complaint_type": "Heat" if item % 2 else "Noise", "borough": "BRONX",
                         "status": "Closed", "source_kind": "synthetic_fixture"})
    return pd.DataFrame(rows, columns=["unique_key", "created_date", "closed_date", "agency", "complaint_type", "borough", "status", "source_kind"])


def coverage(start="2025-01-01", end="2025-01-31", missing=()):
    dates = pd.date_range(start, end)
    return {"available_dates": [str(day.date()) for day in dates],
            "covered_dates": [str(day.date()) for day in dates if str(day.date()) not in missing],
            "sampling": {"population_complete": False}, "population_complete": False}


def test_inclusive_nyc_calendar_and_dst():
    frame = cohort("2025-03-08", (1, 1, 1))
    frame.loc[0, "created_date"] = "2025-03-09T04:59:59Z"  # Mar 8 local
    frame.loc[1, "created_date"] = "2025-03-09T05:00:00Z"  # Mar 9 local
    frame.loc[2, "created_date"] = "2025-03-10T03:59:59Z"  # Mar 9 local, 23-hour DST day
    selected = filter_requests(frame, "2025-03-09", "2025-03-09")
    assert len(selected) == 2
    assert selected["created_local_date"].eq(pd.Timestamp("2025-03-09")).all()
    # Both instances of the repeated fall-back hour belong to the same local day.
    frame["created_date"] = ["2025-11-02T05:30:00Z", "2025-11-02T06:30:00Z", "2025-11-03T05:00:00Z"]
    assert len(filter_requests(frame, "2025-11-02", "2025-11-02")) == 2


def test_through_date_inclusive_and_empty_category_is_not_all():
    frame = cohort(daily_counts=(2,) * 10)
    assert len(filter_requests(frame, "2025-01-01", "2025-01-10", before_date="2025-01-03")) == 6
    assert filter_requests(frame, "2025-01-01", "2025-01-10", agencies=[]).empty
    assert len(filter_requests(frame, "2025-01-01", "2025-01-10", agencies=None)) == 20
    with pytest.raises(ValueError, match="on or after"):
        filter_requests(frame, "2025-01-03", "2025-01-10", before_date="2025-01-02")


def test_missing_day_is_nan_covered_zero_is_zero_and_counts_reconcile():
    frame = cohort(daily_counts=(1, 0, 3, 2, 0))
    result = summarize_selection(frame, coverage(end="2025-01-05", missing=("2025-01-03",)), "2025-01-01", "2025-01-05")
    daily = result["daily"].set_index("date")
    assert daily.loc["2025-01-02", "count"] == 0
    assert pd.isna(daily.loc["2025-01-03", "count"])
    assert daily.loc["2025-01-03", "observed_rows"] == 3
    assert result["metrics"]["selected_rows"] == 6
    assert result["metrics"]["covered_sample_rows"] == 3
    assert result["metrics"]["mean_daily_count"] == .75
    assert result["metrics"]["unknown_days"] == 1
    assert result["agencies"]["count"].sum() == result["metrics"]["selected_rows"]
    assert "partial" in result["insights"][0]["body"]
    assert result["comparison"]["status"] == "Not Estimable"
    assert result["trend"]["status"] == "Not Estimable"


def test_zero_matching_rows_still_has_calendar_and_no_division_by_zero():
    frame = cohort()
    result = summarize_selection(frame, coverage(), "2025-01-17", "2025-01-31", agencies=[])
    assert result["metrics"]["selected_rows"] == 0
    assert result["metrics"]["mean_daily_count"] == 0
    assert result["daily"]["count"].eq(0).all()
    assert result["comparison"]["previous_count"] == 0
    assert result["comparison"]["percent_change"] is None
    assert result["metrics"]["censoring_fraction"] is None
    assert "zero" in result["comparison"]["reason"]
    assert "No requests" in result["insights"][3]["body"]


def test_previous_window_equal_calendar_length_filters_and_dst():
    frame = pd.concat([cohort("2025-03-01", (3,) * 14), cohort("2025-03-01", (1,) * 14, agency="B")], ignore_index=True)
    result = summarize_selection(frame, coverage("2025-03-01", "2025-03-14"), "2025-03-08", "2025-03-14", agencies=["A"])
    comparison = result["comparison"]
    assert comparison["calendar_days_each"] == 7
    assert comparison["previous_start"] == "2025-03-01"
    assert comparison["previous_end"] == "2025-03-07"
    assert comparison["previous_count"] == comparison["current_count"] == 21
    assert comparison["percent_change"] == 0
    assert result["selection"]["agencies"] == ["A"]


def test_missing_previous_window_suppresses_change():
    frame = cohort(daily_counts=(3,) * 14)
    result = summarize_selection(frame, coverage(end="2025-01-14", missing=("2025-01-02",)), "2025-01-08", "2025-01-14")
    assert result["comparison"]["status"] == "Not Estimable"
    assert result["comparison"]["percent_change"] is None
    assert result["comparison"]["previous_count"] is None
    assert result["comparison"]["previous_covered_days"] == 6


def test_cross_year_calendar_and_leap_year_support():
    frame = cohort("2024-12-25", (3,) * 14)
    info = coverage("2024-12-25", "2025-01-07")
    result = summarize_selection(frame, info, "2025-01-01", "2025-01-07", as_of="2025-04-01T00:00:00Z")
    assert set(available_dates(info).year) == {2024, 2025}
    assert result["comparison"]["previous_start"] == "2024-12-25"
    assert result["comparison"]["previous_count"] == 21
    leap = summarize_selection(cohort("2024-02-01", (1,) * 60), coverage("2024-02-01", "2024-03-31"),
                               "2024-03-01", "2024-03-31", as_of="2024-05-01T00:00:00Z")
    assert leap["comparison"]["calendar_days_each"] == 31
    assert leap["comparison"]["previous_start"] == "2024-01-30"
    assert leap["comparison"]["status"] == "Not Estimable"  # no Jan 30–31 coverage


def test_week_month_buckets_partial_edges_and_equal_exposure_trend():
    frame = cohort("2025-01-01", (3,) * 90)
    info = coverage("2025-01-01", "2025-03-31")
    weekly = summarize_selection(frame, info, "2025-01-01", "2025-03-31", frequency="weekly")
    assert not weekly["buckets"].iloc[0]["full_bucket"]
    assert weekly["buckets"].iloc[0]["selected_days"] == 5
    assert weekly["buckets"]["observed_rows"].sum() == 270
    assert weekly["trend"]["days_each"] == 14
    monthly = summarize_selection(frame, info, "2025-02-01", "2025-03-31", frequency="monthly")
    assert monthly["buckets"]["count"].tolist() == [84.0, 93.0]
    assert monthly["trend"]["days_each"] == 28
    assert monthly["trend"]["first_count"] == monthly["trend"]["last_count"] == 84
    assert monthly["trend"]["percent_change"] == 0
    incomplete = summarize_selection(frame, coverage("2025-01-01", "2025-03-31", ("2025-02-10",)),
                                     "2025-01-01", "2025-03-31", frequency="monthly")
    assert pd.isna(incomplete["buckets"].iloc[1]["count"])
    assert incomplete["buckets"].iloc[1]["observed_rows"] == 84


def test_insights_and_metrics_change_with_filters_without_mutating_input():
    frame = pd.concat([cohort(daily_counts=(3,) * 31), cohort(daily_counts=(1,) * 31, agency="B")], ignore_index=True)
    before = frame.copy(deep=True)
    all_groups = summarize_selection(frame, coverage(), "2025-01-01", "2025-01-31")
    only_b = summarize_selection(frame, coverage(), "2025-01-01", "2025-01-31", agencies=["B"])
    assert all_groups["metrics"]["selected_rows"] == 124
    assert only_b["metrics"]["selected_rows"] == 31
    assert all_groups["insights"] != only_b["insights"]
    assert "test-fixture" in only_b["insights"][0]["body"]
    assert "100.0%" in next(item["body"] for item in only_b["insights"] if item["title"] == "Agency mix")
    for name in ("metrics", "selection", "comparison", "trend", "insights"):
        json.dumps(only_b[name], allow_nan=False)
    pd.testing.assert_frame_equal(frame, before)


def test_short_or_low_support_windows_do_not_claim_direction():
    frame = cohort(daily_counts=(1,) * 31)
    result = summarize_selection(frame, coverage(), "2025-01-01", "2025-01-31")
    assert result["trend"]["status"] == "Not Estimable"
    assert "at least 20" in result["trend"]["reason"]
    assert result["insights"][1]["title"] == "Trend comparison unavailable"


def test_previous_window_low_counts_does_not_publish_directional_percentage():
    frame = cohort(daily_counts=(1,) * 14)
    result = summarize_selection(frame, coverage(end="2025-01-14"), "2025-01-08", "2025-01-14")
    assert result["comparison"]["current_count"] == result["comparison"]["previous_count"] == 7
    assert result["comparison"]["absolute_change"] == 0
    assert result["comparison"]["percent_change"] is None
    assert result["comparison"]["minimum_requests_each"] == 20
    assert result["insights"][1]["title"] == "Trend comparison unavailable"


def test_right_censoring_at_fixed_cutoff_ignores_closed_status():
    frame = cohort(daily_counts=(3,))
    frame.loc[0, "closed_date"] = pd.NaT
    frame.loc[1, "closed_date"] = pd.Timestamp("2025-08-01T00:00:00Z")
    result = summarize_selection(frame, coverage(end="2025-01-01"), "2025-01-01", "2025-01-01")
    assert result["metrics"]["censored_rows"] == 2
    assert result["metrics"]["censoring_fraction"] == pytest.approx(2 / 3)
    km = grouped_km(frame, group_by="all")
    assert km["summary"].iloc[0]["censored_n"] == 2
    assert km["curves"]["survival_probability"].iloc[-1] == pytest.approx(2 / 3)
    assert km["summary"].iloc[0]["median_status"] == "Not reached"
    assert km["metrics"]["as_of"] == "2025-07-01T00:00:00+00:00"


def test_partial_as_of_day_unknown_and_future_created_excluded():
    frame = cohort("2025-06-29", (1, 1, 1))
    result = summarize_selection(frame, coverage("2025-06-29", "2025-07-01"), "2025-06-29", "2025-07-01")
    assert result["metrics"]["selected_rows"] == 2
    assert result["daily"]["coverage_complete"].tolist() == [True, False, False]
    assert result["daily"]["count"].isna().sum() == 2


def test_km_filtered_grouping_and_empty_inputs():
    frame = pd.concat([cohort(daily_counts=(3,)), cohort(daily_counts=(1,), agency="B")], ignore_index=True)
    result = grouped_km(frame, group_by="agency", min_group_n=2)
    assert result["summary"]["group"].tolist() == ["A"]
    assert result["metrics"]["plotted_rows"] == 3
    empty = grouped_km(frame.iloc[:0], group_by="all")
    assert empty["curves"].empty
    assert empty["metrics"]["status"] == "Not Estimable"
    with pytest.raises(ValueError, match="group_by"):
        grouped_km(frame, group_by="status")


@pytest.mark.parametrize("marker,value", [("source_kind", "synthetic_scale"), ("replica_id", 0), ("synthetic", True)])
def test_benchmark_replicas_denied_for_all_analyses(marker, value):
    frame = cohort(daily_counts=(1,))
    frame[marker] = value
    with pytest.raises(ValueError):
        summarize_selection(frame, coverage(), "2025-01-01", "2025-01-31")
    with pytest.raises(ValueError):
        grouped_km(frame)


def test_manifest_coverage_success_only_and_strict_provenance(tmp_path, monkeypatch):
    import nyc311.analysis.explorer as explorer
    paired_frame = cohort("2024-12-31", (1, 1, 1)).assign(source_kind="official_snapshot")
    monkeypatch.setattr(explorer, "read_silver", lambda path: paired_frame)
    manifest = {"dataset_id": "erm2-nwe9", "source_kind": "official_nyc311_snapshot",
                "query_config": {"start_date": "2024-12-31", "end_date": "2025-01-03"},
                "sampling": {"population_complete": False}, "windows": [
                    {"start_date": "2024-12-31", "end_date": "2025-01-01", "status": "Success"},
                    {"start_date": "2025-01-01", "end_date": "2025-01-02", "status": "Truncated"},
                    {"start_date": "2025-01-02", "end_date": "2025-01-03", "status": "Failed"}]}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    frame, info = load_explorer_inputs("unused", path)
    assert info["covered_dates"] == ["2024-12-31"]
    assert info["years"] == [2024, 2025]
    assert len(available_dates(info)) == 3
    manifest["source_kind"] = "synthetic_scale"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="official"):
        load_explorer_inputs("unused", path)


def test_loader_refuses_fixture_or_mismatched_sample_and_calendar(tmp_path, monkeypatch):
    import nyc311.analysis.explorer as explorer
    frame = cohort(daily_counts=(1,))
    manifest = {"dataset_id": "erm2-nwe9", "source_kind": "official_nyc311_snapshot",
                "query_config": {"start_date": "2025-01-01", "end_date": "2025-01-02", "sample_suffix": "17"},
                "sampling": {"method": "systematic_numeric_id_suffix", "suffix": "17"},
                "windows": [{"start_date": "2025-01-01", "end_date": "2025-01-02", "status": "Success", "rows": 1}]}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    monkeypatch.setattr(explorer, "read_silver", lambda path: frame)
    with pytest.raises(ValueError, match="fixture"):
        load_explorer_inputs("unused", path)
    frame["source_kind"] = "official_snapshot"
    with pytest.raises(ValueError, match="sample suffix"):
        load_explorer_inputs("unused", path)
    frame["unique_key"] = "12317"
    assert load_explorer_inputs("unused", path)[1]["covered_dates"] == ["2025-01-01"]
    frame["created_date"] = "2025-01-02T12:00:00Z"
    with pytest.raises(ValueError, match="extraction calendar"):
        load_explorer_inputs("unused", path)
    frame["created_date"] = "2025-01-01T12:00:00Z"
    manifest["windows"][0]["rows"] = 0
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="row count"):
        load_explorer_inputs("unused", path)


def test_weather_recalculates_for_filtered_daily_counts_and_gaps(tmp_path):
    days = 20
    frame = cohort(daily_counts=tuple(range(1, days + 1)))
    result = summarize_selection(frame, coverage(end="2025-01-20", missing=("2025-01-05",)), "2025-01-01", "2025-01-20")
    weather = pd.DataFrame({"date": pd.date_range("2025-01-01", periods=days),
                            "temperature_2m_min": range(days), "temperature_2m_max": range(days),
                            "precipitation_sum": [0] * days, "weather_complete": [True] * 19 + [False]})
    path = tmp_path / "weather.csv"
    weather.to_csv(path, index=False)
    value = weather_selection(result["daily"], path)
    minimum = value["associations"].iloc[0]
    assert minimum["n_days"] == 18
    assert minimum["spearman_rho"] == pytest.approx(1)
    assert value["associations"].iloc[2]["status"] == "Not Estimable"  # constant rain
    assert pd.isna(value["joined"].iloc[-1]["temperature_2m_min"])
    no_groups = summarize_selection(frame, coverage(end="2025-01-20"), "2025-01-01", "2025-01-20", agencies=[])
    empty = weather_selection(no_groups["daily"], path)
    assert empty["metrics"]["status"] == "Not Estimable"
    assert empty["insights"] != value["insights"]
    json.dumps(value["metrics"], allow_nan=False)


def test_weather_short_window_stale_manifest_and_duplicate_dates(tmp_path):
    result = summarize_selection(cohort(daily_counts=(3,) * 5), coverage(end="2025-01-05"), "2025-01-01", "2025-01-05")
    path = tmp_path / "weather.csv"
    pd.DataFrame({"date": pd.date_range("2025-01-01", periods=5), "temperature_2m_min": range(5)}).to_csv(path, index=False)
    weather = weather_selection(result["daily"], path)
    assert weather["metrics"]["status"] == "Not Estimable"
    assert weather["associations"].iloc[0]["n_days"] == 5
    (tmp_path / "manifest.json").write_text(json.dumps({"status": "Failed"}))
    assert "stale" in weather_selection(result["daily"], path)["metrics"]["reason"]
    (tmp_path / "manifest.json").unlink()
    pd.DataFrame({"date": ["2025-01-01", "2025-01-01"]}).to_csv(path, index=False)
    with pytest.raises(ValueError, match="duplicate"):
        weather_selection(result["daily"], path)


def test_invalid_frequency_and_weather_minimum():
    frame = cohort(daily_counts=(1,))
    with pytest.raises(ValueError, match="frequency"):
        summarize_selection(frame, coverage(), "2025-01-01", "2025-01-01", frequency="hourly")
    with pytest.raises(ValueError, match="at least 10"):
        weather_selection(pd.DataFrame(), min_days=2)
