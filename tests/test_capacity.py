import pandas as pd

from nyc311.analysis.capacity import run_capacity, simulate_fifo


def _config():
    return {"analysis": {"as_of": "2025-02-01T00:00:00Z"}, "source_timezone": "America/New_York",
            "capacity": {"min_group_n": 1, "top_n": 5, "uplifts": [0, .1, .2, .3]}}


def _write(tmp_path, created, closed):
    frame = pd.DataFrame({"unique_key": [str(i) for i in range(len(created))], "created_date": created,
        "closed_date": closed, "agency": "HPD", "borough": "BRONX", "complaint_type": "HEAT/HOT WATER",
        "source_kind": "synthetic_fixture"})
    file = tmp_path / "silver.parquet"
    frame.to_parquet(file)
    return file


def test_all_censored_queue_does_not_report_closed_only_median(tmp_path):
    silver = _write(tmp_path, ["2025-01-01T12:00:00Z", "2025-01-02T12:00:00Z"], [None, "2025-03-01T00:00:00Z"])
    result = run_capacity(silver, tmp_path / "out", config=_config(), allow_fixture=True)
    rows = pd.read_csv(tmp_path / "out/metrics/capacity_scenarios.csv")
    assert result["censoring_rate"] == 1
    assert rows.observed_km_median_days.isna().all()
    assert rows.baseline_median_days.isna().all()
    assert rows.modelled_delta_days.isna().all()
    assert (rows.calibration_status == "Failed").all()
    assert (rows.scenario_end_backlog == 2).all()


def test_failed_fifo_calibration_suppresses_precision(tmp_path):
    silver = _write(tmp_path, ["2025-01-01T12:00:00Z"] * 10 + ["2025-01-15T12:00:00Z"] * 11,
                    ["2025-01-30T12:00:00Z"] * 10 + ["2025-01-16T12:00:00Z"] * 11)
    result = run_capacity(silver, tmp_path / "out", config=_config(), allow_fixture=True)
    rows = pd.read_csv(tmp_path / "out/metrics/capacity_scenarios.csv")
    assert result["groups_failed_calibration"] == 1
    assert rows.modelled_delta_days.isna().all()
    assert rows.scenario_median_days.notna().all()
    assert (rows.observed_km_median_days == 1).all()
    assert (rows.baseline_median_days > 10).all()


def test_zero_duration_observations_remain_finite(tmp_path):
    silver = _write(tmp_path, ["2025-01-01T12:00:00Z"] * 4, ["2025-01-01T12:00:00Z"] * 4)
    result = run_capacity(silver, tmp_path / "out", config=_config(), allow_fixture=True)
    rows = pd.read_csv(tmp_path / "out/metrics/capacity_scenarios.csv")
    assert result["groups_calibrated"] == 1
    assert (rows.observed_km_median_days == 0).all()
    assert (rows.baseline_median_days >= 0).all()
    assert rows.modelled_delta_days.notna().all()
    assert rows.external_initial_backlog.isna().all()
    assert rows.assumptions.str.contains("not causal").all()


def test_fifo_fractional_capacity_and_backlog_accounting():
    group = pd.DataFrame({"unique_key": [str(i) for i in range(10)],
        "created_date": pd.to_datetime(["2025-01-01T12:00:00Z"] * 10, utc=True),
        "closed_date": pd.to_datetime([f"2025-01-{d:02d}T12:00:00Z" for d in range(2, 12)], utc=True)})
    baseline, trace = simulate_fifo(group, "2025-01-12T00:00:00Z", 0)
    scenario, future = simulate_fifo(group, "2025-01-12T00:00:00Z", .3)
    assert len(baseline) == 10
    assert baseline.event_observed.all()
    assert scenario.duration_days.median() <= baseline.duration_days.median()
    assert (trace.cohort_backlog == (trace.arrivals - trace.served).cumsum()).all()
    assert (future.cohort_backlog >= 0).all()
    assert future.modelled_service_opportunities.sum() == 13


def test_benchmark_source_kind_cannot_enter_capacity(tmp_path):
    import pytest
    silver = _write(tmp_path, ["2025-01-01T12:00:00Z"], [None])
    frame = pd.read_parquet(silver)
    frame["source_kind"] = "synthetic_scale_replica"
    frame.to_parquet(silver)
    with pytest.raises(ValueError, match="provenance"):
        run_capacity(silver, tmp_path / "out", config=_config())


def test_insufficient_followup_cannot_calibrate_30day_survival(tmp_path):
    silver = _write(tmp_path, ["2025-01-01T12:00:00Z"] * 10,
                    ["2025-01-02T12:00:00Z"] * 6 + [None] * 4)
    config = _config()
    config["analysis"]["as_of"] = "2025-01-04T00:00:00Z"
    run_capacity(silver, tmp_path / "out", config=config, allow_fixture=True)
    rows = pd.read_csv(tmp_path / "out/metrics/capacity_scenarios.csv")
    assert rows.observed_km_median_days.notna().all()
    assert rows.observed_p_unresolved_30d.isna().all()
    assert rows.modelled_delta_days.isna().all()
    assert rows.calibration_reason.str.contains("unsupported by follow-up").all()
