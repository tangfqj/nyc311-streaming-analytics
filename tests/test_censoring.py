import pandas as pd
import pytest

from nyc311.analysis.survival import build_labels
from nyc311.analysis.train_cox import chronological_split


def test_right_censoring_fixed_cutoff_and_invalid_rows():
    frame = pd.DataFrame([
        {"unique_key": "open", "created_date": "2025-01-01", "closed_date": None, "status": "Closed"},
        {"unique_key": "future_close", "created_date": "2025-01-01", "closed_date": "2025-03-01", "status": "Closed"},
        {"unique_key": "closed", "created_date": "2025-01-01", "closed_date": "2025-01-03", "status": "Open"},
        {"unique_key": "zero", "created_date": "2025-01-01", "closed_date": "2025-01-01", "status": "Closed"},
        {"unique_key": "negative", "created_date": "2025-01-02", "closed_date": "2025-01-01"},
        {"unique_key": "invalid", "created_date": "2025-01-01", "closed_date": "bad"},
        {"unique_key": "future", "created_date": "2025-04-01", "closed_date": None},
        {"unique_key": "bad_created", "created_date": "bad", "closed_date": None},
    ])
    labels, metrics = build_labels(frame, "2025-02-01T00:00:00Z")
    labels = labels.set_index("unique_key")
    assert set(labels.index) == {"open", "future_close", "closed", "zero"}
    assert labels.loc["open", "duration_days"] == 31
    assert labels.loc["future_close", "duration_days"] == 31
    assert labels.loc["future_close", "event_observed"] == 0
    assert labels.loc["closed", "duration_days"] == 2
    assert labels.loc["closed", "event_observed"] == 1
    assert labels.loc["zero", "duration_days"] == pytest.approx(1 / 1440)
    assert labels.loc["zero", "duration_days_unfloored"] == 0
    assert metrics["input_rows"] == metrics["accepted_rows"] + metrics["rejected_rows"]
    assert metrics["zero_duration_floored_rows"] == 1
    assert metrics["closed_status_without_closed_date"] == 1
    assert metrics["closed_date_with_nonclosed_status"] >= 1
    assert metrics["censoring_fraction"] == 0.5


def test_cutoff_inclusive_closure_and_zero_age_censor():
    labels, metrics = build_labels(pd.DataFrame({
        "created_date": ["2025-02-01", "2025-01-31"],
        "closed_date": [None, "2025-02-01"]}), "2025-02-01")
    assert labels["event_observed"].tolist() == [0, 1]
    assert labels["duration_days"].tolist() == [1 / 1440, 1]
    assert metrics["zero_duration_floored_rows"] == 1


def test_temporal_split_truncates_training_closures():
    frame = pd.DataFrame({"unique_key": ["train", "test", "outside"],
        "created_date": ["2025-02-01", "2025-03-01", "2025-04-01"],
        "closed_date": ["2025-05-01", "2025-03-11", None]})
    cfg = {"train_end": "2025-03-01", "train_as_of": "2025-03-01",
           "test_end": "2025-04-01", "test_as_of": "2025-07-01"}
    train, test, metrics = chronological_split(frame, cfg)
    assert train["unique_key"].tolist() == ["train"]
    assert train["event_observed"].tolist() == [0]
    assert train["duration_days"].tolist() == [28]
    assert test["unique_key"].tolist() == ["test"]
    assert test["event_observed"].tolist() == [1]
    assert metrics["excluded_outside_split_rows"] == 1
    assert metrics["outside_split_reasons"] == {"created_at_or_after_test_end": 1}
    assert metrics["accounted_rows"] == metrics["input_rows"] == 3
    assert metrics["boundaries"]["timezone"] == "UTC"
    assert metrics["boundaries"]["test_created_upper_exclusive"] == "2025-04-01T00:00:00+00:00"
    assert metrics["boundaries"]["test_end_new_york_equivalent"] == "2025-03-31T20:00:00-04:00"
    with pytest.raises(ValueError, match="Require"):
        chronological_split(frame, {**cfg, "train_as_of": "2025-03-02"})
