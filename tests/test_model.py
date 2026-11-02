import json

import numpy as np
import pandas as pd
import pytest

from nyc311.analysis.predict import predict_record
from nyc311.analysis.survival import read_silver
from nyc311.analysis.train_cox import (
    CreationEncoder, HierarchicalKMBaseline, censor_aware_concordance,
    deterministic_sample, train_cox,
)


def model_fixture():
    rows = []
    for index in range(160):
        created = pd.Timestamp("2025-01-02T00:00:00Z") + pd.Timedelta(
            days=index % 50, hours=index % 24, minutes=index % 60)
        if index >= 120:
            created = pd.Timestamp("2025-03-01T00:00:00Z") + pd.Timedelta(days=index % 28, hours=index % 24)
        closed = None if index % 5 == 0 else created + pd.Timedelta(hours=2 + index % 100, days=index % 11)
        rows.append({"unique_key": str(index), "created_date": created, "closed_date": closed,
                     "agency": ["HPD", "DSNY", "DOT"][index % 3],
                     "complaint_type": ["HEAT/HOT WATER", "Noise", "Street Condition", "Sanitation"][index % 4],
                     "borough": ["BRONX", "BROOKLYN"][index % 2], "source_kind": "synthetic_fixture"})
    return pd.DataFrame(rows)


def test_encoder_is_creation_only_and_unseen_categories_are_finite():
    frame = model_fixture()
    encoder = CreationEncoder(top_categories=2).fit(frame)
    original = encoder.transform(frame)
    modified = frame.assign(status="Closed", closed_date=pd.Timestamp("2030-01-01", tz="UTC"), duration_days=9999)
    pd.testing.assert_frame_equal(original, encoder.transform(modified))
    unseen = frame.iloc[:1].assign(agency="UNKNOWN_NEW", complaint_type="NEW", borough="NEW")
    transformed = encoder.transform(unseen)
    assert list(transformed.columns) == encoder.columns
    assert np.isfinite(transformed.to_numpy()).all()
    assert not {"status", "closed_date", "duration_days"} & set(encoder.columns)


def test_baseline_handles_unreached_median_and_no_comparable_pairs():
    frame = model_fixture().iloc[:30].assign(duration_days=15.0, event_observed=0)
    baseline = HierarchicalKMBaseline(min_group_n=2).fit(frame)
    prediction = baseline.predict_record({"agency": "UNKNOWN", "complaint_type": "UNSEEN"})
    assert prediction["predicted_median_days"] is None
    assert prediction["p_closed_7d"] == 0
    assert prediction["p_closed_30d"] is None
    scores = baseline.duration_scores(frame)
    assert np.isfinite(scores).all()
    assert censor_aware_concordance(frame, scores)["c_index"] is None


def test_concordance_direction_and_deterministic_sampling():
    labels = pd.DataFrame({"duration_days": [1, 2, 3], "event_observed": [1, 1, 1]})
    assert censor_aware_concordance(labels, [1, 2, 3])["c_index"] == 1
    assert censor_aware_concordance(labels, [3, 2, 1])["c_index"] == 0
    frame = model_fixture()
    expected = deterministic_sample(frame, 25, 311)["unique_key"].tolist()
    assert expected == deterministic_sample(frame.sample(frac=1, random_state=1), 25, 311)["unique_key"].tolist()


def test_fixture_and_scale_data_cannot_enter_official_statistics(tmp_path):
    frame = model_fixture()
    path = tmp_path / "fixture.parquet"
    frame.to_parquet(path, index=False)
    with pytest.raises(ValueError, match="provenance"):
        read_silver(path)
    assert len(read_silver(path, allow_fixture=True)) == len(frame)
    frame.assign(source_kind="official_snapshot", synthetic=True).to_parquet(path, index=False)
    with pytest.raises(ValueError, match="Synthetic"):
        read_silver(path)


def test_fit_save_predict_and_temporal_holdout(tmp_path):
    path = tmp_path / "fixture.parquet"
    model_fixture().to_parquet(path, index=False)
    cfg = {"seed": 311, "source_timezone": "America/New_York", "analysis": {
        "train_end": "2025-03-01T00:00:00Z", "train_as_of": "2025-03-01T00:00:00Z",
        "test_end": "2025-04-01T00:00:00Z", "test_as_of": "2025-07-01T00:00:00Z",
        "max_model_rows": 1000, "top_categories": 3, "penalizer": 0.1}}
    evaluation = train_cox(path, tmp_path / "artifacts", cfg, allow_fixture=True)
    assert evaluation["status"] == "Passed", evaluation
    assert evaluation["train_rows"] == 120
    assert evaluation["test_rows"] == 40
    assert 0 <= evaluation["cox"]["c_index"] <= 1
    assert 0 <= evaluation["baseline"]["c_index"] <= 1
    assert evaluation["proportional_hazards"]["status"] == "Passed"
    record = {"agency": "HPD", "complaint_type": "HEAT/HOT WATER", "borough": "BRONX",
              "created_date": "2025-03-15T12:00:00Z"}
    model_path = tmp_path / "artifacts/models/cox_model.pkl"
    prediction = predict_record(record, model_path)
    assert prediction == predict_record({**record, "closed_date": "2030-01-01", "status": "Closed"}, model_path)
    assert prediction["model_version"] == evaluation["model_version"]
    assert prediction["p_closed_90d"] is None
    assert 0 <= prediction["p_closed_7d"] <= prediction["p_closed_30d"] <= 1
    json.dumps(prediction, allow_nan=False)
    assert (tmp_path / "artifacts/metrics/cox_ph_test.csv").exists()
    assert (tmp_path / "artifacts/metrics/model_holdout_predictions.csv").exists()
    assert "median_baseline" in evaluation
    alternate = train_cox(path, tmp_path / "utc_artifacts", {**cfg, "source_timezone": "UTC"}, allow_fixture=True)
    assert alternate["status"] == "Passed", alternate
    assert alternate["model_version"] != evaluation["model_version"]
    failed_cfg = {**cfg, "analysis": {**cfg["analysis"], "train_end": "2024-01-01", "train_as_of": "2024-01-01"}}
    failed = train_cox(path, tmp_path / "artifacts", failed_cfg, allow_fixture=True)
    assert failed["status"] == "Failed"
    assert failed["retained_previous_successful_artifacts"] is True
    assert failed["active_model_version"] == evaluation["model_version"]
    assert predict_record(record, model_path)["model_version"] == evaluation["model_version"]
