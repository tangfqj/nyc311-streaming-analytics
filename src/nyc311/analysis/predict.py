"""Small, cached, creation-only inference shared by CLI and stream sink."""

from functools import lru_cache
import math
from pathlib import Path
import pickle

import numpy as np
import pandas as pd

from nyc311.config import ROOT
from nyc311.analysis.survival import finite_number


@lru_cache(maxsize=4)
def _load_bundle(path, modified_ns, size):
    # Only load trusted models created locally by train_cox; pickle is executable.
    with Path(path).open("rb") as stream:
        return pickle.load(stream)


def load_bundle(model_path=None):
    path = Path(model_path or ROOT / "artifacts/models/cox_model.pkl").resolve()
    stat = path.stat()
    return _load_bundle(str(path), stat.st_mtime_ns, stat.st_size)


def predict_record(record, model_path=None):
    bundle = load_bundle(model_path)
    created = record.get("created_date") or record.get("created_at") or record.get("event_time")
    if created is None:
        raise ValueError("A CREATED record requires created_date or created_at")
    # Construct a new frame so final status/closure and arbitrary incoming fields
    # cannot affect either the fitted encoder or the prediction.
    frame = pd.DataFrame([{
        "created_date": created,
        "agency": record.get("agency") or "UNKNOWN",
        "complaint_type": record.get("complaint_type") or "UNKNOWN",
        "borough": record.get("borough") or "UNKNOWN",
    }])
    features = bundle["encoder"].transform(frame)
    model = bundle["model"]
    partial_hazard = float(model.predict_partial_hazard(features).iloc[0])
    if not math.isfinite(partial_hazard) or partial_hazard <= 0:
        raise ValueError("Non-finite Cox partial hazard")
    cumulative = model.baseline_cumulative_hazard_.iloc[:, 0]
    threshold = math.log(2) / partial_hazard
    position = int(np.searchsorted(cumulative.to_numpy(), threshold, side="left"))
    median = finite_number(cumulative.index[position]) if position < len(cumulative) else None
    support = float(bundle["horizon_support_days"])
    supported = [horizon for horizon in (7, 30, 90) if horizon <= support]
    probabilities = {}
    if supported:
        survival = model.predict_survival_function(features, times=supported).iloc[:, 0]
        probabilities = {int(horizon): float(np.clip(1 - value, 0, 1)) for horizon, value in survival.items()}
    result = {
        "model_version": bundle["model_version"], "predicted_median_days": median,
        "median_status": "Reached" if median is not None else "Not reached",
        "horizon_support_days": support, "prediction_status": "Predicted",
        "training_source_kinds": bundle["metadata"].get("source_kinds", []),
    }
    for horizon in (7, 30, 90):
        result[f"p_closed_{horizon}d"] = probabilities.get(horizon)
    return result
