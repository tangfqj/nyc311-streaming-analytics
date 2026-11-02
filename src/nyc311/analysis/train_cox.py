"""Creation-only Cox model and a censor-aware hierarchical KM baseline."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import pickle
import shutil
import tempfile
import time
import warnings

import lifelines
from lifelines import CoxPHFitter, KaplanMeierFitter
from lifelines.statistics import proportional_hazard_test
from lifelines.utils import concordance_index
import numpy as np
import pandas as pd

from nyc311.config import ROOT, load_config, log_event, write_json
from nyc311.analysis.survival import build_labels, finite_number, read_silver, utc_timestamp


CATEGORICAL_FEATURES = ("agency", "complaint_type", "borough")


@dataclass
class CreationEncoder:
    """A fitted vocabulary, reference categories and calendar-only features."""

    top_categories: int = 15
    categories: dict = field(default_factory=dict)
    columns: list = field(default_factory=list)
    timezone: str = "America/New_York"

    def _encode(self, frame):
        created = pd.to_datetime(frame["created_date"], utc=True, errors="coerce", format="mixed")
        if created.isna().any():
            raise ValueError("Prediction requires a valid UTC creation timestamp")
        local = created.dt.tz_convert(self.timezone)
        encoded = pd.DataFrame(index=frame.index)
        for name, values, period in (("month", local.dt.month - 1, 12),
                                     ("day_of_week", local.dt.dayofweek, 7),
                                     ("hour", local.dt.hour + local.dt.minute / 60, 24)):
            encoded[f"{name}_sin"] = np.sin(2 * np.pi * values / period)
            encoded[f"{name}_cos"] = np.cos(2 * np.pi * values / period)
        for name in CATEGORICAL_FEATURES:
            values = frame[name].fillna("UNKNOWN").astype(str)
            levels = self.categories[name]
            # The most common fitted category is the omitted reference level.
            for number, level in enumerate(levels[1:]):
                encoded[f"{name}__{number}"] = values.eq(level).astype(float)
            encoded[f"{name}__other"] = (~values.isin(levels)).astype(float)
        return encoded.astype(float)

    def fit(self, frame):
        if self.top_categories < 1:
            raise ValueError("top_categories must be at least one")
        for name in CATEGORICAL_FEATURES:
            values = frame[name].fillna("UNKNOWN").astype(str)
            counts = values.value_counts().rename_axis("value").reset_index(name="count")
            counts = counts.sort_values(["count", "value"], ascending=[False, True])
            self.categories[name] = counts["value"].head(self.top_categories).tolist()
        encoded = self._encode(frame)
        self.columns = encoded.columns[encoded.var(ddof=0).gt(1e-10)].tolist()
        return self

    def transform(self, frame):
        if not self.categories:
            raise ValueError("Encoder has not been fitted")
        return self._encode(frame).reindex(columns=self.columns, fill_value=0.0)

    def fit_transform(self, frame):
        return self.fit(frame).transform(frame)


def chronological_split(frame, analysis):
    train_end = utc_timestamp(analysis["train_end"])
    train_as_of = utc_timestamp(analysis["train_as_of"])
    test_end = utc_timestamp(analysis["test_end"])
    test_as_of = utc_timestamp(analysis["test_as_of"])
    if not (train_as_of <= train_end < test_end <= test_as_of):
        raise ValueError("Require train_as_of <= train_end < test_end <= test_as_of")
    created = pd.to_datetime(frame["created_date"], utc=True, errors="coerce", format="mixed")
    train_mask = created < train_end
    test_mask = (created >= train_end) & (created < test_end)
    outside_mask = created.notna() & ~(train_mask | test_mask)
    train, train_metrics = build_labels(frame.loc[train_mask], train_as_of)
    test, test_metrics = build_labels(frame.loc[test_mask], test_as_of)
    if not train.empty and not test.empty and train["created_date"].max() >= test["created_date"].min():
        raise ValueError("Chronological train/test overlap")
    accounted_rows = int(train_mask.sum() + test_mask.sum() + outside_mask.sum() + created.isna().sum())
    if accounted_rows != len(frame):
        raise ValueError("Chronological split row accounting failed")
    return train, test, {"train": train_metrics, "test": test_metrics,
        "input_rows": len(frame), "accounted_rows": accounted_rows,
        "invalid_created_outside_split": int(created.isna().sum()),
        "excluded_outside_split_rows": int(outside_mask.sum()),
        "outside_split_reasons": {"created_at_or_after_test_end": int((created >= test_end).sum())},
        "outside_split_created_min": created.loc[outside_mask].min().isoformat() if outside_mask.any() else None,
        "outside_split_created_max": created.loc[outside_mask].max().isoformat() if outside_mask.any() else None,
        "boundaries": {"timezone": "UTC", "train_created_upper_exclusive": train_end.isoformat(),
            "train_observation_cutoff_inclusive": train_as_of.isoformat(),
            "test_created_lower_inclusive": train_end.isoformat(), "test_created_upper_exclusive": test_end.isoformat(),
            "test_observation_cutoff_inclusive": test_as_of.isoformat(),
            "test_end_new_york_equivalent": test_end.tz_convert("America/New_York").isoformat()},
        "outside_split_note": "Ingestion date windows use America/New_York; explicit UTC model boundaries may exclude the last NYC evening. Excluded rows remain in full silver/KM analyses."}


def deterministic_sample(frame, limit, seed):
    if limit < 1:
        raise ValueError("max_model_rows must be positive")
    if len(frame) <= limit:
        return frame.sort_values("unique_key").reset_index(drop=True)
    ranks = frame["unique_key"].astype(str).map(
        lambda key: hashlib.sha256(f"{seed}:{key}".encode()).hexdigest())
    return frame.loc[ranks.sort_values(kind="stable").index[:limit]].sort_values("unique_key").reset_index(drop=True)


class HierarchicalKMBaseline:
    """Agency x complaint -> complaint -> global KM, fitted on train only."""

    def __init__(self, min_group_n=20):
        self.min_group_n = min_group_n
        self.groups = {}

    def fit(self, frame):
        self.groups[()] = KaplanMeierFitter().fit(frame["duration_days"], frame["event_observed"])
        self.support = {(): float(frame["duration_days"].max())}
        for columns in (["complaint_type"], ["agency", "complaint_type"]):
            normalized = frame.copy()
            for column in columns:
                normalized[column] = normalized[column].fillna("UNKNOWN").astype(str)
            grouper = columns[0] if len(columns) == 1 else columns
            for key, group in normalized.groupby(grouper, sort=True):
                if len(group) < self.min_group_n:
                    continue
                key = (key,) if len(columns) == 1 else tuple(key)
                self.groups[key] = KaplanMeierFitter().fit(group["duration_days"], group["event_observed"])
                self.support[key] = float(group["duration_days"].max())
        self.score_horizon_days = min(30.0, min(self.support.values()))
        return self

    def key_for(self, record):
        complaint = str(record.get("complaint_type") or "UNKNOWN")
        agency = str(record.get("agency") or "UNKNOWN")
        for key in ((agency, complaint), (complaint,), ()):
            if key in self.groups:
                return key
        raise ValueError("Baseline has not been fitted")

    def duration_scores(self, frame):
        # Higher survival at a common supported horizon predicts a longer time.
        scores = {key: float(km.predict(self.score_horizon_days)) for key, km in self.groups.items()}
        return np.asarray([scores[self.key_for(row)] for row in frame.to_dict("records")])

    def predict_record(self, record):
        key = self.key_for(record)
        km = self.groups[key]
        support = self.support[key]
        result = {"predicted_median_days": finite_number(km.median_survival_time_),
                  "horizon_support_days": support,
                  "fallback_level": {0: "global", 1: "complaint_type", 2: "agency_x_complaint_type"}[len(key)]}
        for horizon in (7, 30, 90):
            result[f"p_closed_{horizon}d"] = float(1 - km.predict(horizon)) if horizon <= support else None
        return result


def censor_aware_concordance(frame, predicted_duration_scores):
    try:
        value = concordance_index(frame["duration_days"], predicted_duration_scores, frame["event_observed"])
        return {"c_index": finite_number(value), "status": "Passed"}
    except (ZeroDivisionError, ValueError) as error:
        return {"c_index": None, "status": "Not estimable", "reason": str(error)}


def _atomic_pickle(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".pkl.tmp")
    with temporary.open("wb") as stream:
        pickle.dump(value, stream, protocol=pickle.HIGHEST_PROTOCOL)
    temporary.replace(path)


def train_cox(silver_path=None, output_dir=None, config=None, allow_fixture=False):
    started = time.perf_counter()
    cfg = config or load_config()
    analysis = cfg["analysis"]
    output = Path(output_dir or ROOT / "artifacts")
    if allow_fixture and output.resolve() == (ROOT / "artifacts").resolve():
        raise ValueError("Fixture models require an isolated output_dir")
    metrics_path = output / "metrics/model_evaluation.json"
    publish_output = output
    staging = tempfile.TemporaryDirectory(prefix="nyc311-cox-")
    output = Path(staging.name)
    evaluation = {"status": "Failed", "split": {key: analysis[key] for key in (
        "train_end", "train_as_of", "test_end", "test_as_of")},
        "sampling_method": "Deterministic SHA256(seed:unique_key), separately within train/test",
        "seed": cfg.get("seed", 311), "creation_features": list(CATEGORICAL_FEATURES) + ["month", "day_of_week", "hour"],
        "feature_timezone": cfg.get("source_timezone", "America/New_York"),
        "feature_provenance_limitation": "Agency/type/borough are from the downloaded snapshot; historical revisions are unavailable.",
        "c_index_note": "Harrell concordance with observed/censored labels; higher predicted duration scores mean longer survival.",
        "probability_note": "Horizons beyond maximum training follow-up are null; no unsupported tail extrapolation.",
    }
    try:
        frame = read_silver(silver_path, allow_fixture=allow_fixture)
        evaluation["source_kinds"] = sorted(frame["source_kind"].unique().tolist())
        train_all, test_all, label_metrics = chronological_split(frame, analysis)
        evaluation["label_metrics"] = label_metrics
        evaluation["excluded_outside_split_rows"] = label_metrics["excluded_outside_split_rows"]
        evaluation["split_time_boundaries"] = label_metrics["boundaries"]
        limit = int(analysis.get("max_model_rows", 30000))
        train = deterministic_sample(train_all, limit, cfg.get("seed", 311))
        test = deterministic_sample(test_all, limit, cfg.get("seed", 311))
        evaluation.update({"silver_rows": len(frame), "train_available_rows": len(train_all), "test_available_rows": len(test_all),
                           "train_rows": len(train), "test_rows": len(test),
                           "train_sampling_fraction": len(train) / len(train_all) if len(train_all) else None,
                           "test_sampling_fraction": len(test) / len(test_all) if len(test_all) else None,
                           "train_censoring_fraction": float(1 - train["event_observed"].mean()) if len(train) else None,
                           "test_censoring_fraction": float(1 - test["event_observed"].mean()) if len(test) else None})
        if len(train) < 10 or train["event_observed"].sum() < 5:
            raise ValueError("Cox requires at least ten training rows and five observed closures")
        if len(test) < 2:
            raise ValueError("Chronological holdout requires at least two test rows")
        encoder = CreationEncoder(int(analysis.get("top_categories", 15)), timezone=cfg.get("source_timezone", "America/New_York"))
        features = encoder.fit_transform(train)
        if features.shape[1] == 0:
            raise ValueError("No varying creation-time features in training data")
        fit_frame = features.assign(duration_days=train["duration_days"].to_numpy(), event_observed=train["event_observed"].to_numpy())
        cox = CoxPHFitter(penalizer=float(analysis.get("penalizer", 0.1)))
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            cox.fit(fit_frame, duration_col="duration_days", event_col="event_observed", show_progress=False)
        baseline = HierarchicalKMBaseline(int(analysis.get("baseline_min_group_n", 20))).fit(train)
        evaluation["fit_warnings"] = [str(item.message) for item in caught]
        evaluation["encoded_feature_count"] = len(encoder.columns)
        evaluation["encoded_features"] = encoder.columns
        evaluation["cox"] = censor_aware_concordance(test, -cox.predict_partial_hazard(encoder.transform(test)).to_numpy())
        evaluation["baseline"] = censor_aware_concordance(test, baseline.duration_scores(test))
        evaluation["baseline"].update({"method": "Hierarchical grouped Kaplan–Meier survival score",
                                       "minimum_group_n": baseline.min_group_n,
                                       "score_horizon_days": baseline.score_horizon_days,
                                       "fitted_groups": len(baseline.groups)})
        baseline_predictions = [baseline.predict_record(row) for row in test.to_dict("records")]
        baseline_medians = np.asarray([row["predicted_median_days"] if row["predicted_median_days"] is not None else np.nan
                                       for row in baseline_predictions])
        reached = np.isfinite(baseline_medians)
        evaluation["median_baseline"] = censor_aware_concordance(test.loc[reached], baseline_medians[reached])
        evaluation["median_baseline"].update({"method": "Hierarchical grouped KM median (requested baseline)",
            "evaluated_rows": int(reached.sum()), "unreached_median_rows": int((~reached).sum()),
            "limitation": "Only tickets with finite training-group KM medians are evaluable; full-cohort KM survival-score baseline is also reported."})
        (output / "metrics").mkdir(parents=True, exist_ok=True)
        holdout = test[["unique_key", "created_date", "duration_days", "event_observed"]].copy()
        holdout["cox_predicted_duration_score"] = -cox.predict_partial_hazard(encoder.transform(test)).to_numpy()
        holdout["km_survival_duration_score"] = baseline.duration_scores(test)
        holdout["km_predicted_median_days"] = baseline_medians
        holdout.to_csv(output / "metrics/model_holdout_predictions.csv", index=False)
        cox.summary.to_csv(output / "metrics/cox_coefficients.csv")
        try:
            ph = proportional_hazard_test(cox, fit_frame, time_transform="rank").summary
            ph.to_csv(output / "metrics/cox_ph_test.csv")
            p_values = ph["p"].replace([np.inf, -np.inf], np.nan).dropna()
            evaluation["proportional_hazards"] = {
                "status": "Passed", "test": "Scaled Schoenfeld residual rank-time test",
                "tested_features": len(p_values), "p_lt_0_05_count": int(p_values.lt(0.05).sum()),
                "bonferroni_alpha": 0.05 / len(p_values) if len(p_values) else None,
                "bonferroni_violation_count": int(p_values.lt(0.05 / len(p_values)).sum()) if len(p_values) else 0,
                "minimum_p": finite_number(p_values.min()) if len(p_values) else None,
                "limitation": "Exploratory diagnostic; rejection signals a Cox proportional-hazards limitation.",
            }
        except Exception as error:
            evaluation["proportional_hazards"] = {"status": "Failed", "reason": f"{type(error).__name__}: {error}"}
        signature = hashlib.sha256()
        signature.update(json.dumps({"analysis": analysis, "encoder": encoder.categories, "features": encoder.columns,
                                     "feature_timezone": encoder.timezone,
                                     "lifelines": lifelines.__version__, "seed": cfg.get("seed", 311)}, sort_keys=True).encode())
        signature.update(train[["unique_key", "created_date", "duration_days", "event_observed", *CATEGORICAL_FEATURES]].to_json(date_format="iso", orient="records").encode())
        version = "cox-" + signature.hexdigest()[:16]
        evaluation.update({"status": "Passed", "model_version": version,
                           "trained_at_utc": datetime.now(timezone.utc).isoformat(),
                           "lifelines_version": lifelines.__version__,
                           "horizon_support_days": float(train["duration_days"].max()),
                           "train_created_min": train["created_date"].min().isoformat(),
                           "train_created_max": train["created_date"].max().isoformat(),
                           "test_created_min": test["created_date"].min().isoformat(),
                           "test_created_max": test["created_date"].max().isoformat()})
        bundle = {"model": cox, "encoder": encoder, "baseline": baseline, "metadata": evaluation.copy(),
                  "model_version": version, "horizon_support_days": evaluation["horizon_support_days"]}
        _atomic_pickle(output / f"models/{version}.pkl", bundle)
        _atomic_pickle(output / "models/cox_model.pkl", bundle)
        write_json(output / "models/model_metadata.json", evaluation)
        from nyc311.analysis.predict import predict_record
        first_test = test.iloc[0].to_dict()
        example = {key: first_test[key] for key in CATEGORICAL_FEATURES}
        example.update({"request_id": "DEMO_UNSEEN_CREATION", "event_type": "CREATED",
                        "created_date": (first_test["created_date"] + pd.Timedelta(minutes=1)).isoformat()})
        write_json(output / "metrics/prediction_example.json", {
            "demonstration_kind": "Synthetic unseen input using observed categories; excluded from all statistics",
            "input": example,
            "prediction": predict_record(example, output / "models/cox_model.pkl")})
        _plot_evaluation(evaluation, output)
        # Generate and verify every artifact before replacing any current model.
        # Each file is then published atomically; metadata identifies the model.
        for source in sorted(output.rglob("*")):
            if source.is_file():
                target = publish_output / source.relative_to(output)
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_name(target.name + ".publishing")
                shutil.copyfile(source, temporary)
                temporary.replace(target)
        evaluation["active_model_version"] = version
    except Exception as error:
        evaluation["status"] = "Failed"
        evaluation["failure_reason"] = f"{type(error).__name__}: {error}"
        previous_metadata = publish_output / "models/model_metadata.json"
        if previous_metadata.exists():
            previous = json.loads(previous_metadata.read_text())
            evaluation["active_model_version"] = previous.get("model_version")
            evaluation["retained_previous_successful_artifacts"] = True
            evaluation["artifact_warning"] = "Current training failed; any existing model/figures belong to the previous successful model_version."
    finally:
        staging.cleanup()
    evaluation["elapsed_seconds"] = time.perf_counter() - started
    write_json(metrics_path, evaluation)
    log_event("train_cox", **evaluation)
    return evaluation


def _plot_evaluation(evaluation, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for position, key in enumerate(("baseline", "cox")):
        value = evaluation[key]["c_index"]
        if value is not None:
            axes[0].bar(position, value, color=("#6b7280", "#2563eb")[position])
            axes[0].text(position, value + 0.015, f"{value:.3f}", ha="center")
        else:
            axes[0].text(position, 0.5, "Not estimable", ha="center", rotation=90)
    axes[0].set(xticks=[0, 1], xticklabels=["Grouped KM baseline", "CoxPH"], ylim=(0, 1.05), ylabel="Holdout Harrell C-index")
    axes[0].axhline(0.5, ls="--", color="gray", linewidth=1)
    axes[1].bar(["Train", "Test"], [evaluation["train_rows"], evaluation["test_rows"]], label="All rows")
    axes[1].bar(["Train", "Test"], [evaluation["train_rows"] * evaluation["train_censoring_fraction"],
                                     evaluation["test_rows"] * evaluation["test_censoring_fraction"]], label="Right censored")
    axes[1].set(ylabel="Selected ticket count", title="Chronological cohorts")
    axes[1].legend()
    fig.tight_layout()
    (output / "figures").mkdir(parents=True, exist_ok=True)
    fig.savefig(output / "figures/model_evaluation.png", dpi=150)
    plt.close(fig)
