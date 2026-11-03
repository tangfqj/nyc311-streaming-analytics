"""Fixed-cutoff right-censored labels and grouped Kaplan–Meier analysis."""

from pathlib import Path
import time

import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter

from nyc311.config import ROOT, load_config, log_event, write_json


def utc_timestamp(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("A fixed, valid as_of timestamp is required")
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def read_silver(silver_path=None, allow_fixture=False):
    """Read only modeling columns; benchmark replicas cannot enter analyses."""
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(silver_path or ROOT / "data/silver"), format="parquet", partitioning="hive")
    available = set(dataset.schema.names)
    required = {"unique_key", "created_date", "closed_date", "agency", "complaint_type", "borough"}
    missing = required - available
    if missing:
        raise ValueError(f"Missing silver columns: {sorted(missing)}")
    optional = {"status", "source_kind", "synthetic", "is_synthetic", "replica_id"}
    frame = dataset.to_table(columns=sorted(required | (optional & available))).to_pandas()
    if frame.empty:
        raise ValueError("Silver contains no rows")
    for column in ("synthetic", "is_synthetic"):
        if column in frame and frame[column].fillna(False).astype(str).str.lower().isin(["true", "1"]).any():
            raise ValueError("Synthetic scale-test replicas cannot enter statistical analyses")
    if "replica_id" in frame and frame["replica_id"].notna().any():
        raise ValueError("Benchmark replica data cannot enter statistical analyses")
    if "source_kind" not in frame:
        raise ValueError("Silver must include source_kind provenance")
    permitted = {"official_snapshot"} | ({"synthetic_fixture"} if allow_fixture else set())
    observed = set(frame["source_kind"].fillna("UNKNOWN").astype(str))
    if not observed <= permitted:
        raise ValueError(f"Unpermitted statistical provenance: {sorted(observed - permitted)}")
    if len(observed) != 1:
        raise ValueError("Official snapshots and synthetic fixtures must be analyzed separately")
    if frame["unique_key"].duplicated().any():
        raise ValueError("Silver unique_key must be unique")
    return frame


def build_labels(frame, as_of, duration_floor_days=1 / 1440):
    """Exclude invalid/future rows and observe closures only through ``as_of``.

    Missing closures are censored, including rows whose current status is Closed.
    A closure observed after the cutoff is also censored at the fixed cutoff.
    Silver timestamp values are already normalized UTC; naive values mean UTC.
    """
    if duration_floor_days <= 0:
        raise ValueError("duration_floor_days must be positive")
    cutoff = utc_timestamp(as_of)
    result = frame.copy()
    created = pd.to_datetime(result["created_date"], utc=True, errors="coerce", format="mixed")
    raw_closed = result["closed_date"]
    closed = pd.to_datetime(raw_closed, utc=True, errors="coerce", format="mixed")
    has_closed = raw_closed.notna() & raw_closed.astype(str).str.strip().ne("")
    invalid_created = created.isna()
    invalid_closed = has_closed & closed.isna()
    negative = created.notna() & closed.notna() & (closed < created)
    future = created.notna() & (created > cutoff)
    valid = ~(invalid_created | invalid_closed | negative | future)
    result = result.loc[valid].copy()
    result["created_date"] = created.loc[valid]
    result["closed_date"] = closed.loc[valid]
    observed = result["closed_date"].notna() & (result["closed_date"] <= cutoff)
    result["event_observed"] = observed.astype(int)
    result["end_date"] = result["closed_date"].where(observed, cutoff)
    raw_duration = (result["end_date"] - result["created_date"]).dt.total_seconds() / 86400
    result["duration_days_unfloored"] = raw_duration
    zero = raw_duration.eq(0)
    result["duration_days"] = raw_duration.mask(zero, duration_floor_days)
    statuses = frame.get("status", pd.Series("", index=frame.index)).fillna("").astype(str).str.casefold()
    known_status = statuses.str.strip().ne("")
    closed_status = statuses.eq("closed")
    metrics = {
        "as_of": cutoff.isoformat(), "input_rows": len(frame), "accepted_rows": len(result),
        "rejected_rows": int((~valid).sum()), "invalid_created_rows": int(invalid_created.sum()),
        "invalid_closed_rows": int(invalid_closed.sum()), "negative_duration_rows": int(negative.sum()),
        "future_created_rows": int(future.sum()), "closed_observed_rows": int(observed.sum()),
        "censored_rows": int((~observed).sum()),
        "censoring_fraction": float((~observed).mean()) if len(result) else None,
        "zero_duration_floored_rows": int(zero.sum()), "duration_floor_days": duration_floor_days,
        "closed_status_without_closed_date": int((closed_status & ~has_closed).sum()),
        "closed_date_with_nonclosed_status": int((known_status & ~closed_status & closed.notna()).sum()),
        "known_status_rows": int(known_status.sum()),
        "filter_count_note": "Rejection reasons may overlap; rejected_rows is the union.",
    }
    return result, metrics


def finite_number(value):
    return float(value) if value is not None and np.isfinite(value) else None


CURVE_GRID_STEP_DAYS = 0.25
CURVE_GRID_MAX_DAYS = 365.0


def resample_curve(km, step=CURVE_GRID_STEP_DAYS, maximum=CURVE_GRID_MAX_DAYS):
    """Thin a KM step function onto a fixed day grid.

    Writing one row per distinct event time makes the persisted curve file grow
    with the number of tickets; at full population that is hundreds of megabytes
    the dashboard must load. Forward fill is exact for a step function, so the
    grid loses no information between grid points.
    """
    frame = km.survival_function_.join(km.confidence_interval_)
    upper = min(float(frame.index.max()), maximum)
    grid = np.round(np.arange(0.0, upper + step, step), 6)
    aligned = frame.reindex(frame.index.union(grid)).ffill().bfill()
    return aligned.reindex(grid).reset_index()


def km_summary(frame):
    km = KaplanMeierFitter().fit(frame["duration_days"], frame["event_observed"])
    median = finite_number(km.median_survival_time_)
    return km, {
        "sample_n": len(frame), "closed_n": int(frame["event_observed"].sum()),
        "censored_n": int((1 - frame["event_observed"]).sum()),
        "censoring_fraction": float(1 - frame["event_observed"].mean()),
        "km_median_days": median, "median_status": "Reached" if median is not None else "Not reached",
        "followup_support_days": float(frame["duration_days"].max()),
    }


def run_survival(silver_path=None, output_dir=None, config=None, allow_fixture=False):
    started = time.perf_counter()
    try:
        return _run_survival(silver_path, output_dir, config, allow_fixture)
    except Exception as error:
        output = Path(output_dir or ROOT / "artifacts")
        failure = {"status": "Failed", "failure_reason": f"{type(error).__name__}: {error}",
                   "elapsed_seconds": time.perf_counter() - started,
                   "artifact_warning": "Any existing curves/summary belong to a previous successful run."}
        # Invalid attempts to write fixture science into the official artifact
        # directory must not replace official metrics even with failure output.
        if not (allow_fixture and output.resolve() == (ROOT / "artifacts").resolve()):
            write_json(output / "metrics/survival.json", failure)
        log_event("survival", **failure)
        return failure


def _run_survival(silver_path=None, output_dir=None, config=None, allow_fixture=False):
    started = time.perf_counter()
    cfg = config or load_config()
    output = Path(output_dir or ROOT / "artifacts")
    if allow_fixture and output.resolve() == (ROOT / "artifacts").resolve():
        raise ValueError("Fixture statistics require an isolated output_dir")
    frame = read_silver(silver_path, allow_fixture=allow_fixture)
    labels, metrics = build_labels(frame, cfg["analysis"]["as_of"])
    if labels.empty:
        raise ValueError("No valid rows at the configured as_of")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    (output / "figures").mkdir(parents=True, exist_ok=True)
    (output / "metrics").mkdir(parents=True, exist_ok=True)
    rows = []
    curves = []
    for column, filename in (("agency", "agency"), ("complaint_type", "complaint"), ("borough", "borough")):
        values = labels[column].fillna("UNKNOWN").astype(str)
        counts = values.value_counts()
        plotted = set(counts.index if column == "borough" else counts.head(8).index)
        fig, ax = plt.subplots(figsize=(11, 6))
        for group in counts.index:
            subset = labels.loc[values.eq(group)]
            km, summary = km_summary(subset)
            rows.append({"group_by": column, "group": group, "as_of": metrics["as_of"], **summary})
            curve = resample_curve(km)
            curve.columns = ["duration_days", "survival_probability", "lower_95", "upper_95"]
            curve["group_by"] = column
            curve["group"] = group
            curves.append(curve)
            if group in plotted:
                km.plot_survival_function(ax=ax, ci_show=True,
                    label=f"{group} (n={summary['sample_n']}, censored={summary['censoring_fraction']:.1%})")
        ax.set(title=f"Resolution survival by {column} | as of {metrics['as_of'][:10]}",
               xlabel="Days since request creation", ylabel="Probability still unresolved", ylim=(0, 1.02))
        ax.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(output / f"figures/km_by_{filename}.png", dpi=150)
        plt.close(fig)
    pd.DataFrame(rows).to_csv(output / "metrics/survival_summary.csv", index=False)
    pd.concat(curves, ignore_index=True).to_csv(output / "metrics/survival_curves.csv", index=False)
    metrics.update({"status": "Passed", "source_kinds": sorted(frame["source_kind"].unique().tolist()),
                    "elapsed_seconds": time.perf_counter() - started,
                    "confidence_level": 0.95, "group_rows": len(rows)})
    write_json(output / "metrics/survival.json", metrics)
    log_event("survival", **metrics)
    return metrics
