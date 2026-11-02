"""Calendar-complete sample trends with historical-only surge references."""
import json
from pathlib import Path
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

from nyc311.analysis.survival import read_silver
from nyc311.config import ROOT, load_config, log_event, write_json

ALL = "__ALL__"
COUNT_LABEL = "Sample requests (not city totals)"


def _read_silver(silver_path, config, allow_fixture=False):
    frame = read_silver(silver_path, allow_fixture=allow_fixture).copy()
    frame["created_date"] = pd.to_datetime(frame["created_date"], utc=True, errors="coerce")
    frame["closed_date"] = pd.to_datetime(frame.get("closed_date", pd.Series(pd.NaT, index=frame.index)), utc=True, errors="coerce")
    as_of = pd.Timestamp(config["analysis"]["as_of"])
    as_of = as_of.tz_localize("UTC") if as_of.tzinfo is None else as_of.tz_convert("UTC")
    invalid = frame["created_date"].isna() | (frame["created_date"] > as_of)
    frame = frame.loc[~invalid].copy()
    for name in ("agency", "borough", "complaint_type"):
        frame[name] = frame[name].fillna("UNKNOWN").astype(str)
    tz = config.get("source_timezone", "America/New_York")
    frame["date"] = frame["created_date"].dt.tz_convert(tz).dt.tz_localize(None).dt.normalize()
    return frame, {"excluded_synthetic": 0, "invalid_or_future_created": int(invalid.sum())}


def _coverage_dates(config, frame):
    settings = config.get("seasonality", {})
    manifest_path = settings.get("coverage_manifest")
    manifest = {}
    if manifest_path and Path(manifest_path).exists():
        manifest = json.loads(Path(manifest_path).read_text())
    query = manifest.get("query_config", {})
    start = settings.get("start_date", query.get("start_date", config.get("ingest", {}).get("start")))
    end = settings.get("end_date", query.get("end_date", config.get("ingest", {}).get("end")))
    if start is None:
        start = frame["date"].min()
    if end is None:
        end = frame["date"].max() + pd.Timedelta(days=1)
    cutoff = pd.Timestamp(config["analysis"]["as_of"])
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    last_full_day_end = cutoff.tz_convert(config.get("source_timezone", "America/New_York")).tz_localize(None).normalize()
    effective_end = min(pd.Timestamp(end).normalize(), last_full_day_end)
    calendar = pd.date_range(pd.Timestamp(start).normalize(), effective_end, freq="D", inclusive="left")
    covered = set()
    if "coverage_dates" in settings:
        covered = set(pd.to_datetime(settings["coverage_dates"]).normalize())
    else:
        for window in manifest.get("windows", []):
            if window.get("status") == "Success":
                covered.update(pd.date_range(window["start_date"], window["end_date"], inclusive="left", freq="D"))
    metadata = {
        "coverage_manifest": str(manifest_path) if manifest_path else None,
        "coverage_semantics": "Complete extraction of selected systematic ID-suffix sample; never population-complete",
        "sampling": manifest.get("sampling", {"population_complete": False, "sampling_status": "unverified"}),
        "source_kind": sorted(frame["source_kind"].dropna().astype(str).unique().tolist()) if "source_kind" in frame else ["unverified_input"],
        "start_date": calendar.min().date().isoformat() if len(calendar) else None,
        "requested_end_date_exclusive": pd.Timestamp(end).date().isoformat(),
        "end_date_exclusive": effective_end.date().isoformat(),
        "partial_as_of_local_day_excluded": True,
    }
    return calendar, covered, metadata


def add_historical_baseline(daily, history_weeks=8, min_history=4, z_threshold=3.0, min_count=20, min_excess=5):
    """Estimate each weekday from strictly previous same-weekday covered days."""
    frame = daily.sort_values(["complaint_type", "borough", "date"]).copy()
    frame["weekday"] = frame["date"].dt.dayofweek
    groups = frame.groupby(["complaint_type", "borough", "weekday"], sort=False)["count"]
    frame["baseline_mean"] = groups.transform(lambda s: s.shift(1).rolling(history_weeks, min_periods=min_history).mean())
    frame["baseline_std"] = groups.transform(lambda s: s.shift(1).rolling(history_weeks, min_periods=min_history).std())
    frame["baseline_n"] = groups.transform(lambda s: s.shift(1).rolling(history_weeks, min_periods=1).count()).fillna(0).astype(int)
    frame["z_score"] = ((frame["count"] - frame["baseline_mean"]) / frame["baseline_std"].where(frame["baseline_std"] > 0))
    excess = frame["count"] - frame["baseline_mean"]
    varied_surge = frame["z_score"] >= z_threshold
    constant_surge = (frame["baseline_std"] == 0) & (excess >= min_excess)
    frame["is_surge"] = (frame["coverage_complete"] & (frame["baseline_n"] >= min_history)
                         & (frame["count"] >= min_count) & (excess >= min_excess)
                         & (varied_surge | constant_surge)).fillna(False)
    frame["surge_method"] = np.where(frame["baseline_std"] == 0, "historical_weekday_constant_plus_minimum_excess", "historical_weekday_zscore")
    frame["rolling_mean_7d"] = frame.groupby(["complaint_type", "borough"])["count"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    return frame


def _association(frame, target, variable):
    pairs = frame[[target, variable, "month"]].dropna()
    result = {"target": target, "weather_variable": variable, "n_days": len(pairs),
              "spearman_rho": None, "spearman_p_value": None, "month_controlled_rank_correlation": None,
              "month_controlled_p_value": None, "status": "Not Run", "interpretation": "Observational sample association; not causal"}
    if len(pairs) < 10 or pairs[target].nunique() < 2 or pairs[variable].nunique() < 2:
        result["reason"] = "Requires at least 10 covered paired days and variation in both variables"
        return result
    rho = stats.spearmanr(pairs[target], pairs[variable])
    result.update({"spearman_rho": float(rho.statistic), "spearman_p_value": float(rho.pvalue), "status": "Success"})
    ranks = pairs[[target, variable]].rank().to_numpy(dtype=float)
    dummies = pd.get_dummies(pairs["month"].astype(str), drop_first=True, dtype=float).to_numpy()
    design = np.column_stack([np.ones(len(pairs)), dummies])
    residual = ranks - design @ np.linalg.lstsq(design, ranks, rcond=None)[0]
    df = len(pairs) - np.linalg.matrix_rank(design) - 1
    if df > 0 and np.std(residual[:, 0]) > 1e-10 and np.std(residual[:, 1]) > 1e-10:
        partial = float(np.clip(np.corrcoef(residual.T)[0, 1], -1, 1))
        test = abs(partial) * np.sqrt(df / max(1 - partial * partial, 1e-15))
        result.update({"month_controlled_rank_correlation": partial,
                       "month_controlled_p_value": float(2 * stats.t.sf(test, df)),
                       "month_controlled_method": "Rank residual correlation controlling calendar-month fixed effects; approximate t test"})
    else:
        result["month_controlled_reason"] = "Insufficient within-month residual variation"
    return result


def run_seasonality(silver_path, output_dir, weather_path=None, config=None, allow_fixture=False):
    started = time.monotonic()
    cfg = config or load_config()
    settings = cfg.get("seasonality", {})
    if allow_fixture and Path(output_dir).resolve() == (ROOT / "artifacts").resolve():
        raise ValueError("Fixture analyses require an isolated output directory")
    metrics, figures = Path(output_dir) / "metrics", Path(output_dir) / "figures"
    metrics.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)
    frame, excluded = _read_silver(silver_path, cfg, allow_fixture=allow_fixture)
    if frame.empty:
        result = {"status": "Not Run", "reason": "No eligible non-benchmark silver rows", **excluded, "rows": 0}
        write_json(metrics / "seasonality_summary.json", result)
        return result
    calendar, covered, provenance = _coverage_dates(cfg, frame)
    if not len(calendar):
        result = {"status": "Not Run", "reason": "No full local calendar day before fixed as_of in requested range",
                  "rows": 0, **excluded, **provenance}
        write_json(metrics / "seasonality_summary.json", result)
        return result
    selected = frame[frame["date"].isin(calendar)].copy()
    counts = selected.groupby(["date", "complaint_type", "borough"]).size()
    groups = sorted(set(zip(selected["complaint_type"], selected["borough"])))
    pieces = []
    for complaint, borough in [(ALL, ALL), *groups]:
        if complaint == ALL:
            raw_counts = selected.groupby("date").size().reindex(calendar, fill_value=0)
        else:
            raw_counts = counts.xs((complaint, borough), level=("complaint_type", "borough")).reindex(calendar, fill_value=0)
        piece = pd.DataFrame({"date": calendar, "observed_rows": raw_counts.to_numpy(),
                              "complaint_type": complaint, "borough": borough})
        piece["coverage_complete"] = piece["date"].isin(covered)
        piece["coverage_status"] = np.where(piece["coverage_complete"], "complete_selected_sample", "missing_or_unverified_window")
        piece["count"] = piece["observed_rows"].where(piece["coverage_complete"])
        piece["population_complete"] = False
        pieces.append(piece)
    daily = add_historical_baseline(pd.concat(pieces, ignore_index=True),
        history_weeks=int(settings.get("history_weeks", 8)), min_history=int(settings.get("min_history", 4)),
        z_threshold=float(settings.get("z_threshold", 3)), min_count=int(settings.get("min_count", 20)),
        min_excess=int(settings.get("min_excess", 5)))
    daily.to_csv(metrics / "daily_volume.csv", index=False)
    surges = daily[daily["is_surge"]]
    surges.to_csv(metrics / "surge_events.csv", index=False)
    daily["month"] = daily["date"].dt.month
    seasonal = daily.groupby(["complaint_type", "borough", "month", "weekday"], as_index=False).agg(
        mean_daily_sample_count=("count", "mean"), covered_days=("count", "count"), observed_rows=("observed_rows", "sum"))
    seasonal.to_csv(metrics / "seasonal_volume.csv", index=False)
    total = daily[(daily["complaint_type"] == ALL) & (daily["borough"] == ALL)].copy()
    fig, axes = plt.subplots(3, 1, figsize=(10, 9))
    axes[0].plot(total["date"], total["count"], label="Daily sample")
    axes[0].plot(total["date"], total["rolling_mean_7d"], label="7-day mean")
    axes[0].legend()
    axes[0].set_ylabel(COUNT_LABEL)
    total.groupby("month")["count"].mean().plot.bar(ax=axes[1], ylabel="Mean daily sample count", title="Observed months only; not annual seasonality")
    total.groupby("weekday")["count"].mean().reindex(range(7)).plot.bar(ax=axes[2], ylabel="Mean daily sample count", title="Weekday (Monday=0)")
    fig.tight_layout(); fig.savefig(figures / "seasonal_volume.png", dpi=130); plt.close(fig)
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(total["date"], total["count"], label="Daily sample")
    ax.plot(total["date"], total["baseline_mean"], label="Prior same-weekday mean")
    hits = total[total["is_surge"]]
    ax.scatter(hits["date"], hits["count"], color="red", label="Threshold crossings")
    ax.set_ylabel(COUNT_LABEL); ax.legend(); fig.tight_layout()
    fig.savefig(figures / "surge_examples.png", dpi=130); plt.close(fig)
    joined = total[["date", "count", "coverage_complete", "month"]].rename(columns={"count": "total_sample_count"})
    heat = selected[selected["complaint_type"].str.upper().eq("HEAT/HOT WATER")].groupby("date").size()
    joined["heat_sample_count"] = heat.reindex(calendar, fill_value=0).to_numpy(dtype=float)
    joined.loc[~joined["coverage_complete"], "heat_sample_count"] = np.nan
    weather_status = {"status": "Not Run", "reason": "No weather artifact supplied; no weather values fabricated"}
    associations = []
    weather_file = Path(weather_path) if weather_path else None
    if weather_file is not None and weather_file.is_dir():
        weather_file = weather_file / "weather.csv"
    weather_manifest = None
    if weather_file is not None and (weather_file.parent / "manifest.json").exists():
        weather_manifest = json.loads((weather_file.parent / "manifest.json").read_text())
        if weather_manifest.get("status") != "Success":
            weather_status = {"status": "Not Run", "reason": "Latest weather download manifest is not Success; old CSV is not reused"}
            weather_file = None
    if weather_file is not None and weather_file.exists():
        weather = pd.read_csv(weather_file)
        weather["date"] = pd.to_datetime(weather["date"]).dt.normalize()
        if weather["date"].duplicated().any():
            raise ValueError("Weather artifact has duplicate dates")
        joined = joined.merge(weather, on="date", how="left", validate="one_to_one")
        for variable in ("temperature_2m_min", "temperature_2m_max", "precipitation_sum"):
            if variable not in joined:
                joined[variable] = np.nan
            joined[variable] = pd.to_numeric(joined[variable], errors="coerce")
            for target in ("total_sample_count", "heat_sample_count"):
                associations.append(_association(joined, target, variable))
        weather_status = {"status": "Success", "paired_complete_weather_days": int(joined[["total_sample_count", "temperature_2m_min", "temperature_2m_max", "precipitation_sum"]].notna().all(axis=1).sum()),
                          "semantics": "Historical reanalysis association at one NYC point; no weather predictors enter creation-time model"}
        comparison = joined.dropna(subset=["heat_sample_count", "temperature_2m_min"]).copy()
        comparison["cold_day"] = comparison["temperature_2m_min"] < float(settings.get("cold_threshold_c", 0))
        comparison.groupby(["month", "cold_day"], as_index=False).agg(mean_heat_sample_count=("heat_sample_count", "mean"), n_days=("heat_sample_count", "count")).to_csv(metrics / "weather_month_comparison.csv", index=False)
    joined.to_csv(metrics / "weather_joined.csv", index=False)
    pd.DataFrame(associations, columns=["target", "weather_variable", "n_days", "spearman_rho", "spearman_p_value", "month_controlled_rank_correlation", "month_controlled_p_value", "status", "interpretation", "reason", "month_controlled_method", "month_controlled_reason"]).to_csv(metrics / "weather_associations.csv", index=False)
    fig, ax = plt.subplots(figsize=(7, 4))
    if "temperature_2m_min" in joined and joined["temperature_2m_min"].notna().any():
        ax.scatter(joined["temperature_2m_min"], joined["heat_sample_count"], alpha=.65)
        ax.set_xlabel("NYC representative-point minimum temperature (°C)")
        ax.set_ylabel("HEAT/HOT WATER requests in sample")
        ax.set_title("Observed association; month-controlled estimates in CSV")
    else:
        ax.text(.5, .5, "Not Run: no downloaded weather data", ha="center", va="center", transform=ax.transAxes)
        ax.set_axis_off()
    fig.tight_layout(); fig.savefig(figures / "weather_heat_complaints.png", dpi=130); plt.close(fig)
    summary = {"status": "Success" if total["coverage_complete"].any() else "Not Run",
               "reason": None if total["coverage_complete"].any() else "No date window has verified complete selected-sample coverage",
               "input_rows": len(frame), "analyzed_rows": len(selected), **excluded,
               **provenance, "calendar_days": len(calendar), "covered_days": int(total["coverage_complete"].sum()),
               "unverified_or_missing_days": int((~total["coverage_complete"]).sum()),
               "surge_group_days": len(surges), "total_sample_surge_days": len(hits),
               "weather": weather_status, "weather_associations": associations,
               "limitations": ["Systematic ID-suffix sample counts are not population estimates.",
                  "Missing or truncated extraction days have count=NA; covered zero-count days have count=0.",
                  "Historical weekday rolling reference excludes the current day; early history is unavailable.",
                  "A quarter-year pilot cannot establish annual seasonality; unadjusted multiple associations are exploratory.",
                  "Snapshot category fields may have changed since creation; sampling mechanisms may bias associations."],
               "parameters": {"history_weeks": settings.get("history_weeks", 8), "min_history": settings.get("min_history", 4),
                 "z_threshold": settings.get("z_threshold", 3), "min_count": settings.get("min_count", 20), "min_excess": settings.get("min_excess", 5)},
               "elapsed_seconds": time.monotonic() - started}
    write_json(metrics / "seasonality_summary.json", summary)
    log_event("seasonality", status=summary["status"], output_dir=str(output_dir), allow_fixture=allow_fixture, rows=len(selected), covered_days=summary["covered_days"], elapsed_seconds=summary["elapsed_seconds"])
    return summary
