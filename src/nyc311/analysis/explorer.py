"""Interactive cohort analytics, computed from silver rows and verified coverage.

Dates in the public API are inclusive America/New_York calendar dates.  A
``before_date`` is an inclusive upper bound.  Complete extraction describes the
selected sample, never the entire NYC311 population.  These functions do not
change models, persisted scientific results, or their fixed observation cutoff.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from nyc311.analysis.survival import build_labels, km_summary, read_silver, utc_timestamp
from nyc311.config import ROOT

NYC_TIMEZONE = "America/New_York"
DEFAULT_AS_OF = "2025-07-01T00:00:00Z"
CATEGORY_COLUMNS = ("agency", "complaint_type", "borough")
WEATHER_COLUMNS = ("temperature_2m_min", "temperature_2m_max", "precipitation_sum")


def _calendar_date(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("A valid calendar date is required")
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert(NYC_TIMEZONE).tz_localize(None)
    return stamp.normalize()


def _iso(value: Any) -> str:
    return _calendar_date(value).date().isoformat()


def _assert_not_benchmark(frame: pd.DataFrame) -> None:
    for column in ("synthetic", "is_synthetic"):
        if column in frame and frame[column].fillna(False).astype(str).str.lower().isin(["true", "1"]).any():
            raise ValueError("Synthetic benchmark replicas cannot enter interactive analyses")
    if "replica_id" in frame and frame["replica_id"].notna().any():
        raise ValueError("Benchmark replicas cannot enter interactive analyses")
    if "source_kind" not in frame:
        raise ValueError("Interactive analyses require source_kind provenance")
    kinds = set(frame["source_kind"].fillna("UNKNOWN").astype(str))
    if not kinds <= {"official_snapshot", "synthetic_fixture"} or len(kinds) > 1:
        raise ValueError("Interactive analyses require isolated official snapshots or explicit test fixtures")


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    _assert_not_benchmark(frame)
    result = frame.copy()
    result["created_date"] = pd.to_datetime(result["created_date"], utc=True, errors="coerce", format="mixed")
    result["created_local_date"] = result["created_date"].dt.tz_convert(NYC_TIMEZONE).dt.tz_localize(None).dt.normalize()
    for column in CATEGORY_COLUMNS:
        result[column] = result[column].fillna("UNKNOWN").astype(str)
    return result


def load_explorer_inputs(silver_path=None, manifest_path=None):
    """Read provenance-guarded official silver and selected-sample coverage.

    Only Success extraction windows are covered.  Missing, Failed, Truncated,
    and unverified windows are deliberately not inferred from rows being present.
    """
    frame = _normalize_frame(read_silver(silver_path))
    path = Path(manifest_path or ROOT / "artifacts/metrics/source_manifest.json")
    if not path.exists():
        raise FileNotFoundError(f"Coverage manifest does not exist: {path}")
    manifest = json.loads(path.read_text())
    if manifest.get("dataset_id") != "erm2-nwe9" or manifest.get("source_kind") != "official_nyc311_snapshot":
        raise ValueError("Explorer requires the official erm2-nwe9 snapshot manifest")
    if set(frame["source_kind"].unique()) != {"official_snapshot"}:
        raise ValueError("Official coverage cannot be attached to fixture or unverified silver")
    windows = manifest.get("windows", [])
    query = manifest.get("query_config", {})
    starts = [window["start_date"] for window in windows if window.get("start_date")]
    ends = [window["end_date"] for window in windows if window.get("end_date")]
    start = query.get("start_date") or (min(starts) if starts else None)
    end = query.get("end_date") or (max(ends) if ends else None)
    if not start or not end or _calendar_date(end) <= _calendar_date(start):
        raise ValueError("Coverage manifest must specify a nonempty calendar range")
    calendar = pd.date_range(_calendar_date(start), _calendar_date(end), freq="D", inclusive="left")
    if frame["created_local_date"].isna().any() or not frame["created_local_date"].isin(calendar).all():
        raise ValueError("Silver creation dates lie outside the paired manifest's extraction calendar")
    sampling = manifest.get("sampling", {})
    suffix = sampling.get("suffix", query.get("sample_suffix"))
    if sampling.get("method") == "systematic_numeric_id_suffix" or query.get("sample_suffix"):
        if not suffix or not frame["unique_key"].astype(str).str.endswith(str(suffix)).all():
            raise ValueError("Silver request IDs do not match the manifest's systematic sample suffix")
        if query.get("sample_suffix") is not None and str(query["sample_suffix"]) != str(suffix):
            raise ValueError("Manifest query and sampling suffix disagree")
    for window in windows:
        if window.get("start_date") and window.get("end_date") and window.get("rows") is not None:
            in_window = frame["created_local_date"].ge(_calendar_date(window["start_date"])) & frame["created_local_date"].lt(_calendar_date(window["end_date"]))
            if int(in_window.sum()) > int(window["rows"]):
                raise ValueError("Silver rows exceed the paired manifest's extraction-window row count")
    covered: set[pd.Timestamp] = set()
    unverified: set[pd.Timestamp] = set()
    for window in windows:
        if not window.get("start_date") or not window.get("end_date"):
            continue
        days = set(pd.date_range(_calendar_date(window["start_date"]), _calendar_date(window["end_date"]), freq="D", inclusive="left"))
        if window.get("status") == "Success":
            covered.update(days)
        else:
            unverified.update(days)
    # A conflicting failed/truncated overlapping window is conservatively unknown.
    covered = covered.intersection(set(calendar)).difference(unverified)
    coverage = {
        "start_date": _iso(start), "end_date_exclusive": _iso(end),
        "available_dates": [_iso(day) for day in calendar],
        "covered_dates": [_iso(day) for day in sorted(covered)],
        "years": sorted(set(calendar.year.tolist())),
        "dataset_id": manifest["dataset_id"], "source_kind": manifest["source_kind"],
        "sampling": manifest.get("sampling", {}), "population_complete": False,
        "manifest_path": str(path),
        "cohort_manifest_checks": ["official provenance", "creation-date bounds", "sample suffix when specified", "window row upper bounds when available"],
        "artifact_binding_note": "Consistency checks are not a cryptographic binding between silver and the manifest",
        "coverage_semantics": "Complete extraction of the selected sample; not a NYC population census",
    }
    return frame, coverage


def available_dates(coverage: dict) -> pd.DatetimeIndex:
    """Return requested local calendar dates, including explicitly unknown days."""
    return pd.DatetimeIndex(pd.to_datetime(coverage.get("available_dates", []))).normalize().sort_values().unique()


def _bounds(start_date, end_date, before_date=None):
    start, end = _calendar_date(start_date), _calendar_date(end_date)
    if before_date is not None:
        end = min(end, _calendar_date(before_date))
    if end < start:
        raise ValueError("End date / through date must be on or after start date")
    return start, end


def filter_requests(frame, start_date, end_date, *, agencies=None, complaint_types=None, boroughs=None, before_date=None):
    """Select creation-time cohort; None means all, [] means no selected groups."""
    result = _normalize_frame(frame)
    start, end = _bounds(start_date, end_date, before_date)
    mask = result["created_local_date"].between(start, end, inclusive="both")
    for column, values in (("agency", agencies), ("complaint_type", complaint_types), ("borough", boroughs)):
        if values is not None:
            mask &= result[column].isin(list(values))
    return result.loc[mask].copy()


def _daily_counts(selected, start, end, coverage, as_of):
    calendar = pd.date_range(start, end, freq="D")
    covered = set(pd.to_datetime(coverage.get("covered_dates", [])).normalize())
    cutoff_day = utc_timestamp(as_of).tz_convert(NYC_TIMEZONE).tz_localize(None).normalize()
    counts = selected.groupby("created_local_date").size().reindex(calendar, fill_value=0)
    daily = pd.DataFrame({"date": calendar, "observed_rows": counts.to_numpy(dtype=int)})
    daily["coverage_complete"] = daily["date"].isin(covered) & (daily["date"] < cutoff_day)
    daily["count"] = daily["observed_rows"].astype(float).where(daily["coverage_complete"])
    daily["rolling_mean_7d"] = daily["count"].rolling(7, min_periods=7).mean()
    return daily


def _frequency(frequency):
    value = str(frequency).lower()
    aliases = {"d": "daily", "day": "daily", "daily": "daily", "w": "weekly", "week": "weekly", "weekly": "weekly", "m": "monthly", "month": "monthly", "monthly": "monthly"}
    if value not in aliases:
        raise ValueError("frequency must be daily, weekly, or monthly")
    return aliases[value]


def aggregate_daily(daily, frequency="daily"):
    """Aggregate calendar buckets; incomplete selected coverage stays NA.

    Edge weeks/months may have a count, but ``full_bucket=False`` means a partial
    calendar period.  Such buckets never enter the first/last trend comparison.
    """
    frequency = _frequency(frequency)
    source = daily.copy()
    if frequency == "daily":
        source["period"] = source["date"].dt.to_period("D")
    elif frequency == "weekly":
        source["period"] = source["date"].dt.to_period("W-SUN")
    else:
        source["period"] = source["date"].dt.to_period("M")
    rows = []
    for period, group in source.groupby("period", sort=True):
        period_start, period_end = period.start_time.normalize(), period.end_time.normalize()
        selected_days = len(group)
        full_calendar = selected_days == (period_end - period_start).days + 1
        complete = bool(group["coverage_complete"].all())
        count = float(group["count"].sum()) if complete else np.nan
        rows.append({"date": period_start, "period_start": period_start, "period_end": period_end,
                     "count": count, "mean_daily_count": count / selected_days if complete else np.nan,
                     "observed_rows": int(group["observed_rows"].sum()),
                     "covered_days": int(group["coverage_complete"].sum()), "selected_days": selected_days,
                     "coverage_complete": complete, "full_bucket": full_calendar and complete})
    return pd.DataFrame(rows, columns=["date", "period_start", "period_end", "count", "mean_daily_count", "observed_rows", "covered_days", "selected_days", "coverage_complete", "full_bucket"])


def _breakdown(frame, column):
    counts = frame[column].value_counts().rename_axis("category").reset_index(name="count")
    counts["share"] = counts["count"] / len(frame) if len(frame) else pd.Series(dtype=float)
    return counts


def _previous_comparison(frame, coverage, start, end, categories, as_of, current_daily):
    n_days = (end - start).days + 1
    previous_end = start - pd.Timedelta(days=1)
    previous_start = start - pd.Timedelta(days=n_days)
    previous = filter_requests(frame, previous_start, previous_end, **categories)
    previous = previous.loc[previous["created_date"] <= utc_timestamp(as_of)]
    previous_daily = _daily_counts(previous, previous_start, previous_end, coverage, as_of)
    complete = bool(current_daily["coverage_complete"].all() and previous_daily["coverage_complete"].all())
    current_count, previous_count = int(current_daily["observed_rows"].sum()), int(previous_daily["observed_rows"].sum())
    result = {
        "status": "Estimable" if complete else "Not Estimable", "reason": None,
        "method": "Immediately preceding equal-length local calendar window, with identical category filters",
        "calendar_days_each": n_days, "current_start": _iso(start), "current_end": _iso(end),
        "previous_start": _iso(previous_start), "previous_end": _iso(previous_end),
        "current_count": current_count if complete else None,
        "previous_count": previous_count if complete else None,
        "absolute_change": current_count - previous_count if complete else None,
        "percent_change": None,
        "minimum_requests_each": 20,
        "current_covered_days": int(current_daily["coverage_complete"].sum()),
        "previous_covered_days": int(previous_daily["coverage_complete"].sum()),
    }
    if not complete:
        result["reason"] = "Both equal-length windows must have complete selected-sample extraction on every day"
    elif previous_count == 0:
        result["reason"] = "Previous window has zero sample requests; a percentage change is undefined"
    elif min(current_count, previous_count) < 20:
        result["reason"] = "A directional percentage narrative requires at least 20 sample requests in each window"
    else:
        result["percent_change"] = float(100 * (current_count - previous_count) / previous_count)
    return result


def _within_window_trend(daily, buckets, frequency):
    """A cautious first/last equal-length comparison, with explicit support rules."""
    result = {"status": "Not Estimable", "reason": None, "method": None, "percent_change": None,
              "first_count": None, "last_count": None, "days_each": None,
              "minimum_requests_each": 20, "first_start": None, "first_end": None,
              "last_start": None, "last_end": None}
    if not daily["coverage_complete"].all():
        result["reason"] = "A selected extraction day is missing, truncated, unverified, or incomplete at as_of"
        return result
    if frequency == "daily":
        if len(daily) < 14:
            result["reason"] = "Requires two non-overlapping 7-day blocks"
            return result
        first, last = daily.head(7), daily.tail(7)
        result["method"] = "First 7 versus last 7 covered local calendar days"
    elif frequency == "weekly":
        complete = buckets[buckets["full_bucket"]]
        if len(complete) < 4:
            result["reason"] = "Requires four full covered calendar weeks; compares first two versus last two"
            return result
        first_bounds, last_bounds = complete.head(2), complete.tail(2)
        first = daily[daily["date"].between(first_bounds["period_start"].min(), first_bounds["period_end"].max())]
        last = daily[daily["date"].between(last_bounds["period_start"].min(), last_bounds["period_end"].max())]
        result["method"] = "First two versus last two full covered Monday–Sunday weeks"
    else:
        complete = buckets[buckets["full_bucket"]]
        if len(complete) < 2:
            result["reason"] = "Requires at least two full covered calendar months"
            return result
        # Calendar months differ in length.  Compare equal daily exposure, rather
        # than interpreting February versus January totals as demand movement.
        first_bucket, last_bucket = complete.iloc[0], complete.iloc[-1]
        days_each = min((first_bucket.period_end - first_bucket.period_start).days + 1,
                        (last_bucket.period_end - last_bucket.period_start).days + 1)
        first = daily[daily["date"].between(first_bucket.period_start, first_bucket.period_start + pd.Timedelta(days=days_each - 1))]
        last = daily[daily["date"].between(last_bucket.period_start, last_bucket.period_start + pd.Timedelta(days=days_each - 1))]
        result["method"] = "Equal first N days of the first and last full covered calendar months; N is the shorter month's length"
    first_count, last_count = int(first["count"].sum()), int(last["count"].sum())
    result.update({"first_count": first_count, "last_count": last_count, "days_each": len(first),
                   "first_start": _iso(first["date"].min()), "first_end": _iso(first["date"].max()),
                   "last_start": _iso(last["date"].min()), "last_end": _iso(last["date"].max())})
    if first_count < 20 or last_count < 20:
        result["reason"] = "Requires at least 20 sample requests in each comparison block"
    else:
        result.update({"status": "Estimable", "percent_change": float(100 * (last_count - first_count) / first_count)})
    return result


def _insights(metrics, comparison, trend, agencies, selection):
    rows, covered, days = metrics["selected_rows"], metrics["covered_days"], metrics["calendar_days"]
    source = "test-fixture" if metrics["source_kinds"] == ["synthetic_fixture"] else "downloaded systematic sample"
    mean = f"{metrics['mean_daily_count']:.1f}" if metrics["mean_daily_count"] is not None else "not estimable"
    cards = [{"title": "Selected cohort", "tone": "neutral", "body":
              f"{rows:,} {source} requests were created from {selection['start_date']} through {selection['end_date']} (NYC local dates). "
              f"Mean = {mean} requests per covered day ({covered}/{days} days covered). "
              + ("The selected calendar is fully covered for this sample." if covered == days else "Observed rows are partial; missing extraction days are unknown, not zero.")}]
    if comparison["status"] == "Estimable" and comparison["percent_change"] is not None:
        change = comparison["percent_change"]
        cards.append({"title": "Change versus previous window", "tone": "neutral", "body":
                      f"Sample arrivals changed {change:+.1f}% ({comparison['previous_count']:,} → {comparison['current_count']:,}) versus "
                      f"{comparison['previous_start']}–{comparison['previous_end']}. Both windows contain {comparison['calendar_days_each']} local calendar days and use the same category filters. "
                      "Each window contains at least 20 sampled requests. "
                      "This describes request arrivals in the sample; it does not establish a cause or citywide change."})
    elif trend["status"] == "Estimable":
        cards.append({"title": "Movement inside this window", "tone": "neutral", "body":
                      f"{trend['method']}: {trend['first_count']:,} requests ({trend['first_start']}–{trend['first_end']}) versus "
                      f"{trend['last_count']:,} ({trend['last_start']}–{trend['last_end']}), {trend['percent_change']:+.1f}%. "
                      f"Each block has {trend['days_each']} complete calendar days and at least 20 requests. "
                      "This is a descriptive sample contrast, not an annual seasonal pattern or causal effect."})
    else:
        cards.append({"title": "Trend comparison unavailable", "tone": "muted", "body":
                      f"Previous-window comparison: {comparison['reason']}. Within-window contrast: {trend['reason']}. "
                      "No direction of change is inferred from insufficient support."})
    if metrics["peak_day"] is not None:
        cards.append({"title": "Busiest covered day", "tone": "neutral", "body":
                      f"{metrics['peak_day']} recorded {metrics['peak_count']:,} selected sample arrivals "
                      f"({metrics['peak_tied_days']} day(s) tied for this maximum). This can identify periods to investigate; it is not an anomaly test or a measure of service capacity."})
    if len(agencies):
        leader = agencies.iloc[0]
        cards.append({"title": "Agency mix", "tone": "neutral", "body":
                      f"{leader['category']} accounts for {int(leader['count']):,} requests ({leader['share']:.1%}) in the selected cohort. "
                      "This identifies where sampled arrivals concentrate; agencies handle different complaint mixes and request difficulty, so shares are not staff-performance rankings."})
    else:
        cards.append({"title": "Agency mix", "tone": "muted", "body": "No requests match the selected filters. Covered zero-count days remain valid observations."})
    if metrics["label_rows"]:
        cards.append({"title": "Resolution follow-up", "tone": "neutral", "body":
                      f"{metrics['censored_rows']:,}/{metrics['label_rows']:,} requests ({metrics['censoring_fraction']:.1%}) have no observed closure by the fixed cutoff {metrics['as_of']}. "
                      "They are right-censored at that cutoff. This is follow-up for the selected creation cohort, not the city's current backlog."})
    else:
        cards.append({"title": "Resolution follow-up", "tone": "muted", "body": "No valid requests are available for right-censored follow-up under these filters."})
    cards.append({"title": "How to use this view", "tone": "muted", "body":
                  "Use date and category filters to inspect the same sample consistently. Counts are not city totals; systematic ID-suffix sampling may be biased. "
                  "Missing days are gaps, edge weeks/months are labelled partial, and snapshot category fields may have been updated after creation. "
                  "A short available history cannot establish year-round seasonality or staffing effects."})
    return cards


def summarize_selection(frame, coverage, start_date, end_date, *, agencies=None, complaint_types=None,
                        boroughs=None, before_date=None, frequency="daily", as_of=DEFAULT_AS_OF):
    """Return recomputed cohort tables, coverage-aware comparisons and narratives.

    All numeric metrics and selection/comparison dictionaries are JSON-compatible.
    DataFrame values are intentionally returned separately for interactive charts.
    """
    start, end = _bounds(start_date, end_date, before_date)
    frequency = _frequency(frequency)
    categories = {"agencies": agencies, "complaint_types": complaint_types, "boroughs": boroughs}
    selected = filter_requests(frame, start, end, **categories)
    future_created = int((selected["created_date"] > utc_timestamp(as_of)).sum())
    selected = selected.loc[selected["created_date"] <= utc_timestamp(as_of)].copy()
    daily = _daily_counts(selected, start, end, coverage, as_of)
    buckets = aggregate_daily(daily, frequency)
    labels, label_metrics = build_labels(selected, as_of)
    covered = int(daily["coverage_complete"].sum())
    valid_daily = daily.dropna(subset=["count"])
    peak_count = int(valid_daily["count"].max()) if len(valid_daily) else None
    peak_days = valid_daily.loc[valid_daily["count"].eq(peak_count), "date"] if peak_count is not None else pd.Series(dtype="datetime64[ns]")
    kinds = sorted(frame["source_kind"].dropna().astype(str).unique().tolist())
    metrics = {
        "input_rows": len(frame), "selected_rows": len(selected),
        "filtered_out_rows": len(frame) - len(selected), "future_created_excluded_rows": future_created,
        "calendar_days": len(daily), "covered_days": covered,
        "unknown_days": len(daily) - covered, "coverage_complete": covered == len(daily),
        "covered_sample_rows": int(valid_daily["observed_rows"].sum()),
        "mean_daily_count": float(valid_daily["count"].mean()) if len(valid_daily) else None,
        "peak_day": _iso(peak_days.iloc[0]) if len(peak_days) else None,
        "peak_count": peak_count, "peak_tied_days": len(peak_days),
        "as_of": label_metrics["as_of"], "label_rows": len(labels),
        "label_rejected_rows": label_metrics["rejected_rows"],
        "closed_observed_rows": label_metrics["closed_observed_rows"],
        "censored_rows": label_metrics["censored_rows"], "censoring_fraction": label_metrics["censoring_fraction"],
        "source_kinds": kinds, "population_complete": False,
        "scope": "explicit synthetic fixture" if kinds == ["synthetic_fixture"] else "official systematic sample, not city totals",
    }
    selection = {"start_date": _iso(start), "end_date": _iso(end), "end_date_inclusive": True,
                 "timezone": NYC_TIMEZONE, "frequency": frequency,
                 **{name: list(values) if values is not None else None for name, values in categories.items()}}
    agency_table = _breakdown(selected, "agency")
    comparison = _previous_comparison(frame, coverage, start, end, categories, as_of, daily)
    trend = _within_window_trend(daily, buckets, frequency)
    return {"metrics": metrics, "selection": selection, "daily": daily, "buckets": buckets,
            "agencies": agency_table, "boroughs": _breakdown(selected, "borough"),
            "complaints": _breakdown(selected, "complaint_type"), "comparison": comparison,
            "trend": trend, "insights": _insights(metrics, comparison, trend, agency_table, selection)}


def grouped_km(frame, *, group_by="agency", as_of=DEFAULT_AS_OF, top_n=8, min_group_n=1):
    """Refit descriptive KM only for the filtered cohort; never refit Cox here."""
    _assert_not_benchmark(frame)
    if group_by not in CATEGORY_COLUMNS and group_by != "all":
        raise ValueError("group_by must be agency, complaint_type, borough, or all")
    if top_n < 1 or min_group_n < 1:
        raise ValueError("top_n and min_group_n must be positive")
    labels, metrics = build_labels(frame, as_of)
    if group_by == "all":
        labels = labels.assign(_km_group="Selected cohort")
        column = "_km_group"
    else:
        column = group_by
        labels[column] = labels[column].fillna("UNKNOWN").astype(str)
    counts = labels[column].value_counts()
    groups = counts[counts >= min_group_n].head(top_n).index
    curves, summaries = [], []
    for group in groups:
        km, summary = km_summary(labels.loc[labels[column].eq(group)])
        curve = km.survival_function_.join(km.confidence_interval_).reset_index()
        curve.columns = ["duration_days", "survival_probability", "lower_95", "upper_95"]
        curve["group"] = str(group)
        curve["group_by"] = group_by
        curves.append(curve)
        summaries.append({"group": str(group), "group_by": group_by, "as_of": metrics["as_of"], **summary})
    return {"curves": pd.concat(curves, ignore_index=True) if curves else pd.DataFrame(columns=["duration_days", "survival_probability", "lower_95", "upper_95", "group", "group_by"]),
            "summary": pd.DataFrame(summaries, columns=["group", "group_by", "as_of", "sample_n", "closed_n", "censored_n", "censoring_fraction", "km_median_days", "median_status", "followup_support_days"]),
            "metrics": {**metrics, "group_by": group_by, "groups_plotted": len(groups),
                        "top_n": top_n, "min_group_n": min_group_n,
                        "plotted_rows": int(counts.reindex(groups).sum()),
                        "status": "Estimable" if len(groups) else "Not Estimable"}}


def weather_selection(daily, weather_path=None, *, min_days=10):
    """Join filtered daily arrivals to observed historical weather and recompute rho.

    Historical weather is used for retrospective association only, and never
    passed to the creation-time prediction model.  Incomplete weather days stay
    missing, even if an old numeric value exists in the artifact.
    """
    if min_days < 10:
        raise ValueError("min_days must be at least 10")
    path = Path(weather_path or ROOT / "data/weather/weather.csv")
    metrics = {"status": "Not Estimable", "weather_path": str(path), "min_paired_days": min_days,
               "selected_days": len(daily), "paired_days": 0,
               "interpretation": "Exploratory retrospective association; not causal and not a creation-time predictor"}
    associations, joined = [], daily.copy()
    if not path.exists():
        metrics["reason"] = "No downloaded weather artifact is available"
        return {"joined": joined, "associations": pd.DataFrame(), "metrics": metrics,
                "insights": [{"title": "Weather association unavailable", "tone": "muted", "body": metrics["reason"]}]}
    manifest_path = path.parent / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("status") != "Success":
            metrics["reason"] = "Latest weather manifest is not Success; stale CSV is not reused"
            return {"joined": joined, "associations": pd.DataFrame(), "metrics": metrics,
                    "insights": [{"title": "Weather association unavailable", "tone": "muted", "body": metrics["reason"]}]}
    weather = pd.read_csv(path)
    if "date" not in weather:
        raise ValueError("Weather artifact requires a date column")
    weather["date"] = pd.to_datetime(weather["date"], errors="raise").dt.normalize()
    if weather["date"].duplicated().any():
        raise ValueError("Weather artifact contains duplicate local dates")
    keep = ["date", *[name for name in WEATHER_COLUMNS if name in weather]]
    if "weather_complete" in weather:
        complete = weather["weather_complete"].fillna(False).astype(str).str.lower().isin(["true", "1"])
        weather.loc[~complete, [name for name in WEATHER_COLUMNS if name in weather]] = np.nan
    joined = joined.merge(weather[keep], on="date", how="left", validate="one_to_one")
    for variable in WEATHER_COLUMNS:
        joined[variable] = pd.to_numeric(joined[variable], errors="coerce") if variable in joined else np.nan
        paired = joined[["count", variable]].replace([np.inf, -np.inf], np.nan).dropna()
        record = {"target": "selected_sample_count", "weather_variable": variable, "n_days": len(paired),
                  "spearman_rho": None, "spearman_p_value": None, "status": "Not Estimable", "reason": None}
        if len(paired) < min_days:
            record["reason"] = f"Requires at least {min_days} covered paired days"
        elif paired["count"].nunique() < 2 or paired[variable].nunique() < 2:
            record["reason"] = "Both filtered arrivals and weather must vary"
        else:
            value = spearmanr(paired["count"], paired[variable])
            record.update({"status": "Estimable", "spearman_rho": float(value.statistic), "spearman_p_value": float(value.pvalue)})
        associations.append(record)
    table = pd.DataFrame(associations)
    estimated = table.loc[table["status"].eq("Estimable")]
    metrics.update({"status": "Estimable" if len(estimated) else "Not Estimable",
                    "paired_days": int(table["n_days"].max()) if len(table) else 0,
                    "variables_estimated": len(estimated),
                    "reason": None if len(estimated) else "Filtered cohort has insufficient paired days or variation"})
    insights = []
    for record in associations:
        if record["status"] == "Estimable":
            body = (f"For the selected cohort, daily arrivals versus {record['weather_variable']} have Spearman ρ={record['spearman_rho']:+.3f} "
                    f"across {record['n_days']} covered paired days (unadjusted p={record['spearman_p_value']:.3g}). "
                    "This exploratory association may reflect season, complaint mix or sampling; one NYC weather point is not borough-level exposure and does not establish causality.")
        else:
            body = f"{record['weather_variable']}: {record['reason']} ({record['n_days']} paired days). No direction of association is inferred."
        insights.append({"title": "Filtered weather association", "tone": "neutral" if record["status"] == "Estimable" else "muted", "body": body})
    return {"joined": joined, "associations": table, "metrics": metrics, "insights": insights}
