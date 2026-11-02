"""Calibration-gated retrospective cohort FIFO sensitivity simulation."""
from collections import deque
import json
from pathlib import Path
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter

from nyc311.analysis.seasonality import _read_silver
from nyc311.config import ROOT, load_config, log_event, write_json

ASSUMPTIONS = (
    "Retrospective downloaded-cohort FIFO queue only; initial cohort backlog=0 by cohort definition; "
    "pre-window and unsampled agency backlog unknown, not assumed zero. Observed closures in this cohort "
    "proxy daily service opportunities, not staffing or agency capacity. Homogeneous ticket difficulty; "
    "end-of-day service; fractional uplift accumulates while unused integer daily opportunities expire. "
    "No outside-cohort arrivals or resource competition. Fixed as_of; unfinished tickets right-censored. "
    "Retrospective sensitivity, not causal staffing effects or a forecast."
)


def _km_metrics(durations, observed):
    if not len(durations):
        return {"median": None, "p_unresolved_30d": None, "curve": None}
    fit = KaplanMeierFitter().fit(np.asarray(durations, dtype=float), np.asarray(observed, dtype=bool))
    median = float(fit.median_survival_time_)
    support = float(np.max(durations))
    probability_supported = support >= 30 or float(fit.survival_function_.iloc[-1, 0]) == 0
    return {"median": median if np.isfinite(median) else None,
            "p_unresolved_30d": float(fit.predict(30)) if probability_supported else None,
            "followup_support_days": support, "curve": fit}


def simulate_fifo(group, as_of, uplift=0.0, timezone="America/New_York"):
    """Serve selected-cohort arrivals with closure-count daily opportunities.

    All ticket lifetimes, including those unfinished at the cutoff, are returned.
    Original ticket closure identities are discarded by the FIFO assumption.
    """
    if uplift < 0:
        raise ValueError("Capacity uplift must be nonnegative")
    cutoff = pd.Timestamp(as_of)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    tickets = group.sort_values(["created_date", "unique_key"]).reset_index(drop=True).copy()
    if tickets.empty:
        return pd.DataFrame(columns=["unique_key", "duration_days", "event_observed"]), pd.DataFrame()
    created = tickets["created_date"].dt.tz_convert(timezone)
    closed = tickets["closed_date"].dt.tz_convert(timezone)
    cutoff_local = cutoff.tz_convert(timezone)
    arrival_days = created.dt.date
    observed = closed.notna() & (closed <= cutoff_local) & (closed >= created)
    service = closed[observed].dt.date.value_counts().to_dict()
    arrivals = {}
    for idx, day in enumerate(arrival_days):
        arrivals.setdefault(day, []).append(idx)
    days = pd.date_range(created.min().normalize(), cutoff_local.normalize(), freq="D")
    queue, finish, trace = deque(), {}, []
    remainder = 0.0
    for day_start in days:
        day = day_start.date()
        today = arrivals.get(day, [])
        queue.extend(today)
        opportunities = int(service.get(day, 0))
        scaled = opportunities * (1 + uplift) + remainder
        available = int(np.floor(scaled + 1e-12))
        remainder = max(0.0, scaled - available)
        served = min(available, len(queue))
        end_of_day = min(day_start + pd.DateOffset(days=1) - pd.Timedelta(microseconds=1), cutoff_local)
        for _ in range(served):
            finish[queue.popleft()] = end_of_day
        trace.append({"date": day.isoformat(), "arrivals": len(today), "observed_cohort_closures": opportunities,
                      "modelled_service_opportunities": available, "served": served,
                      "unused_opportunities": available - served, "cohort_backlog": len(queue),
                      "scenario_capacity_uplift": uplift})
    durations = []
    for idx, row in tickets.iterrows():
        endpoint = finish.get(idx, cutoff_local)
        durations.append({"unique_key": row["unique_key"], "duration_days": max(0.0, (endpoint - row["created_date"]).total_seconds() / 86400),
                          "event_observed": idx in finish})
    return pd.DataFrame(durations), pd.DataFrame(trace)


def calibration_check(observed, baseline, observed_close_rate, simulated_close_rate, settings):
    median_obs, median_sim = observed["median"], baseline["median"]
    ratio = median_sim / median_obs if median_obs is not None and median_obs > 0 and median_sim is not None else None
    curve_gap = (abs(observed["p_unresolved_30d"] - baseline["p_unresolved_30d"])
                 if observed["p_unresolved_30d"] is not None and baseline["p_unresolved_30d"] is not None else None)
    reason = []
    if curve_gap is None:
        reason.append("30-day survival unsupported by follow-up while unresolved censored tickets remain")
    if median_obs is None or median_sim is None:
        reason.append("Observed or simulated KM median not reached; precise delta not estimable")
    elif abs(median_obs - median_sim) <= float(settings.get("calibration_absolute_tolerance_days", 1.0)):
        pass
    elif ratio is None or not float(settings.get("calibration_ratio_min", .5)) <= ratio <= float(settings.get("calibration_ratio_max", 2.0)):
        reason.append("Baseline KM median outside configured ratio and absolute tolerances")
    if curve_gap is not None and curve_gap > float(settings.get("calibration_survival_gap_max", .15)):
        reason.append("30-day baseline survival mismatch exceeds tolerance")
    if abs(observed_close_rate - simulated_close_rate) > float(settings.get("calibration_closure_fraction_gap_max", .05)):
        reason.append("Baseline closure fraction mismatch exceeds tolerance")
    return {"calibration_status": "Passed" if not reason else "Failed", "calibration_reason": "; ".join(reason) or "Within configured median, 30-day survival, and closure-fraction tolerances",
            "baseline_to_observed_median_ratio": ratio, "baseline_survival_30d_absolute_gap": curve_gap,
            "calibration_is_in_sample": True}


def run_capacity(silver_path, output_dir, config=None, allow_fixture=False):
    started = time.monotonic()
    cfg = config or load_config()
    settings = cfg.get("capacity", {})
    if allow_fixture and Path(output_dir).resolve() == (ROOT / "artifacts").resolve():
        raise ValueError("Fixture analyses require an isolated output directory")
    metrics, figures = Path(output_dir) / "metrics", Path(output_dir) / "figures"
    metrics.mkdir(parents=True, exist_ok=True); figures.mkdir(parents=True, exist_ok=True)
    frame, excluded = _read_silver(silver_path, cfg, allow_fixture=allow_fixture)
    cutoff = pd.Timestamp(cfg["analysis"]["as_of"])
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    invalid_closed = frame["closed_date"].notna() & (frame["closed_date"] < frame["created_date"])
    frame = frame.loc[~invalid_closed].copy()
    frame["event_observed"] = frame["closed_date"].notna() & (frame["closed_date"] <= cutoff)
    frame["duration_days"] = ((frame["closed_date"].where(frame["event_observed"], cutoff) - frame["created_date"]).dt.total_seconds() / 86400)
    ranking = frame.groupby(["agency", "borough"], as_index=False).agg(sample_n=("unique_key", "size"), observed_closed=("event_observed", "sum"))
    ranking["current_backlog"] = ranking["sample_n"] - ranking["observed_closed"]
    eligible = ranking[ranking["sample_n"] >= int(settings.get("min_group_n", 100))]
    eligible = eligible.sort_values(["current_backlog", "sample_n", "agency", "borough"], ascending=[False, False, True, True]).head(int(settings.get("top_n", 5)))
    records, daily_traces, group_records = [], [], []
    uplifts = sorted(set([0.0, *[float(x) for x in settings.get("uplifts", [.1, .2, .3])]]))
    fig, ax = plt.subplots(figsize=(9, 5))
    for selected in eligible.to_dict("records"):
        agency, borough = selected["agency"], selected["borough"]
        group = frame[(frame["agency"] == agency) & (frame["borough"] == borough)].copy()
        observed = _km_metrics(group["duration_days"], group["event_observed"])
        baseline_tickets, baseline_trace = simulate_fifo(group, cutoff, 0, cfg.get("source_timezone", "America/New_York"))
        baseline = _km_metrics(baseline_tickets["duration_days"], baseline_tickets["event_observed"])
        calibration = calibration_check(observed, baseline, float(group["event_observed"].mean()), float(baseline_tickets["event_observed"].mean()), settings)
        recent = cutoff - pd.Timedelta(days=int(settings.get("recent_days", 90)))
        mix = (group["complaint_type"].value_counts(normalize=True).head(5).round(4)).to_dict()
        common = {**selected, "current_backlog_scope": "Selected downloaded cohort only; agency total unknown",
                  "as_of": cutoff.isoformat(), "censoring_rate": float(1 - group["event_observed"].mean()),
                  "observed_km_median_days": observed["median"], "baseline_median_days": baseline["median"],
                  "observed_p_unresolved_30d": observed["p_unresolved_30d"], "baseline_p_unresolved_30d": baseline["p_unresolved_30d"],
                  "observed_followup_support_days": observed["followup_support_days"], "baseline_followup_support_days": baseline["followup_support_days"],
                  "recent_arrivals": int((group["created_date"] >= recent).sum()),
                  "recent_closures": int((group["event_observed"] & (group["closed_date"] >= recent)).sum()),
                  "top_complaint_mix": json.dumps(mix, sort_keys=True), "assumptions": ASSUMPTIONS,
                  "initial_cohort_backlog": 0, "external_initial_backlog": None, "sampling_scope": "Downloaded sample; not citywide estimates", **calibration}
        group_records.append(common)
        observed["curve"].plot_survival_function(ax=ax, ci_show=False, label=f"{agency}/{borough}: observed cohort")
        baseline["curve"].plot_survival_function(ax=ax, ci_show=False, linestyle="--", label=f"{agency}/{borough}: FIFO baseline")
        for uplift in uplifts:
            simulated, trace = ((baseline_tickets, baseline_trace.copy()) if uplift == 0 else simulate_fifo(group, cutoff, uplift, cfg.get("source_timezone", "America/New_York")))
            scenario = _km_metrics(simulated["duration_days"], simulated["event_observed"])
            calibrated = calibration["calibration_status"] == "Passed"
            estimable = calibrated and scenario["median"] is not None and baseline["median"] is not None
            delta = baseline["median"] - scenario["median"] if estimable else None
            records.append({**common, "scenario_capacity_uplift": uplift,
                            "scenario_median_days": scenario["median"], "modelled_delta_days": delta,
                            "scenario_censoring_rate": float(1 - simulated["event_observed"].mean()),
                            "scenario_end_backlog": int((~simulated["event_observed"]).sum()),
                            "delta_publication_status": "Conditional sensitivity only" if estimable else "Suppressed: failed calibration or KM median not reached",
                            "uncertainty": "No causal or sampling uncertainty interval estimated; uplift and FIFO are sensitivity assumptions"})
            trace["agency"], trace["borough"] = agency, borough
            daily_traces.append(trace)
    columns = ["agency", "borough", "sample_n", "current_backlog", "observed_km_median_days", "baseline_median_days", "scenario_capacity_uplift", "scenario_median_days", "modelled_delta_days", "assumptions", "calibration_status"]
    scenarios = pd.DataFrame(records) if records else pd.DataFrame(columns=columns)
    scenarios.to_csv(metrics / "capacity_scenarios.csv", index=False)
    (pd.DataFrame(group_records) if group_records else pd.DataFrame(columns=["agency", "borough", "sample_n", "calibration_status"])).to_csv(metrics / "capacity_group_summary.csv", index=False)
    (pd.concat(daily_traces, ignore_index=True) if daily_traces else pd.DataFrame(columns=["date", "agency", "borough", "scenario_capacity_uplift", "cohort_backlog"])).to_csv(metrics / "capacity_daily_queues.csv", index=False)
    if records:
        ax.set_xlabel("Days since creation"); ax.set_ylabel("Probability unresolved")
        ax.set_title("In-sample calibration: observed KM and retrospective FIFO cohort")
        ax.legend(fontsize=7)
    else:
        ax.text(.5, .5, "Not Run: no groups meet configured sample threshold", ha="center", va="center", transform=ax.transAxes)
        ax.set_axis_off()
    fig.tight_layout(); fig.savefig(figures / "capacity_calibration.png", dpi=130); plt.close(fig)
    passed = sum(row["calibration_status"] == "Passed" for row in group_records)
    summary = {"status": "Success" if records else "Not Run", "reason": None if records else "No groups meet configured minimum sample size",
               "as_of": cutoff.isoformat(), "input_rows": len(frame), "source_kinds": sorted(frame["source_kind"].unique().tolist()), **excluded, "invalid_closed_removed": int(invalid_closed.sum()),
               "groups_considered": len(ranking), "groups_selected": len(eligible), "groups_calibrated": passed,
               "groups_failed_calibration": len(group_records) - passed, "scenario_rows": len(scenarios),
               "published_nonbaseline_deltas": sum(r["modelled_delta_days"] is not None and r["scenario_capacity_uplift"] > 0 for r in records),
               "censoring_rate": float(1 - frame["event_observed"].mean()) if len(frame) else None,
               "assumptions": ASSUMPTIONS, "calibration_parameters": settings,
               "limitations": ["Calibration reuses the cohort closure process and is not independent validation.",
                  "Incomplete window or systematic-sample coverage can distort cohort composition and service proxy.",
                  "Only downloaded cohort demand is queued; outside-cohort arrivals and historical backlog are unknown.",
                  "Ranking prioritizes investigation and complaint-mix context, not agency performance or hiring decisions."],
               "elapsed_seconds": time.monotonic() - started}
    write_json(metrics / "capacity_summary.json", summary)
    log_event("capacity", status=summary["status"], output_dir=str(output_dir), allow_fixture=allow_fixture, rows=len(frame), groups=len(eligible), calibrated=passed, elapsed_seconds=summary["elapsed_seconds"])
    return summary
