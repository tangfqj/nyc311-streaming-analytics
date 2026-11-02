"""A read-only analytical workspace built from persisted project evidence."""
from html import escape
import json
import os
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from nyc311.config import ROOT
sys.path.insert(0, str(Path(__file__).resolve().parent))
import explorer_ui as explore


st.set_page_config(page_title="NYC 311 · City Service Analytics", page_icon="◉", layout="wide")
st.markdown(f"<style>{Path(__file__).with_name('style.css').read_text()}</style>", unsafe_allow_html=True)
alt.data_transformers.disable_max_rows()
COLORS = ["#117C83", "#4D79A6", "#C69752", "#A07E9F", "#74988B", "#C77B68"]
METRIC_ROWS = 0


def read_json(name):
    path = ROOT / name
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        st.warning(f"Could not read {name}. Regenerate the associated artifact.")
        return {}


def read_csv(name):
    path = ROOT / name
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except (OSError, ValueError, pd.errors.EmptyDataError):
        st.warning(f"Could not read {name}. Regenerate the associated artifact.")
        return pd.DataFrame()


def number(value, digits=0, suffix=""):
    if value is None or pd.isna(value):
        return "Not run"
    return f"{value:,.{digits}f}{suffix}"


def percent(value):
    return "Unavailable" if value is None or pd.isna(value) else f"{100 * value:.1f}%"


def panel(title, subtitle="", badge=None, tone=""):
    tag = f'<span class="status-badge {tone}">{escape(str(badge))}</span>' if badge else ""
    st.markdown(f'<div class="panel-title"><h3>{escape(title)}</h3>{tag}</div>'
                f'<div class="panel-subtitle">{escape(subtitle)}</div>', unsafe_allow_html=True)


def note(text, amber=False):
    st.markdown(f'<div class="note {"amber" if amber else ""}">{escape(text)}</div>', unsafe_allow_html=True)


def chart(plot, height=290):
    plot = (plot.properties(height=height)
            .configure_view(strokeWidth=0)
            .configure_axis(labelColor="#7A8B92", titleColor="#61767E", labelFontSize=11,
                            titleFontSize=11, titleFontWeight=400, gridColor="#EDF1EE",
                            domain=False, tickSize=0, labelPadding=9, titlePadding=12)
            .configure_legend(labelColor="#61767E", title=None, orient="bottom", labelFontSize=11,
                              symbolType="circle", padding=12)
            .configure(background="#FFFFFF", font="Arial"))
    st.altair_chart(plot, use_container_width=True, theme=None)


def evidence(title, *paths):
    with st.expander(title):
        for path in paths:
            st.caption(f"Source · {path}")
            if path.endswith(".json"):
                value = read_json(path)
                if value:
                    st.json(value, expanded=False)
                else:
                    st.info("Not run / no artifact available.")
            else:
                frame = read_csv(path)
                if frame.empty:
                    st.info("No rows available.")
                else:
                    st.dataframe(frame, use_container_width=True, hide_index=True)


def empty(message, command):
    st.info(message)
    st.code(command, language="bash")


def daily_chart(frame, value="total_sample_count", label="Requests", height=285):
    work = frame.copy()
    work["date"] = pd.to_datetime(work["date"])
    work["rolling_average"] = work[value].rolling(7, min_periods=7).mean()
    base = alt.Chart(work).encode(x=alt.X("date:T", title=None, axis=alt.Axis(format="%b %d", tickCount=6)))
    area = base.mark_area(color="#117C83", opacity=.1).encode(y=alt.Y(f"{value}:Q", title=label))
    line = base.mark_line(color="#117C83", strokeWidth=2).encode(
        y=alt.Y(f"{value}:Q", title=label),
        tooltip=[alt.Tooltip("date:T", title="Date", format="%b %d, %Y"),
                 alt.Tooltip(f"{value}:Q", title=label, format=","),
                 alt.Tooltip("rolling_average:Q", title="7-day average", format=".1f")])
    trend = base.mark_line(color="#C69752", strokeDash=[5, 4], strokeWidth=2).encode(y="rolling_average:Q")
    chart(area + line + trend, height)


def metric_row(items):
    global METRIC_ROWS
    METRIC_ROWS += 1
    with st.container(key=f"metric_row_{METRIC_ROWS}"):
        cols = st.columns(len(items), gap="medium")
        for col, (label, value, help_text) in zip(cols, items):
            col.metric(label, value, help=help_text)


def check_row(label, value):
    tone = "passed" if value is True else "failed" if value is False else ""
    text = "Verified" if value is True else "Failed" if value is False else "Not run"
    st.markdown(f'<div class="check-row"><span class="check-label">{escape(label)}</span>'
                f'<span class="status-badge {tone}">{text}</span></div>', unsafe_allow_html=True)


dq = read_json("artifacts/metrics/data_quality.json")
survival = read_json("artifacts/metrics/survival.json")
model = read_json("artifacts/metrics/model_evaluation.json")
season = read_json("artifacts/metrics/seasonality_summary.json")
capacity = read_json("artifacts/metrics/capacity_summary.json")
stream = read_json("artifacts/metrics/streaming_metrics.json")
source = read_json("artifacts/metrics/source_manifest.json")
source_kinds = list(dq.get("source_kinds", {}))
official_sample = bool(source_kinds) and all(kind.startswith("official") for kind in source_kinds)
cohort_label = "Official sample" if official_sample else "Synthetic fixture" if source_kinds and all("synthetic" in kind or "fixture" in kind for kind in source_kinds) else "Sample cohort" if source_kinds else "No cohort artifact"
query = source.get("query_config", {})
start = query.get("start_date", "Not run")
end = query.get("end_date", "Not run")
end_inclusive = (pd.Timestamp(end) - pd.Timedelta(days=1)).strftime("%Y-%m-%d") if end != "Not run" else end
as_of = str(survival.get("as_of", "Not run"))[:10]

with st.sidebar:
    st.markdown('<div class="sidebar-brand"><div class="brand-mark">311</div><div>'
                '<div class="brand-name">City Service<br>Analytics</div>'
                '<div class="brand-sub">NEW YORK CITY</div></div></div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-label">WORKSPACE</div>', unsafe_allow_html=True)
    page = st.radio("View", ["Overview", "Survival & Prediction", "Seasonality & Capacity", "Scalability"])
    st.markdown("---")
    st.markdown(f'<div class="sidebar-note"><strong>DOWNLOADED WINDOW</strong><br>{escape(start)} → {escape(end_inclusive)}'
                f'<br><br><strong>FIXED AS OF</strong><br>{escape(as_of)}'
                '<br><br>Historical snapshot replay.<br>Sample counts, not citywide totals.</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-footer">NYC OPEN DATA · erm2-nwe9</div>', unsafe_allow_html=True)

st.markdown('<div class="topline"><div class="eyebrow">City intelligence / NYC 311</div>'
            '<div class="source-tag">Historical snapshots · reproducible evidence</div></div>', unsafe_allow_html=True)
HEADERS = {
    "Overview": ("The story behind every request.",
                 "A closer look at service demand, resolution times, and the pipeline that connects them."),
    "Survival & Prediction": ("Understand the time to resolution.",
                              "Follow unresolved requests over time, evaluate the model, and predict a new request at creation."),
    "Seasonality & Capacity": ("See demand. Examine capacity.",
                               "Explore weather associations and test the assumptions behind capacity scenarios."),
    "Scalability": ("Measure what actually scales.",
                    "Measured Spark runtimes, individual repetitions, and the limits of a single-host experiment."),
}
headline, description = HEADERS[page]
st.markdown(f'<div class="hero"><div class="eyebrow">{escape(page)}</div><h1>{escape(headline)}</h1>'
            f'<p>{escape(description)}</p><div class="hero-pills">'
            f'<span class="hero-pill">{number(dq.get("accepted_rows"))} saved silver records · {escape(cohort_label.lower())}</span>'
            f'<span class="hero-pill">Observation cutoff · {escape(as_of)}</span>'
            '</div></div>', unsafe_allow_html=True)

explorer = explore.controls(survival.get("as_of", "2025-07-01T00:00:00Z")) if page != "Scalability" else None

if page == "Overview":
    if explorer:
        selected = explorer["summary"]
        selected_metrics = selected["metrics"]
        metric_row([
            ("Selected sample requests", number(selected_metrics["selected_rows"]), "Creation cohort selected by dates, categories and the timeline brush."),
            ("Mean daily arrivals", number(selected_metrics["mean_daily_count"], 1), "Average over verified covered sample days; unknown extraction days are excluded."),
            ("Observed resolved", number(selected_metrics["closed_observed_rows"]), f"Selected cohort closures observed by fixed as-of {as_of}."),
            ("Right-censored", number(selected_metrics["censored_rows"]), f"Selected requests without an observed closure by {as_of}; retained as right-censored."),
        ])
        st.markdown('<div class="section-label">01 / Your selected cohort</div>', unsafe_allow_html=True)
        left, right = st.columns([1.8, 1], gap="large")
        with left, st.container(border=True, key="panel_1"):
            panel("Service demand over time", f"{selected['selection']['start_date']} → {selected['selection']['end_date']} · {explorer['frequency']} view", cohort_label)
            explore.demand_chart(explorer)
        with right, st.container(border=True, key="panel_2"):
            dimension = st.selectbox("Compare request mix", ["Agencies", "Boroughs", "Complaint types"], key="overview_mix")
            dimensions = {"Agencies": "agencies", "Boroughs": "boroughs", "Complaint types": "complaints"}
            panel(f"Requests by {dimension.lower()}", "Largest eight categories · same selected creation cohort")
            explore.breakdown(explorer, dimensions[dimension])
        explore.insights(explorer)
        explore.cohort_tables(explorer)
    else:
        st.info("The interactive cohort is unavailable. The saved evidence below remains accessible.")
    st.markdown('<div class="section-label">02 / From snapshot to a recoverable stream</div>', unsafe_allow_html=True)
    note("Saved streaming-demo scope · replay and recovery metrics below describe their own persisted demo cohort. They do not change with the exploration filters above.")
    left, right = st.columns([1.8, 1], gap="large")
    with left, st.container(border=True, key="panel_3"):
        state = stream.get("final_database", {}).get("counts", {})
        run_status = stream.get("run_status", "Not run")
        replay_source = "official snapshots" if str(stream.get("source_kind", "")).startswith("official") else "labelled fixture data"
        panel("Replay & recovery", f"Created/closed events reconstructed from {replay_source}", run_status,
              "passed" if run_status == "Passed" else "warning")
        metric_row([
            ("Replayed tickets", number(stream.get("snapshot_tickets")), "Small replay cohort; separate from the full analysis sample."),
            ("Stored predictions", number(state.get("ticket_predictions")), "Predictions atomically committed with creation events."),
            ("Committed batches", number(state.get("batch_commits")), "PostgreSQL batch ledger after replay and recovery."),
        ])
        st.markdown(f'<div class="pipeline"><div class="pipeline-step"><strong>Snapshot</strong><span>{escape(cohort_label.lower())}</span></div>'
                    '<div class="pipeline-step"><strong>Reconstruct</strong><span>created / closed</span></div>'
                    '<div class="pipeline-step"><strong>Replay</strong><span>Spark microbatches</span></div>'
                    '<div class="pipeline-step"><strong>Persist</strong><span>atomic PostgreSQL</span></div></div>', unsafe_allow_html=True)
        producer = stream.get("producer", {})
        st.caption(f"Injected tests: {number(producer.get('synthetic_update_rows'))} synthetic updates · "
                   f"{number(producer.get('duplicate_rows'))} duplicate rows · {number(producer.get('late_rows'))} late rows. "
                   "This replay is not a live CDC feed.")
    with right, st.container(border=True, key="panel_4"):
        panel("Recovery checks", "Results recorded by the streaming demo")
        check_row("Replay after database commit", stream.get("fault_after_commit_observed"))
        check_row("Second restart leaves state unchanged", stream.get("second_restart_unchanged"))
        check_row("Prediction coverage", bool(state.get("ticket_predictions") == stream.get("snapshot_tickets"))
                  if state and stream.get("snapshot_tickets") is not None else None)
        st.caption(f"{number(dq.get('rejected_rows'))} source rows quarantined · "
                   f"{number(dq.get('duplicate_silver_keys'))} duplicate silver keys.")
    with st.expander("Inspect the PostgreSQL cohort state"):
        st.caption("The summary above is a persisted demo result. Refresh below to query the running database.")
        if st.button("Refresh database state"):
            try:
                from nyc311.streaming.sink import database_snapshot
                from psycopg.conninfo import make_conninfo
                schema = stream.get("database_schema", "demo_official")
                if schema not in {"demo_official", "demo_fixture"}:
                    raise ValueError("Unexpected demo database schema")
                conninfo = make_conninfo(os.getenv("DATABASE_URL", "postgresql://nyc311:nyc311_local@localhost:55432/nyc311"),
                                         options=f"-csearch_path={schema}")
                snapshot = database_snapshot(conninfo)
                st.session_state["database_snapshot"] = snapshot
            except Exception as exc:
                st.warning(f"Database unavailable ({type(exc).__name__}). Start make db-up and run make stream-demo.")
        snapshot = st.session_state.get("database_snapshot")
        if snapshot:
            st.dataframe(pd.DataFrame(snapshot.get("agency_borough", [])), hide_index=True, use_container_width=True)
            st.json(snapshot, expanded=False)
        else:
            st.dataframe(pd.DataFrame(stream.get("final_database", {}).get("agency_borough", [])),
                         hide_index=True, use_container_width=True)
        st.caption("Backlog counts cover the selected replay cohort, not all outstanding requests in NYC.")
    evidence("Evidence & source provenance", "artifacts/metrics/data_quality.json", "artifacts/metrics/survival.json",
             "artifacts/metrics/streaming_metrics.json", "artifacts/metrics/source_manifest.json")

elif page == "Survival & Prediction":
    if explorer:
        m = explorer["summary"]["metrics"]
        metric_row([
            ("Selected survival cohort", number(m["label_rows"]), "Valid requests in the currently selected creation cohort, including right-censored requests."),
            ("Selected censoring rate", percent(m["censoring_fraction"]), f"Fixed observation cutoff remains {as_of}; date filters select creation dates, not closure cutoff."),
            ("Observed resolved", number(m["closed_observed_rows"]), "Observed closures in this selected cohort by the fixed as-of timestamp."),
            ("Right-censored", number(m["censored_rows"]), "Requests retained as censored in the selected Kaplan–Meier estimate."),
        ])
    curves_tab, evaluation_tab, prediction_tab = st.tabs(["Survival curves", "Model evaluation", "Predict a request"])
    with curves_tab:
        if explorer:
            with st.container(border=True, key="panel_5"):
                panel("How long do selected requests remain unresolved?", "Kaplan–Meier is recomputed for your filtered creation cohort, with the fixed as-of cutoff.", "Interactive cohort")
                choices = {"Agency": "agency", "Borough": "borough", "Complaint type": "complaint_type", "All selected requests": "all"}
                controls = st.columns([1, 1, 2])
                grouping = controls[0].selectbox("Compare by", list(choices), key="km_grouping")
                horizon = controls[1].select_slider("Time window (days)", options=[7, 30, 60, 90, 180], value=30, key="km_horizon")
                km = explore.km_result(explorer, choices[grouping])
                group_summary, curves = km["summary"], km["curves"]
                available = group_summary["group"].astype(str).tolist()
                groups = controls[2].multiselect("Groups · largest eight available", available, default=available[:5], key=f"km_groups_{choices[grouping]}")
                selected_curves = curves.loc[curves["group"].isin(groups)]
                if groups and not selected_curves.empty:
                    legend = alt.selection_point(name="km_legend", fields=["group"], bind="legend")
                    base = alt.Chart(selected_curves).encode(
                        x=alt.X("duration_days:Q", title="Days since request creation", scale=alt.Scale(domain=[0, horizon])),
                        color=alt.Color("group:N", scale=alt.Scale(range=COLORS), title=None))
                    line = base.mark_line(interpolate="step-after", strokeWidth=2.1, clip=True).encode(
                        y=alt.Y("survival_probability:Q", title="Probability unresolved", scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(format="%")),
                        opacity=alt.condition(legend, alt.value(1), alt.value(.12)),
                        tooltip=[alt.Tooltip("group:N", title=grouping), alt.Tooltip("duration_days:Q", title="Days", format=".2f"),
                                 alt.Tooltip("survival_probability:Q", title="Unresolved", format=".1%"),
                                 alt.Tooltip("lower_95:Q", title="Lower 95% CI", format=".1%"), alt.Tooltip("upper_95:Q", title="Upper 95% CI", format=".1%")]).add_params(legend)
                    show_ci = st.checkbox("Show 95% confidence intervals", value=False, key="km_show_ci")
                    if show_ci:
                        band = base.mark_area(interpolate="step-after", clip=True).encode(y="lower_95:Q", y2="upper_95:Q", opacity=alt.condition(legend, alt.value(.09), alt.value(.015)))
                        explore.render_chart(band + line, 340)
                    else:
                        explore.render_chart(line, 340)
                else:
                    st.info("Choose at least one group with matching requests to display its survival curve.")
                display = group_summary.loc[group_summary["group"].isin(groups), ["group", "sample_n", "closed_n", "censored_n", "censoring_fraction", "km_median_days"]].copy()
                display = display.rename(columns={"group": grouping, "sample_n": "Sample requests", "closed_n": "Observed closed", "censored_n": "Censored", "censoring_fraction": "Censoring rate", "km_median_days": "Median days"})
                display["Censoring rate"] = display["Censoring rate"] * 100
                st.dataframe(display, hide_index=True, use_container_width=True, column_config={"Censoring rate": st.column_config.NumberColumn(format="%.1f%%"), "Median days": st.column_config.NumberColumn(format="%.2f")})
                st.caption(f"Displayed groups cover {int(display['Sample requests'].sum()):,} of {km['metrics']['accepted_rows']:,} selected valid requests. Hover for estimates; click a legend label to highlight (Shift-click for several). Legend highlighting does not change the cohort or table. A missing median means 50% resolution was not reached within observed support.")
                finite = display.dropna(subset=["Median days"])
                if len(finite):
                    fastest = finite.sort_values("Median days").iloc[0]
                    note(f"Among displayed groups with an estimable median, {fastest[grouping]} has the shortest observed KM median ({fastest['Median days']:.2f} days) in this selected cohort. Complaint mix and difficulty differ between groups; this is not an agency performance ranking.")
                st.download_button("Download selected KM curves · CSV", selected_curves.to_csv(index=False), "nyc311_selected_km.csv", "text/csv", key="download_km")
            explore.insights(explorer, "What this survival cohort tells us", key="survival")
        else:
            empty("Interactive silver data is unavailable for filtered Kaplan–Meier estimates.", "make fetch-sample etl")
    with evaluation_tab:
        note("Saved model-evaluation scope · these scores use the persisted chronological training/test split, independent of the exploration filters. Filtering does not retrain the Cox model or change its holdout scores.")
        metric_row([("Saved training requests", number(model.get("train_rows")), "Persisted chronological training split."), ("Saved holdout requests", number(model.get("test_rows")), "Persisted most-recent creation period."), ("Holdout Cox concordance", number(model.get("cox", {}).get("c_index"), 3), "Persisted holdout ranking score; higher is better.")])
        left, right = st.columns([1.3, 1], gap="large")
        with left, st.container(border=True, key="panel_6"):
            panel("Chronological holdout performance", "Harrell C-index · larger values indicate better ranking")
            scores = pd.DataFrame([
                {"model": "CoxPH", "c_index": model.get("cox", {}).get("c_index")},
                {"model": "Grouped KM survival score", "c_index": model.get("baseline", {}).get("c_index")},
                {"model": "Grouped KM median", "c_index": model.get("median_baseline", {}).get("c_index")},
            ]).dropna(subset=["c_index"])
            if not scores.empty:
                chart(alt.Chart(scores).mark_bar(cornerRadiusEnd=4, size=30).encode(
                    y=alt.Y("model:N", title=None, sort=None),
                    x=alt.X("c_index:Q", title="Concordance index", scale=alt.Scale(domain=[0, 1])),
                    color=alt.Color("model:N", scale=alt.Scale(range=COLORS), legend=None),
                    tooltip=[alt.Tooltip("model:N", title="Model"), alt.Tooltip("c_index:Q", title="C-index", format=".4f")]), 240)
                st.dataframe(scores.rename(columns={"model": "Model", "c_index": "C-index"}), hide_index=True,
                             use_container_width=True, column_config={"C-index": st.column_config.NumberColumn(format="%.4f")})
            else:
                st.info("Model evaluation has not been run.")
        with right, st.container(border=True, key="panel_7"):
            panel("Evaluation boundaries", "Features use information available at request creation")
            split = model.get("split", {})
            st.markdown(f"**Train before**  \n{split.get('train_end', 'Not run')}\n\n"
                        f"**Test creation window**  \n{split.get('train_end', 'Not run')} → {split.get('test_end', 'Not run')}\n\n"
                        f"**Test observation cutoff**  \n{split.get('test_as_of', 'Not run')}")
            st.caption("Agency · complaint type · borough · month · weekday · hour")
            ph = model.get("proportional_hazards", {})
            if ph.get("bonferroni_violation_count", 0):
                note(f"Cox assumption check: {ph['bonferroni_violation_count']} of {ph.get('tested_features')} features violate proportional hazards after Bonferroni correction. Interpret predictions cautiously.", amber=True)
        baseline = model.get("median_baseline", {})
        st.caption(f"Cox and KM survival-score baseline use {number(model.get('test_rows'))} holdout requests. "
                   f"The KM median baseline uses {number(baseline.get('evaluated_rows'))}; "
                   f"{number(baseline.get('unreached_median_rows'))} have no finite training median. "
                   f"{number(model.get('excluded_outside_split_rows'))} silver records fall outside the explicit UTC split boundaries.")
    with prediction_tab:
        note("Enter a hypothetical request. Inference uses the saved Cox model and creation-time fields; the result is an estimate, not a service guarantee.")
        with st.form("predict"):
            panel("A new service request", "Use the information known when the request was created")
            left, right = st.columns(2, gap="large")
            agencies = sorted(dq.get("counts_by_agency", {}) or {"HPD": 0})
            complaints = sorted(dq.get("counts_by_complaint_type", {}) or {"HEAT/HOT WATER": 0})
            agency = left.selectbox("Agency at creation", agencies, index=agencies.index("HPD") if "HPD" in agencies else 0)
            borough = left.selectbox("Borough", ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND", "UNKNOWN"])
            complaint = right.selectbox("Complaint type", complaints, index=complaints.index("HEAT/HOT WATER") if "HEAT/HOT WATER" in complaints else 0)
            created = right.text_input("Created at · NYC local time or explicit UTC offset", "2025-03-15T12:00:00-04:00")
            submitted = st.form_submit_button("Predict resolution survival", type="primary", use_container_width=True)
        if submitted:
            try:
                from nyc311.analysis.predict import predict_record
                created_timestamp = pd.Timestamp(created)
                if pd.isna(created_timestamp):
                    raise ValueError("Enter a valid creation timestamp.")
                if created_timestamp.tzinfo is None:
                    try:
                        created_timestamp = created_timestamp.tz_localize("America/New_York", ambiguous="raise", nonexistent="raise")
                    except Exception as exc:
                        raise ValueError("This NYC local time is ambiguous or does not exist at the daylight-saving transition. Enter an explicit UTC offset.") from exc
                prediction = predict_record({"agency": agency, "complaint_type": complaint, "borough": borough,
                                             "created_at": created_timestamp.isoformat()})
                median = prediction.get("predicted_median_days")
                metric_row([
                    ("Estimated median", f"{median:.2f} days" if median is not None else "Not reached", "A null median means 50% resolution was not reached within observed support."),
                    ("Resolved within 7 days", percent(prediction.get("p_closed_7d")), "Predicted probability, conditional on the creation-time inputs."),
                    ("Resolved within 30 days", percent(prediction.get("p_closed_30d")), "Predicted probability within observed training support."),
                ])
                st.caption(f"Model · {prediction.get('model_version', 'Unavailable')} · Follow-up support · "
                           f"{number(prediction.get('horizon_support_days'), 1)} days. Horizons outside support are unavailable.")
                with st.expander("Prediction evidence"):
                    st.json(prediction)
            except Exception as exc:
                st.warning(f"Prediction unavailable: {exc}")
        else:
            st.caption(f"Active model · {model.get('model_version', 'Not run')} · Snapshot category assignments may have changed after creation.")
    evidence("Evidence & model methodology", "artifacts/metrics/survival.json", "artifacts/metrics/model_evaluation.json")

elif page == "Seasonality & Capacity":
    demand_tab, capacity_tab = st.tabs(["Demand & weather", "Capacity scenarios"])
    with demand_tab:
        if explorer:
            selected = explorer["summary"]
            m = selected["metrics"]
            filtered_weather = explore.weather_result(explorer)
            metric_row([
                ("Selected sample requests", number(m["selected_rows"]), "Request creation cohort after all active date and category filters."),
                ("Mean daily arrivals", number(m["mean_daily_count"], 1), "Average selected arrivals over verified covered sample days."),
                ("Covered extraction days", f"{m['covered_days']} / {m['calendar_days']}", "Complete extraction of the selected sample, not population-complete coverage."),
                ("Paired weather days", number(filtered_weather["metrics"]["paired_days"]), "Covered dates with both filtered sample counts and downloaded weather."),
            ])
            with st.container(border=True, key="panel_8"):
                panel("Demand in your selected window", f"{selected['selection']['start_date']} → {selected['selection']['end_date']} · same date and category filters", "Recomputed")
                explore.demand_chart(explorer)
            left, right = st.columns([1.4, 1], gap="large")
            with left, st.container(border=True, key="panel_9"):
                panel("Weather & selected arrivals", "Every point is one covered day in this filtered sample", "Association only")
                variables = {"Minimum temperature": "temperature_2m_min", "Maximum temperature": "temperature_2m_max", "Precipitation": "precipitation_sum"}
                variable = st.selectbox("Weather variable", list(variables), key="weather_variable")
                explore.weather_chart(filtered_weather, variables[variable])
            with right, st.container(border=True, key="panel_10"):
                panel("What this relationship supports", "The association is recalculated when your cohort changes")
                associations = filtered_weather["associations"]
                row = associations.loc[associations.weather_variable.eq(variables[variable])] if not associations.empty else pd.DataFrame()
                if not row.empty:
                    value = row.iloc[0]
                    st.metric("Filtered arrivals / weather · Spearman ρ", number(value.spearman_rho, 3), help="Unadjusted rank correlation across covered paired calendar days; it is not causal.")
                    st.caption(f"{number(value.n_days)} paired days · {value.status} · minimum 10 days and variation in both variables required.")
                    if value.status == "Estimable":
                        note(f"The selected sample has a {'positive' if value.spearman_rho > 0 else 'negative' if value.spearman_rho < 0 else 'zero'} rank association (ρ={value.spearman_rho:+.3f}) with {variable.lower()}. Complaint mix, sampling and season may explain the relationship. This is exploratory evidence, not a weather effect or a forecast.")
                    else:
                        note(f"No direction is inferred: {value.reason}.", amber=True)
                else:
                    st.info(filtered_weather["metrics"].get("reason", "Weather is unavailable for this cohort."))
                note("The available quarter-year history cannot establish annual seasonality. Weather comes from one NYC reference point and is used for retrospective analysis only.", amber=True)
            explore.insights(explorer, "What this demand window tells us", key="demand")
            explore.cohort_tables(explorer, key="demand")
            with st.expander("Filtered weather association details"):
                if not associations.empty:
                    st.dataframe(associations, hide_index=True, use_container_width=True)
                    st.download_button("Download filtered weather pairs · CSV", filtered_weather["joined"].to_csv(index=False), "nyc311_filtered_weather.csv", "text/csv", key="download_weather")
        else:
            empty("Interactive silver data or coverage is unavailable.", "make fetch-sample etl fetch-weather")
    with capacity_tab:
        note("Saved capacity-analysis scope · calibration and scenarios below use the persisted full analytical cohort. Exploration filters above do not recalibrate the queue model or change these scenarios.")
        metric_row([("Saved capacity groups", number(capacity.get("groups_selected")), "Groups selected in the persisted analysis."), ("Calibrated groups", number(capacity.get("groups_calibrated")), "Only passing groups may publish scenario improvement estimates.")])
        groups = read_csv("artifacts/metrics/capacity_group_summary.csv")
        if not groups.empty:
            calibrated = capacity.get("groups_calibrated", 0)
            failed = capacity.get("groups_failed_calibration", 0)
            if failed:
                note(f"{failed} of {capacity.get('groups_selected')} groups failed baseline calibration. Improvement estimates for those groups are withheld. This is a diagnostic result, not evidence that capacity changes cannot help.", amber=True)
            with st.container(border=True, key="panel_11"):
                panel("Does the queue model match the observed cohort?", "Median resolution time · observed Kaplan–Meier versus the FIFO baseline",
                      f"{calibrated} calibrated", "passed" if calibrated else "warning")
                medians = groups[["agency", "borough", "observed_km_median_days", "baseline_median_days"]].copy()
                medians["group"] = medians.agency + " · " + medians.borough.str.title()
                medians = medians.melt(id_vars="group", value_vars=["observed_km_median_days", "baseline_median_days"],
                                       var_name="series", value_name="days")
                medians["series"] = medians.series.map({"observed_km_median_days": "Observed KM", "baseline_median_days": "FIFO baseline"})
                chart(alt.Chart(medians).mark_bar(cornerRadiusEnd=3).encode(
                    x=alt.X("days:Q", title="Median resolution time (days)"),
                    y=alt.Y("group:N", title=None, sort=None), yOffset="series:N",
                    color=alt.Color("series:N", scale=alt.Scale(domain=["Observed KM", "FIFO baseline"], range=["#117C83", "#C69752"])),
                    tooltip=[alt.Tooltip("group:N", title="Group"), alt.Tooltip("series:N", title="Estimate"),
                             alt.Tooltip("days:Q", title="Days", format=".2f")]), 300)
                summary = groups[["agency", "borough", "sample_n", "current_backlog", "baseline_to_observed_median_ratio", "calibration_status"]].rename(columns={
                    "agency": "Agency", "borough": "Borough", "sample_n": "Sample requests", "current_backlog": "Cohort backlog",
                    "baseline_to_observed_median_ratio": "Baseline / observed", "calibration_status": "Calibration"})
                st.dataframe(summary, hide_index=True, use_container_width=True,
                             column_config={"Baseline / observed": st.column_config.NumberColumn(format="%.2f×")})
            scenarios = read_csv("artifacts/metrics/capacity_scenarios.csv")
            with st.expander("Scenario outputs & calibration details"):
                st.caption("NA improvement values are intentionally withheld when calibration fails; they do not mean zero improvement.")
                if not scenarios.empty:
                    display = scenarios[["agency", "borough", "scenario_capacity_uplift", "modelled_delta_days", "delta_publication_status"]].copy()
                    display["scenario_capacity_uplift"] = display.scenario_capacity_uplift.map(lambda v: f"+{v:.0%}")
                    display = display.rename(columns={"agency": "Agency", "borough": "Borough", "scenario_capacity_uplift": "Service-opportunity uplift",
                        "modelled_delta_days": "Modelled improvement (days)", "delta_publication_status": "Publication status"})
                    st.dataframe(display, hide_index=True, use_container_width=True)
                st.dataframe(groups[["agency", "borough", "calibration_status", "calibration_reason"]], hide_index=True, use_container_width=True)
            st.caption("Closure counts proxy service opportunities only within the downloaded cohort. FIFO, homogeneous difficulty, and proportional uplift are assumptions. Backlog is not a citywide total; these are not causal staffing estimates.")
        else:
            empty("Capacity analysis has not been run.", "make analyze")
    evidence("Evidence & analytical assumptions", "artifacts/metrics/seasonality_summary.json", "artifacts/metrics/capacity_summary.json")

else:
    local_runs = read_csv("artifacts/benchmark/local/runs.csv")
    worker_runs = read_csv("artifacts/benchmark/workers/runs.csv")
    local_summary = read_csv("artifacts/benchmark/local/summary.csv")
    worker_summary = read_csv("artifacts/benchmark/workers/summary.csv")
    all_runs = pd.concat([local_runs, worker_runs], ignore_index=True)
    successful = int(all_runs.run_status.eq("Succeeded").sum()) if "run_status" in all_runs else 0
    failed = int(all_runs.run_status.eq("Failed").sum()) if "run_status" in all_runs else 0
    metric_row([
        ("Successful measured runs", number(successful), "All successful raw repetitions in the two displayed benchmark suites."),
        ("Failed runs", number(failed), "Failed raw runs are retained in the evidence tables."),
        ("Largest measured input", number(all_runs.input_rows.max()) if "input_rows" in all_runs else "Not run", "Scale-test replicas are synthetic; not additional official observations."),
        ("Largest worker configuration", number(worker_runs.worker_count.max()) if "worker_count" in worker_runs else "Not run", "Separate Spark standalone workers on a single physical host."),
    ])
    data_tab, worker_tab = st.tabs(["Data volume · 1× / 10× / 100×", "Worker scaling · 1 / 2 / 4 / 8"])

    def benchmark_slice(summary, runs, mode):
        """Keep medians and repetition points within one compatible experiment."""
        if summary.empty:
            return summary, runs
        fields = ["experiment", "master", "source_sha256", "partitions", "executor_memory", "executor_cores",
                  "workload", "cluster_id", "driver_memory", "base_rows", "base_sample_method", "base_source_kinds"]
        if mode == "workers":
            fields += ["input_sha256", "data_multiplier"]
        fields = [field for field in fields if field in summary and field in runs]
        if not fields:
            return summary, runs
        configurations = summary[fields].drop_duplicates().reset_index(drop=True)
        selected = 0
        if len(configurations) > 1:
            def describe(index):
                row = configurations.iloc[index]
                return (f"Configuration {index + 1} · {row.get('master', 'Spark')} · "
                        f"{row.get('partitions', '?')} partitions · {row.get('executor_memory', '?')} executor memory · "
                        f"source {str(row.get('source_sha256', 'unknown'))[:8]}")
            selected = st.selectbox("Measured configuration", range(len(configurations)), format_func=describe,
                                    key=f"configuration_{mode}")
        configuration = configurations.iloc[selected]

        def select(frame):
            mask = pd.Series(True, index=frame.index)
            for field in fields:
                value = configuration[field]
                mask &= frame[field].isna() if pd.isna(value) else frame[field].eq(value)
            return frame.loc[mask].copy()

        return select(summary), select(runs)

    def completed(summary):
        if summary.empty or "repetitions_complete" not in summary:
            return summary.iloc[:0]
        complete = summary.repetitions_complete.astype(str).str.lower().eq("true")
        return summary.loc[complete & summary.successful_runs.ge(3)].copy()

    def measured_chart(summary, runs, x_field, y_field, raw_field, y_title, x_title):
        order = sorted(summary[x_field].dropna().unique().tolist())
        base = alt.Chart(summary).encode(x=alt.X(f"{x_field}:O", title=x_title, sort=order, axis=alt.Axis(labelAngle=0)))
        bars = base.mark_bar(color="#5A9C99", cornerRadiusTopLeft=4, cornerRadiusTopRight=4, size=45).encode(
            y=alt.Y(f"{y_field}:Q", title=y_title),
            tooltip=[alt.Tooltip(f"{x_field}:O", title=x_title), alt.Tooltip(f"{y_field}:Q", title=f"Median {y_title.lower()}", format=".2f"),
                     alt.Tooltip("successful_runs:Q", title="Successful repetitions")])
        eligible = runs.loc[runs.run_status.eq("Succeeded") & runs[x_field].isin(order)]
        dots = alt.Chart(eligible).mark_circle(color="#173E49", size=44, opacity=.68, stroke="#FFFFFF", strokeWidth=1).encode(
            x=alt.X(f"{x_field}:O", sort=order), y=alt.Y(f"{raw_field}:Q"),
            tooltip=[alt.Tooltip(f"{x_field}:O", title=x_title), alt.Tooltip("repetition:Q", title="Repetition"),
                     alt.Tooltip(f"{raw_field}:Q", title=y_title, format=".2f")])
        chart(bars + dots, 265)

    def benchmark_evidence(summary, runs, mode):
        key = "worker_count" if mode == "workers" else "data_multiplier"
        if not summary.empty:
            display = summary[[key, "input_rows", "successful_runs", "attempted_runs", "failed_runs", "median_elapsed_seconds",
                               "median_throughput_rows_per_sec", "repetitions_complete"]].copy()
            display["Repetitions"] = display.successful_runs.astype(str) + " / " + display.attempted_runs.astype(str)
            display["Status"] = display.repetitions_complete.astype(str).str.lower().map({"true": "Complete", "false": "Incomplete"})
            display = display.drop(columns=["successful_runs", "attempted_runs", "repetitions_complete"]).rename(columns={
                key: "Workers" if mode == "workers" else "Multiplier", "input_rows": "Input rows", "failed_runs": "Failed",
                "median_elapsed_seconds": "Median runtime (s)", "median_throughput_rows_per_sec": "Median rows / sec"})
            st.dataframe(display, hide_index=True, use_container_width=True,
                column_config={"Median runtime (s)": st.column_config.NumberColumn(format="%.2f"),
                               "Median rows / sec": st.column_config.NumberColumn(format="%.1f")})
        with st.expander("Measured runs & reproducibility details"):
            st.caption(f"Sources · artifacts/benchmark/{mode}/runs.csv and summary.csv")
            if not runs.empty:
                cols = [key, "repetition", "input_rows", "elapsed_seconds", "throughput_rows_per_sec", "run_status", "reason"]
                st.dataframe(runs[[c for c in cols if c in runs]], hide_index=True, use_container_width=True)
                st.download_button("Download measured runs (CSV)", runs.to_csv(index=False), f"{mode}_benchmark_runs.csv", "text/csv", key=f"download_{mode}")
            st.json(read_json(f"artifacts/benchmark/{mode}/audit.json"), expanded=False)
            if not summary.empty:
                st.dataframe(summary, hide_index=True, use_container_width=True)

    with data_tab:
        note("Data-volume stress tests use labelled synthetic replicas of the base sample, not additional official observations. Spark local[2] uses driver threads; it is not a two-worker cluster.")
        local_summary, local_runs = benchmark_slice(local_summary, local_runs, "local")
        complete = completed(local_summary)
        if not complete.empty:
            left, right = st.columns(2, gap="large")
            with left, st.container(border=True, key="panel_12"):
                panel("Runtime by data volume", "Bars: median runtime · dots: individual measured repetitions")
                measured_chart(complete, local_runs, "data_multiplier", "median_elapsed_seconds", "elapsed_seconds", "Runtime (seconds)", "Data multiplier")
            with right, st.container(border=True, key="panel_13"):
                panel("Throughput by data volume", "Same ETL workload and local Spark configuration")
                measured_chart(complete, local_runs, "data_multiplier", "median_throughput_rows_per_sec", "throughput_rows_per_sec", "Throughput (rows / second)", "Data multiplier")
            st.caption("Timings exclude Spark startup. Only configurations with at least three successful repetitions appear in median charts.")
        else:
            empty("No data-volume configuration has three successful measured repetitions.", "make bench")
        benchmark_evidence(local_summary, local_runs, "local")
    with worker_tab:
        note("Standalone workers share one physical host. These measurements test process-level parallelism, not cross-machine scaling or a production cluster.")
        worker_summary, worker_runs = benchmark_slice(worker_summary, worker_runs, "workers")
        complete = completed(worker_summary)
        if not complete.empty:
            left, right = st.columns(2, gap="large")
            with left, st.container(border=True, key="panel_14"):
                panel("Runtime by worker count", "Bars: median runtime · dots: individual measured repetitions")
                measured_chart(complete, worker_runs, "worker_count", "median_elapsed_seconds", "elapsed_seconds", "Runtime (seconds)", "Workers")
            with right, st.container(border=True, key="panel_15"):
                panel("Throughput by worker count", "Fixed input and ETL workload across configurations")
                measured_chart(complete, worker_runs, "worker_count", "median_throughput_rows_per_sec", "throughput_rows_per_sec", "Throughput (rows / second)", "Workers")
            left, right = st.columns(2, gap="large")
            with left, st.container(border=True, key="panel_16"):
                panel("Speedup relative to one worker", "One-worker median runtime / configuration median runtime")
                base = alt.Chart(complete).encode(x=alt.X("worker_count:O", title="Workers", axis=alt.Axis(labelAngle=0)))
                line = base.mark_line(point=alt.OverlayMarkDef(filled=True, size=75), color="#117C83", strokeWidth=2.5).encode(
                    y=alt.Y("speedup:Q", title="Speedup (×)", scale=alt.Scale(zero=True)),
                    tooltip=[alt.Tooltip("worker_count:O", title="Workers"), alt.Tooltip("speedup:Q", title="Speedup", format=".3f")])
                reference = alt.Chart(pd.DataFrame({"baseline": [1]})).mark_rule(color="#B5C4BC", strokeDash=[5, 4]).encode(y="baseline:Q")
                chart(line + reference, 230)
            with right, st.container(border=True, key="panel_17"):
                panel("Parallel efficiency", "Speedup / worker count · single-host results")
                chart(alt.Chart(complete).mark_line(point=alt.OverlayMarkDef(filled=True, size=75), color="#4D79A6", strokeWidth=2.5).encode(
                    x=alt.X("worker_count:O", title="Workers", axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("parallel_efficiency:Q", title="Efficiency", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                    tooltip=[alt.Tooltip("worker_count:O", title="Workers"), alt.Tooltip("parallel_efficiency:Q", title="Efficiency", format=".1%")]), 230)
            multi_worker_speedup = complete.loc[complete.worker_count.gt(1), "speedup"]
            if not multi_worker_speedup.empty and multi_worker_speedup.lt(1).all():
                note("For this small workload, every multi-worker configuration was slower than the one-worker median. More workers do not guarantee a speedup.", amber=True)
            st.caption("Timings exclude Spark startup. Medians require three successful repetitions; failed and incomplete configurations remain in the evidence table.")
        else:
            empty("No worker configuration has three successful measured repetitions.", "make bench-workers")
        benchmark_evidence(worker_summary, worker_runs, "workers")

st.markdown('<div class="footer"><span>NYC 311 · Streaming Analytics, Resolution-Time Prediction & Capacity Planning</span>'
            '<span><a href="https://data.cityofnewyork.us/resource/erm2-nwe9.json" target="_blank" rel="noopener noreferrer">NYC Open Data</a>'
            ' · Persisted project evidence</span></div>', unsafe_allow_html=True)
