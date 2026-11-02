"""Interactive presentation of provenance-guarded, filtered silver cohorts."""
from __future__ import annotations

from datetime import date
from hashlib import sha1
from html import escape
import json

import altair as alt
import pandas as pd
import streamlit as st

from nyc311.analysis.explorer import (
    available_dates, filter_requests, grouped_km, load_explorer_inputs,
    summarize_selection, weather_selection,
)
from nyc311.config import ROOT

COLORS = ["#117C83", "#4D79A6", "#C69752", "#A07E9F", "#74988B", "#C77B68"]
FREQUENCIES = {"Day": "daily", "Week": "weekly", "Month": "monthly"}


@st.cache_data(show_spinner=False)
def _load(version):
    return load_explorer_inputs()


@st.cache_data(show_spinner=False)
def _summary(frame, coverage, start, end, agencies, boroughs, complaints, frequency, as_of):
    return summarize_selection(frame, coverage, start, end, agencies=agencies,
        boroughs=boroughs, complaint_types=complaints, frequency=frequency, as_of=as_of)


@st.cache_data(show_spinner=False)
def _km(frame, grouping, as_of):
    return grouped_km(frame, group_by=grouping, as_of=as_of, top_n=8)


def styled(plot, height=290):
    return (plot.properties(height=height).configure_view(strokeWidth=0)
        .configure_axis(labelColor="#7A8B92", titleColor="#61767E", labelFontSize=11,
            titleFontSize=11, titleFontWeight=400, gridColor="#EDF1EE", domain=False,
            tickSize=0, labelPadding=9, titlePadding=12)
        .configure_legend(labelColor="#61767E", title=None, orient="bottom", labelFontSize=11,
            symbolType="circle", padding=12)
        .configure(background="#FFFFFF", font="Arial"))


def render_chart(plot, height=290, **kwargs):
    return st.altair_chart(styled(plot, height), use_container_width=True, theme=None, **kwargs)


def _safe_number(value, digits=0):
    return "Unavailable" if value is None or pd.isna(value) else f"{value:,.{digits}f}"


def _date_bound(value):
    # The chart encodes local calendar dates on a UTC scale, not event instants.
    if isinstance(value, (int, float)):
        return pd.to_datetime(value, unit="ms", utc=True).date()
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("Missing chart date")
    return stamp.date()


def _categories(label, column, frame, key):
    all_selected = st.checkbox(f"All {label.lower()}", value=True, key=f"explore_all_{key}")
    values = sorted(frame[column].dropna().astype(str).unique().tolist())
    selected = st.multiselect(label, values, default=[], disabled=all_selected,
        placeholder=f"Choose {label.lower()}", key=f"explore_{key}")
    return None if all_selected else selected


def controls(as_of):
    """Render shared filters and a stable master timeline; return linked analysis."""
    manifest = ROOT / "artifacts/metrics/source_manifest.json"
    parquet = list((ROOT / "data/silver").rglob("*.parquet"))
    version = tuple((str(path), path.stat().st_mtime_ns, path.stat().st_size)
                    for path in [manifest, *parquet] if path.exists())
    try:
        frame, coverage = _load(version)
    except Exception as exc:
        st.warning(f"Interactive silver cohort unavailable: {exc}")
        st.caption("Generate the official sample and silver artifacts with make fetch-sample etl.")
        return None
    dates = available_dates(coverage)
    if not len(dates):
        st.info("No downloaded calendar dates are available to explore.")
        return None
    with st.container(border=True, key="panel_explore"):
        st.markdown('<div class="filter-heading"><div><div class="eyebrow">EXPLORE REQUESTS</div>'
                    '<h3>Your question, your cohort.</h3></div>'
                    '<span class="status-badge">Live calculations · saved silver data</span></div>', unsafe_allow_html=True)
        top = st.columns([1, 1.4, 1, 1.25])
        years = sorted({day.year for day in dates})
        year = top[0].selectbox("Available year", ["All available", *years], key="explore_year")
        mode = top[1].selectbox("Date selection", ["Date range", "On or before date"], key="explore_mode")
        frequency = top[2].selectbox("Chart aggregation", list(FREQUENCIES), key="explore_frequency")
        smoothing = top[3].select_slider("Moving average · calendar days", options=[1, 3, 7, 14, 28], value=7,
                                       key="explore_smoothing")
        year_dates = dates if year == "All available" else dates[dates.year == year]
        lower, upper = year_dates.min().date(), year_dates.max().date()
        date_key = str(year).replace(" ", "_")
        second = st.columns([1, 1, 1.3])
        if mode == "Date range":
            start = second[0].date_input("Created on or after", value=lower, min_value=lower, max_value=upper,
                key=f"explore_start_{date_key}")
            end = second[1].date_input("Created on or before", value=upper, min_value=lower, max_value=upper,
                key=f"explore_end_{date_key}")
        else:
            start = lower
            end = second[0].date_input("Created on or before · inclusive", value=upper, min_value=lower,
                max_value=upper, key=f"explore_cutoff_{date_key}")
            second[1].caption(f"From the first downloaded day in this year selection: {lower}.")
        compare = second[2].checkbox("Compare previous equal-length period", value=False, key="explore_compare")
        with st.expander("Refine the cohort · agency, borough & complaint type", expanded=False):
            cols = st.columns(3)
            with cols[0]:
                agencies = _categories("Agencies", "agency", frame, "agencies")
            with cols[1]:
                boroughs = _categories("Boroughs", "borough", frame, "boroughs")
            with cols[2]:
                complaints = _categories("Complaint types", "complaint_type", frame, "complaints")
            st.caption("Uncheck an All option to choose categories. An empty category selection deliberately matches no requests.")
        st.caption(f"Downloaded coverage: {dates.min().date()} – {dates.max().date()} · "
                   f"{len(dates):,} calendar days · America/New_York · systematic sample, not NYC totals. "
                   "Only downloaded years are offered; date filters do not download new history.")
        if end < start:
            st.warning("Choose an end date on or after the start date.")
            return None
        full = _summary(frame, coverage, start, end, agencies, boroughs, complaints, FREQUENCIES[frequency], as_of)
        context = json.dumps([str(start), str(end), agencies, boroughs, complaints], sort_keys=True)
        context_id = sha1(context.encode()).hexdigest()[:12]
        epoch = st.session_state.get("explore_brush_epoch", 0)
        navigator_key = f"explore_navigator_{context_id}_{epoch}"
        nav = full["daily"].copy()
        nav["date"] = pd.to_datetime(nav["date"], utc=True)
        nav["date_label"] = nav["date"].dt.strftime("%Y-%m-%d")
        brush = alt.selection_interval(name="cohort_window", encodings=["x"], clear="dblclick")
        nav_base = alt.Chart(nav).encode(x=alt.X("date:T", title=None,
            scale=alt.Scale(type="utc"), axis=alt.Axis(format="%b %d", tickCount=6)))
        nav_chart = nav_base.mark_area(color="#559C97", opacity=.35, line={"color": "#117C83", "strokeWidth": 1.5}).encode(
            y=alt.Y("count:Q", title="Daily requests", axis=alt.Axis(tickCount=3)),
            tooltip=[alt.Tooltip("date_label:N", title="Created date"),
                     alt.Tooltip("count:Q", title="Sample arrivals", format=",")]).add_params(brush)
        st.markdown('<div class="navigator-label">TIMELINE NAVIGATOR <span>Drag across dates to focus every linked view below</span></div>',
                    unsafe_allow_html=True)
        event = render_chart(nav_chart, 105, key=navigator_key, on_select="rerun", selection_mode=["cohort_window"])
        selected = event.get("selection", {}).get("cohort_window", {}) if event else {}
        bounds = selected.get("date", selected.get("x"))
        active_start, active_end = start, end
        brushed = False
        if isinstance(bounds, (list, tuple)) and len(bounds) == 2:
            try:
                active_start = max(start, _date_bound(bounds[0]))
                active_end = min(end, _date_bound(bounds[1]))
                if active_start <= active_end:
                    brushed = True
                else:
                    active_start, active_end = start, end
            except (ValueError, TypeError, OverflowError):
                active_start, active_end = start, end
        status, reset = st.columns([4, 1])
        status.caption(f"{'Brush active' if brushed else 'Active creation window'} · {active_start} through {active_end}, inclusive. "
                       "Metrics, detail charts, cohort tables and explanations below use these dates. "
                       "Double-click the navigator or reset to clear the brush.")
        if reset.button("Reset chart focus", key="explore_reset_brush", use_container_width=True):
            st.session_state["explore_brush_epoch"] = epoch + 1
            st.rerun()
    summary = _summary(frame, coverage, active_start, active_end, agencies, boroughs, complaints,
                       FREQUENCIES[frequency], as_of)
    selected_frame = filter_requests(frame, active_start, active_end, agencies=agencies,
        boroughs=boroughs, complaint_types=complaints)
    return {"summary": summary, "frame": selected_frame, "coverage": coverage,
            "frequency": FREQUENCIES[frequency], "smoothing": smoothing, "compare": compare,
            "as_of": as_of, "brushed": brushed, "context": context_id}


def _insight_cards(cards, key):
    html = f'<div class="insight-grid insight-grid-{len(cards)}">'
    for index, card in enumerate(cards):
        html += (f'<article class="insight-card {escape(card.get("tone", "neutral"))}">'
                 f'<span class="insight-index">{index + 1:02d}</span>'
                 f'<h4>{escape(card["title"])}</h4><p>{escape(card["body"])}</p></article>')
    st.markdown(html + "</div>", unsafe_allow_html=True)


def insights(explorer, title="What this view tells us", key="overview"):
    summary = explorer["summary"]
    cards = list(summary["insights"])
    if not explorer["compare"]:
        cards = [card for card in cards if card["title"] not in {"Change versus previous window", "Movement inside this window", "Trend comparison unavailable"}]
        trend = summary["trend"]
        if trend["status"] == "Estimable":
            movement = {"title": "Movement inside this window", "tone": "neutral", "body":
                f"{trend['method']}: {trend['first_count']:,} arrivals ({trend['first_start']}–{trend['first_end']}) "
                f"versus {trend['last_count']:,} ({trend['last_start']}–{trend['last_end']}), {trend['percent_change']:+.1f}%. "
                "This describes sampled arrivals, not a causal effect, annual seasonality or citywide demand."}
        else:
            movement = {"title": "Trend needs more support", "tone": "muted", "body":
                f"{trend['reason']}. No direction of change is inferred from this selection."}
        cards.insert(1, movement)
    elif summary["comparison"]["status"] != "Estimable" or summary["comparison"]["percent_change"] is None:
        compare = summary["comparison"]
        cards.insert(1, {"title": "Previous-period comparison unavailable", "tone": "muted", "body":
            f"{compare['previous_start']}–{compare['previous_end']}: {compare['reason']}. "
            f"Covered days: {compare['previous_covered_days']}/{compare['calendar_days_each']}. Unknown dates are not zero arrivals."})
    with st.container(border=True, key=f"panel_insights_{key}"):
        st.markdown(f'<div class="panel-title"><h3>{escape(title)}</h3><span class="status-badge passed">Updates with your filters</span></div>',
                    unsafe_allow_html=True)
        st.caption(f"Creation cohort · {summary['selection']['start_date']} → {summary['selection']['end_date']} · "
                   f"{summary['metrics']['selected_rows']:,} requests. Follow-up cutoff remains {explorer['as_of']}.")
        # Lead with three observations; retain follow-up and sample limitations explicitly.
        primary = [card for card in cards if card["title"] in {"Selected cohort", "Movement inside this window", "Trend needs more support", "Change versus previous window", "Previous-period comparison unavailable", "Trend comparison unavailable", "Agency mix"}]
        _insight_cards(primary[:4] if explorer["compare"] else primary[:3], key)
        other = [card for card in cards if card not in primary]
        with st.expander("Interpretation, follow-up & sample limitations"):
            for card in other:
                st.markdown(f"**{card['title']}**  \n{card['body']}")
        text = f"{title}\nCreation cohort: {summary['selection']['start_date']} through {summary['selection']['end_date']}\nFixed as_of: {explorer['as_of']}\n\n"
        text += "Category filters (null means all): " + json.dumps({key: summary["selection"][key] for key in ["agencies", "boroughs", "complaint_types"]}, sort_keys=True) + "\n\n"
        text += "\n\n".join(f"{card['title']}\n{card['body']}" for card in cards)
        cols = st.columns(2)
        cols[0].download_button("Download filtered series · CSV", series(explorer).to_csv(index=False),
            "nyc311_filtered_series.csv", "text/csv", key=f"explore_series_{key}", use_container_width=True)
        cols[1].download_button("Download current interpretation · TXT", text, "nyc311_current_insights.txt",
            "text/plain", key=f"explore_insights_{key}", use_container_width=True)


def series(explorer):
    summary = explorer["summary"]
    result = summary["buckets"].copy()
    daily = summary["daily"].copy()
    smoothing = explorer["smoothing"]
    daily["moving_average"] = daily["count"].rolling(smoothing, min_periods=smoothing).mean()
    # At weekly/monthly resolution expose the daily moving average at each bucket's last selected day.
    if explorer["frequency"] == "daily":
        result["moving_average"] = daily["moving_average"].to_numpy()
    else:
        grouped = daily.assign(period=daily["date"].dt.to_period("W-SUN" if explorer["frequency"] == "weekly" else "M"))
        means = grouped.groupby("period")["moving_average"].agg(lambda values: values.iloc[-1])
        period = result["date"].dt.to_period("W-SUN" if explorer["frequency"] == "weekly" else "M")
        result["moving_average"] = period.map(means).to_numpy()
    result["moving_average_days"] = smoothing
    result["selection_start"] = summary["selection"]["start_date"]
    result["selection_end_inclusive"] = summary["selection"]["end_date"]
    result["as_of"] = explorer["as_of"]
    result["selection_filters"] = json.dumps({key: summary["selection"][key] for key in ["agencies", "boroughs", "complaint_types"]}, sort_keys=True)
    result["bucket_kind"] = result["full_bucket"].map({True: "Full calendar period", False: "Partial calendar period"})
    result["coverage"] = result["coverage_complete"].map({True: "Complete selected sample", False: "Unknown extraction coverage"})
    return result


def demand_chart(explorer, height=285):
    work = series(explorer)
    work["date"] = pd.to_datetime(work["date"], utc=True)
    work["period_end"] = pd.to_datetime(work["period_end"], utc=True)
    work["date_label"] = work["date"].dt.strftime("%Y-%m-%d")
    work["period_end_label"] = work["period_end"].dt.strftime("%Y-%m-%d")
    is_daily = explorer["frequency"] == "daily"
    value = "count" if is_daily else "mean_daily_count"
    label = "Sample arrivals per day" if is_daily else "Mean daily arrivals in period"
    nearest = alt.selection_point(name="hover_date", nearest=True, on="pointerover", fields=["date"], empty=False, clear="pointerout")
    zoom = alt.selection_interval(name="detail_zoom", bind="scales", encodings=["x"])
    x = alt.X("date:T", title=None, scale=alt.Scale(type="utc"), axis=alt.Axis(format="%b %d", tickCount=6))
    base = alt.Chart(work).encode(x=x)
    tips = [alt.Tooltip("date_label:N", title="Period starts"),
            alt.Tooltip("period_end_label:N", title="Period ends"),
            alt.Tooltip("count:Q", title="Sample arrivals", format=","),
            alt.Tooltip("mean_daily_count:Q", title="Mean per selected day", format=".1f"),
            alt.Tooltip("moving_average:Q", title=f"{explorer['smoothing']}-day moving mean", format=".1f"),
            alt.Tooltip("bucket_kind:N", title="Calendar bucket"), alt.Tooltip("coverage:N", title="Coverage")]
    area = base.mark_area(color="#117C83", opacity=.09).encode(y=alt.Y(f"{value}:Q", title=label))
    line = base.mark_line(color="#117C83", strokeWidth=2.2, point=alt.OverlayMarkDef(filled=True, size=18)).encode(y=f"{value}:Q")
    trend = base.mark_line(color="#C69752", strokeDash=[5, 4], strokeWidth=2).encode(y="moving_average:Q")
    hit = base.mark_point(opacity=0).encode(y=f"{value}:Q").add_params(nearest)
    points = base.mark_circle(color="#117C83", size=65).encode(y=f"{value}:Q", opacity=alt.condition(nearest, alt.value(1), alt.value(0)))
    rule = base.mark_rule(color="#82958F", strokeDash=[3, 3]).encode(tooltip=tips).transform_filter(nearest)
    render_chart((area + line + trend + hit + points + rule).add_params(zoom), height)
    st.caption(f"Teal: {'daily counts' if is_daily else 'mean daily count per calendar bucket'} · gold: {explorer['smoothing']}-day moving mean. "
               "Hover for details; scroll to zoom or drag to pan this chart only. Use the navigator above to change the analysis cohort. "
               "Partial edge weeks/months are flagged in tooltips; gaps remain unknown.")


def breakdown(explorer, dimension="agencies", height=285):
    labels = {"agencies": "Agency", "boroughs": "Borough", "complaints": "Complaint type"}
    table = explorer["summary"][dimension].head(8)
    if table.empty:
        st.info("No requests match this cohort. Change a date or category filter.")
        return
    render_chart(alt.Chart(table).mark_bar(color="#508C89", cornerRadiusEnd=4, size=21).encode(
        x=alt.X("count:Q", title=None, axis=alt.Axis(format="~s", tickCount=4)),
        y=alt.Y("category:N", title=None, sort="-x"),
        tooltip=[alt.Tooltip("category:N", title=labels[dimension]), alt.Tooltip("count:Q", title="Sample requests", format=","),
                 alt.Tooltip("share:Q", title="Cohort share", format=".1%")]), height)


def cohort_tables(explorer, key="overview"):
    with st.expander("Explore the filtered tables"):
        tabs = st.tabs(["Time series", "Agencies", "Boroughs", "Complaint types"])
        with tabs[0]:
            st.dataframe(series(explorer), hide_index=True, use_container_width=True)
        for tab, dimension in zip(tabs[1:], ["agencies", "boroughs", "complaints"]):
            with tab:
                display = explorer["summary"][dimension].rename(columns={"category": "Category", "count": "Sample requests", "share": "Share"}).copy()
                display["Share"] = display["Share"] * 100
                st.dataframe(display, hide_index=True, use_container_width=True, column_config={"Share": st.column_config.NumberColumn(format="%.1f%%")})


def km_result(explorer, grouping):
    return _km(explorer["frame"], grouping, explorer["as_of"])


def weather_result(explorer):
    return weather_selection(explorer["summary"]["daily"])


def weather_chart(result, variable):
    frame = result["joined"].copy()
    if variable not in frame:
        st.info("Downloaded weather is unavailable for this variable.")
        return
    frame["date"] = pd.to_datetime(frame["date"], utc=True)
    frame["month"] = frame["date"].dt.strftime("%B %Y")
    frame["date_label"] = frame["date"].dt.strftime("%Y-%m-%d")
    legend = alt.selection_point(name="weather_month", fields=["month"], bind="legend")
    zoom = alt.selection_interval(name="weather_zoom", bind="scales")
    names = {"temperature_2m_min": "Minimum daily temperature (°C)", "temperature_2m_max": "Maximum daily temperature (°C)", "precipitation_sum": "Daily precipitation (mm)"}
    plot = alt.Chart(frame.dropna(subset=["count", variable])).mark_circle(size=75, stroke="white", strokeWidth=.7).encode(
        x=alt.X(f"{variable}:Q", title=names[variable], scale=alt.Scale(zero=False)),
        y=alt.Y("count:Q", title="Selected sample arrivals"),
        color=alt.Color("month:N", title=None, scale=alt.Scale(range=COLORS)),
        opacity=alt.condition(legend, alt.value(.8), alt.value(.1)),
        tooltip=[alt.Tooltip("date_label:N", title="Date"), alt.Tooltip(f"{variable}:Q", title=names[variable], format=".1f"),
                 alt.Tooltip("count:Q", title="Selected sample arrivals", format=",")]).add_params(legend, zoom)
    render_chart(plot, 300)
    st.caption("Hover a point; click a month in the legend to highlight it (Shift-click for several). Scroll to zoom; drag to pan. "
               "Legend highlighting changes the view only; date and category filters determine the reported association.")
