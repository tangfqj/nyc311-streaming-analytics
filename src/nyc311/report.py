"""Generate the English report directly from saved, auditable run artifacts."""
import csv
import json
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from .config import ROOT, write_json


def build_report():
    def read(name):
        path = ROOT / name
        return json.loads(path.read_text()) if path.exists() else {"status":"Not Run","artifact":name}
    def block(name):
        return f"Artifact: [{name}](../{name}).\n\n```json\n{json.dumps(read(name),indent=2,default=str)}\n```"
    def compact(name,keys):
        item = read(name)
        return {key:item.get(key) for key in keys if key in item} or item
    def json_text(value): return "```json\n" + json.dumps(value,indent=2,default=str) + "\n```"
    environment = read("artifacts/metrics/environment.json")
    dq = read("artifacts/metrics/data_quality.json")
    survival = read("artifacts/metrics/survival.json")
    model = read("artifacts/metrics/model_evaluation.json")
    stream = read("artifacts/metrics/streaming_metrics.json")
    seasons = read("artifacts/metrics/seasonality_summary.json")
    capacity = read("artifacts/metrics/capacity_summary.json")
    explorer_examples = read("artifacts/metrics/interactive_analytics_examples.json")
    explorer_checks = [{"selection": item.get("selection"),
                        "selected_rows": item.get("metrics", {}).get("selected_rows"),
                        "covered_days": item.get("metrics", {}).get("covered_days"),
                        "censored_rows": item.get("metrics", {}).get("censored_rows")}
                       for item in explorer_examples.get("examples", [])]
    if model.get("status") == "Passed":
        model_findings = f"The actual chronological pilot uses {model.get('train_rows'):,} training requests and {model.get('test_rows'):,} holdout requests. Cox Harrell C-index is {model.get('cox',{}).get('c_index'):.6f}, versus the full-holdout KM survival-score baseline {model.get('baseline',{}).get('c_index'):.6f}. The KM median baseline uses {model.get('median_baseline',{}).get('evaluated_rows')} finite-median requests and excludes {model.get('median_baseline',{}).get('unreached_median_rows')} unreached medians. Cox does not show a large advantage over the requested median baseline, and the unequal denominator prevents a direct like-for-like claim. There are {model.get('proportional_hazards',{}).get('bonferroni_violation_count')} Bonferroni-significant PH violations among {model.get('proportional_hazards',{}).get('tested_features')} tested features. Predictive discrimination does not validate the Cox PH assumption or probability calibration."
    else:
        model_findings = "Not Run: no successful model evidence available."
    weather_findings = []
    for item in seasons.get("weather_associations",[]):
        if item.get("target") == "heat_sample_count":
            weather_findings.append(f"For HEAT/HOT WATER and {item.get('weather_variable')}, the sample Spearman correlation is {item.get('spearman_rho')}; the month-controlled rank correlation is {item.get('month_controlled_rank_correlation')} (exploratory approximate p={item.get('month_controlled_p_value')}).")
    weather_findings = " ".join(weather_findings) or "Exact weather association values and statuses are retained in the summary below."
    capacity_findings = f"Of {capacity.get('groups_selected','Not Run')} selected groups, {capacity.get('groups_calibrated','Not Run')} passed calibration and {capacity.get('groups_failed_calibration','Not Run')} failed. Published nonbaseline improvement deltas: {capacity.get('published_nonbaseline_deltas','Not Run')}. Failed calibration is a negative result, and this execution supports no numerical staffing benefit claim."

    manifest_path = ROOT/"data/raw/nyc311_2025q1_suffix17/manifest.json"
    if manifest_path.exists():
        write_json(ROOT/"artifacts/metrics/source_manifest.json",json.loads(manifest_path.read_text()))
    test_path = ROOT/"artifacts/metrics/pytest.xml"
    test_counts = {"status":"Not Run"}
    if test_path.exists():
        root = ET.parse(test_path).getroot()
        suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
        test_counts = {key:sum(float(s.get(key,0)) for s in suites) for key in ("tests","failures","errors","skipped","time")}
        test_counts["status"] = "Passed" if not test_counts["failures"] and not test_counts["errors"] else "Failed"
    write_json(ROOT/"artifacts/metrics/test_summary.json",test_counts)
    bench = []
    bench_tables = []
    bench_figure_paths = []
    for path in sorted((ROOT/"artifacts/benchmark").rglob("runs.csv")):
        with path.open() as f: rows = list(csv.DictReader(f))
        by_status = {}
        for row in rows: by_status[row["run_status"]] = by_status.get(row["run_status"],0)+1
        bench.append({"path":str(path.relative_to(ROOT)),"attempts":len(rows),"status_counts":by_status})
        manifest_path = path.parent / "plot_manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            for chart in manifest.get("charts", []):
                if chart.get("status") == "Generated":
                    figure = Path(chart["path"])
                    figure = figure if figure.is_absolute() else ROOT / figure
                    if figure.exists():
                        bench_figure_paths.append(figure)
        else:
            families = {row.get("experiment") for row in rows}
            names = []
            if "worker_scaling" in families:
                names.extend(["runtime_vs_workers.png", "throughput_vs_workers.png", "speedup_efficiency.png"])
            if "local_data_scale" in families:
                names.extend(["runtime_vs_data_size.png", "throughput_vs_data_size.png"])
            bench_figure_paths.extend(path.parent / name for name in names if (path.parent / name).exists())
        summary_path = path.parent/"summary.csv"
        if summary_path.exists():
            summary_rows = list(csv.DictReader(summary_path.open()))
            table = [f"Measured summary: [{summary_path.relative_to(ROOT)}](../{summary_path.relative_to(ROOT)})", "", "| Mode | Workers | Multiplier | Rows | Successful/attempted | Median ETL seconds | Rows/s | Speedup |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
            for r in summary_rows:
                def number(key):
                    return f"{float(r[key]):.3f}" if r.get(key) else "NA"
                table.append(f"| {r.get('experiment')} | {r.get('worker_count') or 'local[2]'} | {r.get('data_multiplier')} | {r.get('input_rows')} | {r.get('successful_runs')}/{r.get('attempted_runs')} | {number('median_elapsed_seconds')} | {number('median_throughput_rows_per_sec')} | {number('speedup')} |")
            bench_tables.append("\n".join(table))
    figures = []
    for path in sorted((ROOT/"artifacts/figures").glob("*.png")):
        figures.append(f"![{path.stem}](../{path.relative_to(ROOT)})")
    bench_figures = []
    for path in sorted(set(bench_figure_paths)):
        bench_figures.append(f"![{path.stem}](../{path.relative_to(ROOT)})")
    report = f'''# NYC 311 Streaming Analytics, Resolution-Time Prediction & Capacity Planning

Generated from persisted artifacts at {datetime.now(timezone.utc).isoformat()}. This report records completed work and retains unsuccessful and unavailable configurations. Engineering fixtures are excluded from all scientific results below.

## 1. Abstract and business question

This reproducible course prototype reconstructs creation and closure events from official NYC311 snapshots, maintains an idempotent PostgreSQL ticket ledger through Spark Structured Streaming, and estimates time to closure with right-censored survival analysis. It examines demand patterns and explicit capacity scenarios. The pilot is a systematic request-ID suffix sample, not a census and not an official real-time CDC feed. Results describe the observed sample only; they do not establish citywide staffing effects or production-scale performance.

## 2. Dataset, provenance, schema and limitations

Source: [NYC Open Data erm2-nwe9](https://data.cityofnewyork.us/d/erm2-nwe9). NYC's [schema update](https://www.nyc.gov/opendata/news/all-news/311-Service-Requests-Updates) confirms the UI label “Problem” retains the API field `complaint_type`. Snapshots are updated daily. The downloader requests only selected non-address fields, uses optional `X-App-Token`, bounded retries, stable ordering, daily windows and SHA256 manifests. This execution covers January–March 2025 with IDs ending in `17`, approximately 1% by suffix rule; ID allocation can induce bias, and no inverse-probability population claims are made. A row cap is a safety limit: incomplete windows must be flagged, never counted as complete daily observations.

The exact request parameters, checksums, covered windows, row counts and download times are in [source_manifest.json](../artifacts/metrics/source_manifest.json). Downloaded snapshots can later change. The checked-in official fixture has its own provenance; synthetic engineering fixtures and synthetic scale replicas are separate. Final snapshot agency/type/borough may reflect revisions after creation; historical field revisions cannot be recovered, so these features approximate creation-time availability.

## 3. System architecture and streaming semantics

`Socrata JSONL → Spark validation/deduplication → partitioned silver Parquet → event reconstruction → atomic file producer → Spark foreachBatch → PostgreSQL transaction → stored predictions/report/dashboard`.

Only observed creation and closure timestamps reconstruct historical events. Injected duplicates, disorder, late delivery and updates carry explicit flags. A directory source and Spark checkpoint provide at-least-once delivery. The sink uses unique event IDs, monotone request versions, atomic prediction writes and `(pipeline_id,batch_id)` commits in one database transaction. Business idempotency survives a committed database batch followed by failure before checkpoint acknowledgement. The test demo intentionally triggers that failure and restarts twice; an empty restart must leave counts unchanged.

Late valid events always update authoritative ticket state. Exact one-hour/day ledger summaries are recomputed against an explicit event-time `as_of`; persistent ledger storage grows. This path does **not** claim bounded-state Spark watermark aggregates: a separate actual two-stage watermarked Spark query deduplicates event IDs and appends finalized hourly aggregate Parquet. It may evict/drop late events and is not authoritative for late corrections; both stages have independent checkpoints and persist real state/drop progress. Synthetic fixture and official replay use separate PostgreSQL schemas.

Streaming evidence: [streaming_metrics.json](../artifacts/metrics/streaming_metrics.json).

{json_text(compact("artifacts/metrics/streaming_metrics.json",["run_status","run_id","database_schema","source_kind","snapshot_tickets","reconstructed_events","fault_after_commit_observed","second_restart_unchanged","prediction_status","prediction_summary","min_created_at","selected_created_min","selected_created_max","elapsed_seconds"]))}

## 4. Data quality and preparation

Floating source timestamps are interpreted in America/New_York and normalized to UTC. Ambiguous autumn times use the earlier offset; nonexistent spring times are quarantined rather than silently moved. Invalid keys/dates/negative durations are quarantined. Latest observed snapshot wins, with a deterministic content-hash tie-break. Accepted rows are distinct valid keys: `input = accepted + rejected + deduplicated`.

[Data quality evidence](../artifacts/metrics/data_quality.json):

{json_text(dq)}

## 5. Survival analysis and right censoring

At fixed `as_of=2025-07-01T00:00:00Z`, eligible requests are observed closed only if `created ≤ closed ≤ as_of`; other eligible requests are right-censored at `as_of`. Durations equal zero are floored to one minute for model fitting and counted. Invalid negative closure intervals do not enter fitting. Final status does not override closure labels. Kaplan–Meier curves include confidence intervals, group sample counts and censoring fractions; a median beyond support is “Not reached,” not a fabricated number.

See [survival_summary.csv](../artifacts/metrics/survival_summary.csv) and [survival.json](../artifacts/metrics/survival.json).

{json_text(survival)}

## 6. Prediction at creation and temporal validation

Training requests were created before March 1, 2025 and training closure labels were truncated at March 1. Holdout requests were created from March 1 through March 31 with follow-up through July 1. This is the latest creation interval within this bounded Q1 pilot, **not** the latest available 2026 NYC data. All split boundaries are UTC; ten March31 evening NYC requests fall beyond the April1 UTC cutoff and are excluded from modeling while retained in overall KM. Exact excluded-row accounting is persisted. There is no random cross-time split. Category vocabularies, reference levels, and Cox parameters are fit on training data only. Features are agency, complaint type, borough, and creation calendar cycles. Closed timestamp, final status and future weather are excluded. The penalized Cox model is compared with an agency×complaint → complaint → global KM baseline using censor-aware Harrell concordance. The requested grouped-KM median baseline is separately scored only where its median is reached; its denominator is retained and differs from the full-cohort survival-score baseline. Proportional-hazard diagnostics are reported even when they indicate violations; PH test execution passing does not mean the assumption holds.

[Model evaluation](../artifacts/metrics/model_evaluation.json):

{json_text(model)}

The saved model and encoder share a version hash. Predictions include median survival and supported 7/30/90-day closure probabilities; unsupported horizons are null. See [prediction_example.json](../artifacts/metrics/prediction_example.json). Streaming inference is bounded per microbatch and uses the saved model rather than refitting it.

{model_findings}

## 7. Seasonality and weather associations

Daily counts refer to the complete suffix-selected sample for each covered day. Missing/truncated date windows are excluded rather than filled with false zeros. Surge reference levels use prior observations only and compare weekday patterns. This three-month pilot cannot establish full-year seasonality. Weather uses [Open-Meteo historical reanalysis](https://open-meteo.com/en/docs/historical-weather-api) at one NYC point as a citywide approximation. Temperature/precipitation relationships are associations, with within-month comparisons where estimable; they are not weather causal effects. Reported p-values are exploratory, assume independent days, and are not adjusted for serial autocorrelation or multiple weather comparisons. Three month fixed effects cannot eliminate all seasonal confounding.

{weather_findings}

{block("artifacts/metrics/seasonality_summary.json")}

Daily and surge records: [daily_volume.csv](../artifacts/metrics/daily_volume.csv), [surge_events.csv](../artifacts/metrics/surge_events.csv), [weather_joined.csv](../artifacts/metrics/weather_joined.csv).

## 8. Capacity scenarios and assumptions

FIFO is a simplified sampled-cohort queue. Daily observed cohort closures proxy service capacity; they are not staffing levels or full agency capacity. Prior-period citywide backlog and heterogeneous service requirements are unavailable. The baseline simulated median is distinct from observed Kaplan–Meier median. Basic calibration compares them before publication of precise deltas. Failed or unidentifiable calibration suppresses precise savings and retains scenario sensitivity evidence only. Capacity uplifts of 10%, 20%, and 30% are assumption-bound scenarios, not effects of hiring people. Ranking identifies groups to investigate and is not agency performance ranking.

{capacity_findings}

{block("artifacts/metrics/capacity_summary.json")}

[capacity_scenarios.csv](../artifacts/metrics/capacity_scenarios.csv) preserves group counts, cohort backlog, observed KM, simulated baseline, calibration and assumptions.

## 9. Scale-out and data-size experiments

All experiments execute the same Spark cleaning, deduplication, Parquet write and aggregates, with raw per-run timing and status. Local thread mode is separately labelled and never called a worker cluster. A standalone worker run requires actual registered worker processes and executor attestation. Worker configurations use one offered CPU core and 512 MiB executor memory per worker, fixed eight shuffle partitions and three repetitions when successful. Total resources grow with worker count. Every worker is on this physical machine; these results do not demonstrate cross-machine scaling. Data stress uses a bounded deterministic official base sample with unique replica IDs, labelled synthetic workload metadata. Replicas never enter statistical analyses. Runtime includes the workload; Spark application launch and a JVM-only range-query prewarm are separately timed. Operating-system file caches were not cleared, run order is fixed, and short workloads are sensitive to startup/cache/noise. Throughput is ETL rows per second, not end-to-end producer/database throughput. Failed and Not Run attempts remain in the denominator; complete three-repetition points alone enter the main comparison curves.

{chr(10).join(bench_tables)}

{json_text(bench)}

Each benchmark directory contains `runs.csv`, `summary.csv`, `summary.json`, per-run specs, cluster attestations and logs. Speedup/efficiency only compare the same workload. No curve is extrapolated to missing configurations.

Figure applicability is recorded in each directory's `plot_manifest.json`. Data-size charts use the local data-scale experiment; Worker charts use the standalone Worker experiment. Earlier rendering mistakenly emitted both families in each directory, creating misleading empty “three successful repetitions” placeholders despite complete measurements. The corrected renderer uses the existing actual run records, shows individual repetitions and medians, and omits inapplicable figures. `make bench-plots` regenerates these charts without rerunning Spark measurements.

{chr(10).join(bench_figures) or 'Not Run: no measured benchmark figures available.'}

### Interactive exploration and interpretation

Overview, survival and demand/weather views share creation-year, inclusive local-date range/cutoff, agency, borough and complaint filters. Daily/weekly/monthly aggregation and moving averages support different questions. A timeline brush changes the analysis cohort; detail-chart pan/zoom and legend highlights change only the visual presentation. Linked counts, composition tables, KM estimates and explanatory cards are recomputed from the persisted official silver data. Exported series and interpretation text include the selected dates, category filters and fixed follow-up cutoff. Only downloaded years are selectable; the current pilot remains January–March 2025.

Dynamic interpretations describe sample arrivals and agency concentration. Previous-period changes require two fully covered, equal-length local calendars; low-count or zero baselines suppress percentage claims. Within-window comparisons use equal calendar blocks and are explicitly labelled. Unknown extraction days remain gaps, partial calendar buckets are identified, and annual/causal conclusions are not inferred. Interactive weather associations are unadjusted exploratory Spearman correlations for the selected cohort; they do not replace the saved heat-specific month-controlled analysis in section 7.

Saved Cox training/holdout metrics, the prediction model, streaming evidence, capacity calibration and benchmark records keep their own labelled scopes. Date/category filters do not retrain the model or recalibrate capacity scenarios. Backend examples from the real silver artifact are in [interactive_analytics_examples.json](../artifacts/metrics/interactive_analytics_examples.json):

{json_text(explorer_checks) if explorer_checks else 'Not Run: no interactive cohort examples available.'}

Control checks: [interactive_dashboard_app_test.json](../artifacts/metrics/interactive_dashboard_app_test.json), reproducible with `source scripts/env.sh && .venv/bin/python dashboard/verify_interactions.py`. Actual browser pointer/brush, export and responsive-layout evidence: [dashboard_interaction_qa.json](../artifacts/metrics/dashboard_interaction_qa.json). AppTest and browser checks are separate evidence; neither implies an unexecuted deployment.

## 10. Failures, limitations, ethics and reproducibility

Environment evidence: [environment.json](../artifacts/metrics/environment.json), [postgres_environment.json](../artifacts/metrics/postgres_environment.json). Initial sandbox DNS/socket restrictions and the original unsupported Java 18 were resolved using authorized host access and a project-local fixed Java 17 runtime. Docker startup initially used an older local demo port/password during concurrent setup; the final service uses the documented port 55432. Raw attempt logs remain local in `artifacts/logs`; benchmark failure records remain in tracked CSV/JSON. Remaining blockers and exact checks are in [IMPLEMENTATION_STATUS.md](../IMPLEMENTATION_STATUS.md).

{json_text({k:environment.get(k) for k in ["python","platform","machine","logical_cpu_count","ram_bytes","disk_free_bytes","java","docker","network"]})}

Pinned Python dependencies are in `requirements.lock`; source dates, seeds and thresholds are in `config/default.yaml`. Run `make setup`, `make db-up`, `make test`, `make smoke`, `make fetch-sample`, `make etl`, `make analyze`, `make stream-demo`, `make bench`, `make bench-workers`, `make report`. `make fetch-weather` is optional before `make analyze`; unavailable weather is explicitly recorded. See [README](../README.md) and [demo_script](demo_script.md). This existing environment has been exercised; a separate clean machine/container rebuild is reported only if actually executed. Do not publish personal address fields; output uses selected categories and borough-level aggregates. Model scores reflect observational administrative closure, not service quality or fairness.

Clean build evidence: [container_build.json](../artifacts/metrics/container_build.json). Fresh Linux container smoke, without host bind mounts: [container_smoke.json](../artifacts/metrics/container_smoke.json). The separate container uses the same project PostgreSQL service; it is not a second physical machine or the Docker Spark worker matrix.

Final pytest evidence [pytest.xml](../artifacts/metrics/pytest.xml):

{json_text(test_counts)}

Offline engineering evidence: [smoke.json](../artifacts/metrics/smoke.json). Synthetic smoke scores are not research outcomes.

## 11. Figures and conclusion

{chr(10).join(figures) or 'Not Run: no scientific figures available.'}

The auditable outputs above establish only what was run. Sample and single-host limitations constrain statistical and scaling conclusions. Capacity scenarios require operational validation before any resource decision.

## 12. References

1. [NYC Open Data, 311 Service Requests from 2020 to Present](https://data.cityofnewyork.us/d/erm2-nwe9).
2. [NYC Open Data, 311 Service Requests Updates](https://www.nyc.gov/opendata/news/all-news/311-Service-Requests-Updates).
3. [Socrata application tokens](https://dev.socrata.com/docs/app-tokens).
4. [Apache Spark 3.5 Structured Streaming documentation](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html).
5. [lifelines KaplanMeierFitter](https://lifelines.readthedocs.io/en/latest/fitters/univariate/KaplanMeierFitter.html).
6. [lifelines CoxPHFitter](https://lifelines.readthedocs.io/en/latest/fitters/regression/CoxPHFitter.html).
7. [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api).
'''
    path = ROOT/"reports/final_report.md"
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(report)
    return {"status":"Passed","report":str(path),"test_summary":test_counts,"benchmarks":bench}
