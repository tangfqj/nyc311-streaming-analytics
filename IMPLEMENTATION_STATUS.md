# Implementation status — 2026-11-02

All M1–M5 minimum deliverables are implemented and actually exercised. This is a bounded official-data pilot and single-host Spark experiment, not a production deployment or citywide census. The [generated English report](reports/final_report.md) derives its numbers and figures from persisted artifacts.

| Gate | Actual result | Evidence |
|---|---|---|
| M1 environment, ingestion, Spark ETL | Passed; Python3.11/Java17/PostgreSQL16.4. Official raw8,822 → silver8,813 + quarantined9.90/90 daily windows. | `artifacts/metrics/{environment,source_manifest,data_quality,m1_tests}.json/xml` |
| M2 stream/recovery/idempotency | Passed; final23 M2 tests, actual PG and Spark. Duplicate/disorder/late/poison inputs, after-commit replay and same-checkpoint restart. Auxiliary watermarked windows really executed. | `artifacts/metrics/m2_tests.xml`, `streaming_metrics.json` |
| M3 KM/Cox/prediction | Passed;8,813 KM;270censored.6,003 train/2,800 test/10outside UTC split. CoxC-index0.746309 vs complete-holdout KMscore0.741663. PH diagnostic18/39 Bonferroni violations disclosed. | `artifacts/metrics/{survival,model_evaluation,prediction_example}.json`, holdout CSV |
| M4 weather/seasonality/capacity | Executed;90weatherdays,0surge threshold crossings.5capacity groups×4uplifts=20rows; all5baseline calibrations failed, so improvement deltas are NA. | `seasonality_summary.json`, `capacity_summary.json`, associated CSVs/PNGs |
| M5 benchmarks/dashboard/report | Passed execution;local scale9/9 plus real standalone Worker12/12. Four UI pages render; local HTTPhealth200. | `artifacts/benchmark/{local,workers}/{runs.csv,summary.json,audit.json}`, dashboard metrics |
| Final host suite | **95passed,0failed,0skipped**,20.81seconds;14upstream matplotlib/pyparsing deprecation warnings. | `artifacts/metrics/pytest.xml` |
| Host offline smoke | Passed, real Spark/PG,188labelled synthetic rows isolated from scientific outputs. | `artifacts/metrics/smoke.json` |
| Official predictive stream | Passed;100held-out tickets,197source-reconstructed events,14labelled synthetic updates,100new saved-model predictions; post-commit fault observed, second restart unchanged. | `artifacts/metrics/streaming_metrics.json` |
| Clean container | Build Passed;fresh Linux ARM container, no host bind mounts,Python3.11.10/Spark3.5.3,full fixture smoke exit0. Uses existing project PostgreSQL service. | `artifacts/metrics/{container_build,container_smoke,container_smoke_inner}.json` |

Actual median ETL benchmark times, seconds:

| Matrix | Configurations | Median seconds |
|---|---|---|
| local[2] data stress |1k/10k/100k records;3repetitions each|3.098/4.597/10.820|
| Native standalone process cluster |1/2/4/8 Workers;1k records;3repetitions each|8.695/10.040/9.672/22.235|

All21benchmark attempts succeeded; none were omitted. Each Worker run attests distinct registered WorkerIDs and active executor counts, one core/512MiB per executor. More Workers did not accelerate this small workload. All processes from the benchmark were cleaned up. Operating-system caches were not cleared; short-task overhead and background host load limit generalization. Experiments were run from an uncommitted working tree; input hashes and source fingerprints are retained instead of inventing a commit hash.

## Dashboard and chart correction — 2026-11-02

The Dashboard was redesigned with a consistent light canvas, dark navigation, metric cards, interactive charts and collapsible evidence. Actual browser checks cover desktop1440×1000 and mobile390×844. Prediction and database-refresh functionality remain available.

The empty “no configuration with three successful repetitions” figures were a chart-family routing bug, not missing experiments: local had9/9successful runs and workers12/12, seven configurations with3repetitions each. The fixed renderer rebuilds only2data-scale and3Worker charts, shows individual measurements and medians, and removes4inapplicable placeholder PNGs. Original benchmark CSV SHA256 values remain unchanged. Use `make bench-plots` to redraw without launching Spark. Plot manifests and `artifacts/metrics/chart_repair.json` record applicability and provenance. The original benchmark source fingerprints and clean-container smoke describe their earlier backend snapshot; presentation changes do not imply those historical images were rebuilt.

## Interactive cohorts and explanations — 2026-11-02

Overview, survival and demand/weather now share downloaded-year, inclusive NYC creation-date range/cutoff, agency, borough and complaint filters. The timeline brush changes linked calculations; detail-chart zoom/pan and legend highlights change only the visual view. Daily/weekly/monthly charts and adjustable moving means preserve unknown extraction gaps and identify partial edge buckets. Overview interpretation cards recalculate counts, supported trend comparisons and agency shares from the selected sample, with explicit statistical limits. CSV/TXT downloads retain exact filters and the fixed follow-up cutoff. Model, streaming, capacity and benchmark results keep their separately labelled saved scopes.

The 25 new pytest cases cover cohort calculations and presentation transforms. `make dashboard-check` actually passes14 control/data checks across all4pages. Real Chromium browser checks independently reconcile4cohorts: full quarter8,813; February HPD/BRONX253; pointer-brushed February2–26 cohort2,287; inclusive January15 cutoff1,902. Reset restores8,813; weekly aggregation preserves brush membership and changes the trend-support explanation. A downloaded28-day CSV sums to253 and matches the displayed interpretation. Filtered KM confidence intervals, weather-variable changes and mobile390×844 rendering were inspected. No browser exceptions were observed; transient Vega empty-extent warnings during dataset updates remain recorded.

Evidence: `artifacts/metrics/{interactive_analytics_examples,interactive_dashboard_app_test,dashboard_interaction_qa}.json`, `tests/{test_explorer,test_dashboard_ui}.py`, `dashboard/verify_interactions.py`, and actual screenshots/downloads under `output/playwright/interactive-*`. Available official years remain2025only, January–March; changing filters does not download history. Multi-year calendar behavior is tested with isolated fixtures, not presented as additional official observations.

## Not run, unsupported, or limited

- No full multi-million-row or multi-year census download/model fit; actual scale test max100,000replica rows from1,000official base rows. Scientific results use the8,813distinct official sample rows only.
- No cross-machine scale-out, cloud acceptance, production deployment, or Docker Spark Worker1/2/4/8 matrix. The required Worker matrix ran as genuine separate native Spark JVMs on this one physical machine; the clean Linux application image was built and smoke-tested separately.
- No causal staffing recommendation. All5FIFO baseline calibrations failed, and precise capacity savings are deliberately suppressed. Closure counts do not measure employee headcount or outside-cohort capacity.
- Current 2026 NYC data is not the model evaluation period. Holdout is the most recent month inside the declared2025Q1pilot. Agency/type/borough historical revisions are unavailable.90-day probabilities are null beyond58.7003days of training support; Cox PH violations and absence of probability-calibration validation remain limitations.
- No full annual seasonality claim from three months. Weather p-values are exploratory, not adjusted for time autocorrelation/multiple comparisons, and cannot establish causality.
- Actual CDC cannot be reconstructed from the official daily snapshots. Synthetic updates and delivery tests remain explicitly labelled. Watermarked Spark windows can omit very late events; PostgreSQL event-ledger recomputation provides authoritative late corrections.

No unresolved environment blocker remains for the minimum workflow. Initial sandbox DNS/socket limitations, stopped Docker, unsupported default Java, macOS make stub and local demo port/password setup were resolved; observed attempts are recorded in `artifacts/metrics/setup_attempts.json`. Future external failures should use `make smoke`; never substitute synthetic statistics for official data.

## Reproduce

On this macOS host first run `export PATH="/opt/homebrew/opt/make/libexec/gnubin:$PATH"`. Linux/WSL2 normally uses its installed GNU Make. See README for Python3.11/Java17 setup and the macOS Java helper.

```bash
make setup
make db-up
make test
make smoke
make fetch-sample
make etl
make fetch-weather
make analyze
make stream-demo
make bench
make bench-workers
make report
make dashboard
```

Optional isolated application verification:

```bash
docker compose --profile app build runner
make container-smoke
```

`make bench`/`make bench-workers` append new actual attempts. Keep per-run denominators when comparing repeated experiments. `make stream-demo` retains its ledger; replayed CREATED events are not rescored, and actual per-model prediction coverage is reported. Source configs/lock, fixtures, tests, provenance, summaries and figure outputs are reviewable without pretending the entire source dataset is checked in.
