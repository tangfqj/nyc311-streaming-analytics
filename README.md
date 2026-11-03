# NYC 311 Streaming Analytics, Resolution-Time Prediction & Capacity Planning

A reproducible Python 3.11 / Spark 3.5.3 course project: official snapshot ingestion, partitioned Parquet, historical event replay, transactional PostgreSQL state, right-censored KM/Cox survival, weather associations, calibrated capacity scenarios, and measured Spark benchmarks.

**Start with [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) and the generated [English report](reports/final_report.md).** All numeric claims come from persisted artifacts. NYC311 publishes daily snapshots; this project is historical event reconstruction, not an official live CDC feed. Scientific analyses exclude engineering fixtures and benchmark replicas.

On macOS without Command Line Tools, use GNU Make: `export PATH="/opt/homebrew/opt/make/libexec/gnubin:$PATH"` after `brew install make`.

## Requirements and setup

- Python **3.11**, Java **17**, ~3 GB disk for dependencies/runtime; additional data and benchmark space varies. Tested environment evidence: [environment.json](artifacts/metrics/environment.json).
- Docker Desktop / Engine with Compose v2 for PostgreSQL, or an existing PostgreSQL 16 database. Spark local mode does not need Docker. `psql` is optional.
- macOS ARM, Linux and Windows WSL2 can use the shell Makefile. Native Windows users should use WSL2 or the supplied container build. Linux/WSL2 install OpenJDK17 and set `JAVA_HOME`; no global machine configuration is modified by project scripts.
- Internet for package installation and official sample/weather downloads. `SOCRATA_APP_TOKEN` is optional for a bounded, throttled sample. No token is checked in.

```bash
make setup
# macOS ARM optional Java17 helper, if no supported runtime already installed:
bash scripts/install_java17.sh
make doctor
make db-up
make test
make smoke
```

`make setup` creates `.venv` with `python3.11`, installs the complete fixed dependency lock and the local package, then writes an environment check. If `uv` is installed it is used, otherwise pip. `PYTHON_BIN=/path/to/python3.11 make setup` overrides discovery. `make doctor` records failures without claiming missing services work. Source `scripts/env.sh` before direct Python commands to use the project-local Java17, UTC Python process timezone and localhost Spark binding.

The PostgreSQL demo is bound to **127.0.0.1:55432**, database/user `nyc311`, local demo password `nyc311_local`. Override `DATABASE_URL` in your shell for your own database; Docker Compose variables `POSTGRES_PASSWORD`/`POSTGRES_PORT` must match it. `.env.example` contains demo values only. Direct Python commands do not automatically load a `.env` file. Use `set -a; source .env; set +a` for an explicitly trusted local file. PostgreSQL volumes preserve initial credentials; changing Compose environment does not change an already initialized database password.

`make smoke` is offline and uses **188 labelled synthetic fixture rows**, real Spark, real PostgreSQL, a prediction-format callback, injected duplicate/disordered/late/invalid events, and failure after database commit followed by same-checkpoint restart. It writes `data/smoke/` and [smoke.json](artifacts/metrics/smoke.json); it does not publish synthetic research scores. Without PostgreSQL, non-PG tests remain available, and smoke records a concrete failure with the `make db-up` recovery step. There is also a small official fixture with source/checksum provenance in `tests/fixtures/`.

## Run the bounded real workflow

```bash
make fetch-sample       # 2025-11-01 <= created < 2026-11-01, full population
make etl                # data/raw/nyc311_12mo_full -> data/silver
make fetch-weather      # optional actual Open-Meteo ERA5 Nov 2025–Nov 2026
make analyze            # KM, temporal Cox/KM baseline, seasonality, capacity
make stream-demo        # saved Cox prediction on actual reconstructed events
make bench              # local[2] data scale 1x/10x/100x, 3 repetitions each
make bench-workers      # actual standalone JVM workers1/2/4/8, 3 repetitions
make bench-plots        # redraw applicable charts from saved benchmark CSVs; no Spark rerun
make report
make dashboard          # http://localhost:8501; Ctrl-C stops the UI
```

This is a **systematic suffix sample**, approximately 1% of IDs, not citywide totals or a probability-validated random sample. Every daily window is fully paginated within that rule; the 50,000-row cap is only a guard. Manifests retain full/partial/failed coverage, SHA256 and download times. `make fetch-sample` resumes matching successful windows after validating checksums. To use another date range or sample rule, choose a new output directory; incompatible query configs are rejected rather than mixed. SODA API snapshots can change between downloads. No precise incident address is requested.

```bash
source scripts/env.sh
.venv/bin/python -m nyc311.cli fetch-sample --start 2025-01-01 --end 2025-01-08 \
  --output data/raw/another-window --sample-suffix 17 --max-rows 5000
.venv/bin/python -m nyc311.cli etl --input data/raw/another-window \
  --output data/silver-small --metrics artifacts/metrics/small_quality.json
.venv/bin/python -m nyc311.cli analyze --part model
.venv/bin/python -m nyc311.cli predict --agency HPD --complaint-type 'HEAT/HOT WATER' \
  --borough BRONX --created-at '2025-03-15T12:00:00-04:00'
```

Adjust `config/default.yaml` for a scientifically valid new train/test calendar and coverage manifest when changing input. Default training requests precede March1, with training labels truncated at March1; March holdout labels use fixed July1 follow-up. This is the most recent holdout **inside the pilot**, not the latest available NYC snapshot period. Final snapshot agency/type/borough may have been revised after request creation, a documented source limitation. Only creation categories/calendar features enter the Cox encoder; status, closure time and future weather never do. Predictions beyond observed follow-up are null, and KM medians that are not reached are explicitly marked.

## Streaming and failure semantics

`stream-demo` publishes bounded atomic JSON files, intentionally commits one DB batch and raises before checkpoint completion, then restarts the same checkpoint twice. Source flags identify synthetic updates, duplicate injection and delivery disorder/late injection. Only known creation and closure timestamps reconstruct source history. `processed_events`, monotone `ticket_current`, `ticket_predictions` and `batch_commits` share one PostgreSQL transaction. Per-microbatch collection has a strict cap. Pipeline/checkpoint identity registration prevents a reused pipeline ID from skipping a new checkpoint's batch0. Predictions on previously deduplicated events are not retriggered; a new prediction model should use an explicit backfill workflow rather than pretending replay is a new creation.

`demo_fixture` and `demo_official` are separate database schemas. Late valid events are retained in the ledger and correct authoritative current state and one-hour/day event-time windows at explicit `as_of`. The authoritative implementation uses exact PostgreSQL ledger recomputation; storage grows with event count. A separate two-stage Spark window stream deduplicates within a two-day watermark and appends finalized hourly count Parquet with independent checkpoints. Very late input can be dropped by that auxiliary path, and the newest unfinalized windows are not in append output; stage progress/drop metrics are saved. The PostgreSQL ledger still corrects those late events. Timestamp-less or conflicting events enter quarantine.

For an interactive producer/consumer demo use two terminals:

```bash
# Terminal1: start an actual long-running Structured Streaming query
source scripts/env.sh
.venv/bin/python -m nyc311.cli consume --continuous --incoming data/live/incoming \
  --checkpoint data/live/checkpoint --pipeline-id class-demo-v1
# Terminal2: files arrive while the consumer remains active
source scripts/env.sh
.venv/bin/python -m nyc311.cli produce --silver data/silver --incoming data/live/incoming \
  --max-events 300 --batch-size 25 --speed 20 --duplicate-fraction 0.2 --disorder-fraction 0.5
```

Restart the same consumer command/checkpoint. If you intentionally create a different checkpoint, choose a new pipeline ID. `--speed` is the producer event emission rate, not consumer throughput. The dashboard refresh reads actual persisted data; absent artifacts show “Not run / No data available.” The default live consumer uses the database's configured schema; demo schema snapshots are also persisted in `data/stream-demo/database_snapshot.json`.

## Benchmarks

The Dashboard separates **Data scale** and **Worker scale** so each view uses the appropriate experiment records. Runtime/throughput versus data size come from `artifacts/benchmark/local`; runtime/throughput versus Workers and speedup/efficiency come from `artifacts/benchmark/workers`. The chart manifests distinguish `Generated`, `NotApplicable`, and `NotRun`. A missing experiment family is not a failed benchmark.

The earlier “no configuration with 3 successful repetitions” images were caused by rendering both chart families in both directories. All 21 measured attempts were already successful. The fixed renderer removes those inapplicable placeholder images and plots the actual repetitions plus their medians. Run `make bench-plots` to reproduce the corrected figures without repeating the measurements.

`make bench` uses **1,000 actual base rows**, separately copied with unique replica IDs at1x/10x/100x, and runs the same complete Spark ETL logic three times each on `local[2]`. This measures a 1k/10k/100k workload, not millions of original observations. Every copy is isolated under `data/benchmark`; source-kind metadata prevents statistical ingestion of replicas. `--base-rows`/`--multipliers`/`--repetitions` adjust scale.

`make bench-workers` uses separate native Spark Standalone Master/Worker/executor JVM processes, one core and512MiB per worker. Workers must be registered and running executors must be attested through Spark REST. The physical machine is shared, total offered resources increase, and results are **single-host process-cluster** experiments. Fixed eight shuffle partitions; end-to-end ETL timing and separate subprocess time are recorded. Not `local[8]` renamed as8workers.

```bash
source scripts/env.sh
BENCH_BASE_ROWS=1000 BENCH_REPETITIONS=3 bash scripts/cluster_bench.sh \
  data/raw/nyc311_12mo_full artifacts/benchmark/workers
```

The script cleans up its own JVMs on exit. `BENCH_WORKERS='1 2'` limits configurations; changing configuration yields new explicit records. Full run details and failed denominators remain in `runs.csv` and per-run JSON. Only comparable complete repetitions enter scaling summaries. Review available memory/disk before increasing sample size. Invalid/OOM/unavailable configurations are Failed/Not Run with actual reasons, never estimated runtimes. The benchmark does not measure database streaming throughput.

Optional container Spark topology (not required for native benchmark):

```bash
docker compose --profile spark up -d --scale spark-worker=2
# Stop only this project's Spark services when done:
docker compose --profile spark stop spark-worker spark-master
# Optional clean application image build/run:
docker compose --profile app build runner
docker compose --profile app run --rm runner
```

A fresh Linux application image was actually built and smoke-tested without host bind mounts; evidence is in `artifacts/metrics/container_smoke.json`. Reproduce with `docker compose --profile app build runner` then `make container-smoke`. This is distinct from the native Worker benchmark; the Docker Spark Worker matrix was not executed. Docker worker counts on one host do not establish cross-machine scale-out.

## Evidence and modules

The four Dashboard pages use a consistent theme, summary cards, interactive charts and compact comparison tables. Raw JSON and run records remain available in expandable evidence sections. Capacity calibration failures and unsupported prediction horizons stay visible. Start `make dashboard` and open `http://localhost:8501`; presentation changes do not alter the underlying sample, model scores, benchmark measurements, or scenario results.

### Explore dates and interpret the charts

The **Overview**, **Survival & Prediction** and **Seasonality & Capacity** pages calculate linked cohorts from the official silver Parquet. The current download covers **2025-11-01 through 2026-10-31**; the year selector offers downloaded years only. Selecting a date does not fetch additional history. Calendar filters use **America/New_York** creation dates with an inclusive end date. Follow-up remains fixed at **2026-11-03T00:00:00Z**, including after filtering.

1. Choose an available year, a date range or **On or before date**, and daily/weekly/monthly aggregation. Adjust the moving-average window for the detail chart.
2. Expand **Refine the cohort** to select agencies, boroughs and complaint types. Uncheck the corresponding **All** option first. An empty selection intentionally produces zero requests.
3. Drag over the **Timeline navigator** to focus a smaller creation window. Request counts, resolution/censoring counts, detail charts, composition tables, KM curves and explanatory text recalculate for the selected cohort. **Reset chart focus** or double-click the navigator to restore the date controls' window.
4. Hover charts for values; scroll to zoom and drag to pan the detail charts. These visual zooms, and KM/weather legend highlights, do not change the analysis cohort. Use the date/category controls or navigator to change calculations.
5. Read **What this view tells us**, then download the matching filtered CSV and interpretation TXT. The text reports the selected count, supported changes within the window, agency concentration and limitations. Previous-period comparisons require both equal-length local calendars to be fully covered; sparse/zero baselines suppress percentage claims. Unknown extraction days remain gaps rather than becoming zero arrivals. Weekly/monthly edge buckets are labelled partial, with mean daily arrivals used for comparison.

For example, February with agency **HPD** and borough **BRONX** contains **253** sampled requests in the current silver artifact, compared with **8,813** for the full quarter. These are downloaded sample counts, not citywide demand. The dynamic weather view recomputes **unadjusted exploratory Spearman associations** for the selected requests; the saved scientific analysis separately retains its original heat-specific, month-controlled results. Neither establishes weather causality or annual seasonality.

The saved Cox holdout scores, prediction model, streaming demonstration, capacity scenarios and benchmark measurements retain their own explicit cohorts. They are labelled separately and are not retrained or replaced when filters change. Backend examples are recorded in [interactive_analytics_examples.json](artifacts/metrics/interactive_analytics_examples.json); browser interaction evidence is in [dashboard_interaction_qa.json](artifacts/metrics/dashboard_interaction_qa.json).

Run `make dashboard-check` to reproduce the 14 control/data assertions against the downloaded sample. This uses Streamlit AppTest; pointer dragging and responsive rendering are checked separately in the browser evidence above. `make test` also covers time zones, inclusive cutoff dates, multi-year calendar fixtures, zero/unknown days, comparison support, filtered censoring/KM, weather gaps and export context.

- Ingestion: `src/nyc311/ingest`, resumable raw manifests and weather provenance.
- Spark ETL: `src/nyc311/etl`, row reconciliation, UTC/DST policy, quarantine and partitioned silver.
- Streaming: `src/nyc311/streaming`, atomic replay producer, transactional sink, checkpoint identity and restart tests.
- Survival/prediction: `src/nyc311/analysis/{survival,train_cox,predict}.py`, training encoder/model/metadata, reproducible holdout predictions.
- Weather/capacity: `src/nyc311/analysis/{seasonality,capacity}.py`, complete-sample calendar and calibration-gated cohort FIFO.
- Benchmark: `src/nyc311/benchmark`, actual per-run manifests, resources, timing and worker/executor attestations.
- Presentation: `dashboard/{app,explorer_ui}.py`, pure cohort calculations in `src/nyc311/analysis/explorer.py`, artifact-driven `src/nyc311/report.py`, [methodology](reports/methodology.md), [demo script](reports/demo_script.md).

`data/`, `.env`, virtual environments, model pickle files, raw event logs and checkpoints are ignored. Small official fixtures, metric CSV/JSON, tests and figures are retained for review. Models are retrained reproducibly from source; load only locally trusted model pickle files. All counts are cohort/sample counts; closure is an administrative outcome, not a service-quality judgment. Capacity uplift is a conditional scenario and cannot be translated into an employee headcount or a causal recommendation.

macOS without Apple Command Line Tools: `/usr/bin/make` may be only an Xcode installer stub. Install GNU Make (`brew install make`) and run `export PATH="/opt/homebrew/opt/make/libexec/gnubin:$PATH"` before the commands above, or use `gmake` in place of `make`. The current host uses this GNU Make path. No Xcode installation is required for this pure-Python/JVM workflow.
