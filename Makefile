SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
PY := .venv/bin/python
RUN = source scripts/env.sh && $(PY) -m nyc311.cli
.PHONY: setup doctor test smoke fetch-sample fetch-weather etl analyze stream-demo bench bench-workers bench-plots report dashboard dashboard-check db-up db-down
setup:
	bash scripts/setup.sh
doctor:
	$(RUN) doctor
test:
	mkdir -p artifacts/metrics
	source scripts/env.sh && $(PY) -m pytest -q --junitxml=artifacts/metrics/pytest.xml
smoke:
	$(RUN) smoke
fetch-sample:
	$(RUN) fetch-sample
fetch-weather:
	$(RUN) fetch-weather
etl:
	$(RUN) etl
analyze:
	$(RUN) analyze
stream-demo:
	$(RUN) stream-demo
bench:
	$(RUN) bench
bench-workers:
	source scripts/env.sh && bash scripts/cluster_bench.sh data/raw/nyc311_12mo_full artifacts/benchmark/workers
bench-plots:
	$(RUN) bench-plots
report:
	$(RUN) report
dashboard:
	source scripts/env.sh && .venv/bin/streamlit run dashboard/app.py --server.address 127.0.0.1
dashboard-check:
	source scripts/env.sh && $(PY) dashboard/verify_interactions.py
db-up:
	docker compose up -d --wait postgres
db-down:
	docker compose stop postgres
container-smoke:
	$(PY) scripts/verify_container.py
