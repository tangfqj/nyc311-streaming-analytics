#!/usr/bin/env bash
# Separate Spark Worker JVMs on one physical host, never local[N] "workers".
set -euo pipefail
cd "$(dirname "$0")/.."
BENCH_PYTHON="${BENCH_PYTHON:-$PWD/.venv/bin/python}"
BENCH_INPUT="${1:-data/raw}"
BENCH_OUTPUT="${2:-artifacts/benchmark}"
BENCH_WORKERS="${BENCH_WORKERS:-1 2 4 8}"
BENCH_REPETITIONS="${BENCH_REPETITIONS:-3}"
BENCH_MULTIPLIERS="${BENCH_MULTIPLIERS:-1}"
BENCH_BASE_ROWS="${BENCH_BASE_ROWS:-1000}"
BENCH_MASTER_PORT="${BENCH_MASTER_PORT:-17077}"
BENCH_UI_PORT="${BENCH_UI_PORT:-18080}"
BENCH_CLUSTER_ID="${BENCH_CLUSTER_ID:-local-jvm-single-host-1core512m}"
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_DAEMON_MEMORY=256m
export PYSPARK_PYTHON="$BENCH_PYTHON"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
if [[ ! -x "$BENCH_PYTHON" ]]; then
  echo "Missing Python environment. Run make setup first." >&2
  exit 1
fi
SPARK_DIST="$($BENCH_PYTHON -c 'import pathlib,pyspark; print(pathlib.Path(pyspark.__file__).parent)')"
if [[ -z "${JAVA_HOME:-}" && -d "$PWD/data/runtime/java17/Contents/Home" ]]; then
  export JAVA_HOME="$PWD/data/runtime/java17/Contents/Home"
fi
if [[ -z "${JAVA_HOME:-}" && -d "$PWD/data/runtime/java17" ]]; then
  export JAVA_HOME="$PWD/data/runtime/java17"
fi
BENCH_PIDS=()
cleanup() {
  for pid in "${BENCH_PIDS[@]:-}"; do
    [[ -z "$pid" ]] || kill "$pid" 2>/dev/null || true
  done
  for pid in "${BENCH_PIDS[@]:-}"; do
    [[ -z "$pid" ]] || wait "$pid" 2>/dev/null || true
  done
  BENCH_PIDS=()
}
trap cleanup EXIT INT TERM
for worker_count in $BENCH_WORKERS; do
  if [[ ! "$worker_count" =~ ^(1|2|4|8)$ ]]; then
    echo "Worker count must be one of 1,2,4,8" >&2
    exit 1
  fi
  cluster_dir="$PWD/data/benchmark/cluster/${worker_count}workers"
  log_dir="$PWD/$BENCH_OUTPUT/cluster/${worker_count}workers"
  mkdir -p "$cluster_dir" "$log_dir"
  "$SPARK_DIST/bin/spark-class" org.apache.spark.deploy.master.Master \
    --host 127.0.0.1 --port "$BENCH_MASTER_PORT" --webui-port "$BENCH_UI_PORT" \
    >"$log_dir/master.log" 2>&1 &
  BENCH_PIDS+=("$!")
  # Startup readiness is verified against the actual master's JSON endpoint.
  "$BENCH_PYTHON" - "$BENCH_UI_PORT" <<'PY'
import json,sys,time,urllib.request
url=f"http://127.0.0.1:{sys.argv[1]}/json/"
for attempt in range(120):
    try:
        with urllib.request.urlopen(url,timeout=1) as response:
            data=json.load(response)
        if data.get('status') == 'ALIVE':
            break
    except Exception:
        pass
    time.sleep(.25)
else:
    raise SystemExit('Spark master not ready; inspect cluster/master.log')
PY
  for ((worker=1; worker<=worker_count; worker++)); do
    "$SPARK_DIST/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --host 127.0.0.1 --port 0 --webui-port "$((BENCH_UI_PORT + worker))" \
      --cores 1 --memory 512m --work-dir "$cluster_dir/work$worker" \
      "spark://127.0.0.1:$BENCH_MASTER_PORT" >"$log_dir/worker$worker.log" 2>&1 &
    BENCH_PIDS+=("$!")
  done
  "$BENCH_PYTHON" -m nyc311.benchmark.run_benchmark \
    --input "$BENCH_INPUT" --output "$BENCH_OUTPUT" \
    --master "spark://127.0.0.1:$BENCH_MASTER_PORT" \
    --master-ui "http://127.0.0.1:$BENCH_UI_PORT" --workers "$worker_count" \
    --multipliers "$BENCH_MULTIPLIERS" --repetitions "$BENCH_REPETITIONS" \
    --partitions 8 --base-rows "$BENCH_BASE_ROWS" --cluster-id "$BENCH_CLUSTER_ID"
  cleanup
  sleep 1
done
