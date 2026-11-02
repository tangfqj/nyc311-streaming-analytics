"""Measured batch ETL experiments with explicit cluster/process attestation."""
import argparse
import csv
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from urllib.request import urlopen

from nyc311.benchmark.expand_data import expand_data
from nyc311.config import ROOT, write_json

FIELDS = [
    "run_id", "timestamp", "commit_hash", "experiment", "master", "worker_count",
    "registered_workers", "active_executors", "executor_cores", "executor_memory",
    "offered_cores", "host_cpu_count", "host_ram_bytes", "host_cpu_percent_mean",
    "host_used_ram_peak_bytes", "driver_tree_rss_peak_bytes", "resource_samples",
    "disk_free_before_bytes", "disk_free_after_bytes", "spark_version", "python_version",
    "platform", "partitions", "source_sha256", "input_sha256", "input_rows",
    "processed_rows", "data_multiplier", "synthetic", "seed", "repetition",
    "spark_startup_seconds", "elapsed_seconds", "throughput_rows_per_sec",
    "run_status", "reason", "metrics_path", "attestation_path", "single_physical_host",
    "workload", "cluster_id", "driver_memory", "base_rows", "base_sample_method", "base_source_kinds",
]
WORKLOAD = "build_silver_v1_clean_deduplicate_partitioned_parquet_group_aggregates"


def _get_json(url):
    with urlopen(url, timeout=3) as response:
        return json.load(response)


def attest_cluster(spark, worker_count, master_ui=None, timeout=45):
    """Only declare worker scaling when separate active executors are observed."""
    if not spark.sparkContext.master.startswith("spark://"):
        return {"mode": "local_threads", "worker_count": None, "registered_workers": None,
                "active_executors": None, "note": "local[N] threads are not standalone workers"}
    if not worker_count or not master_ui:
        raise ValueError("Standalone runs require worker_count and master_ui for verification")
    deadline = time.monotonic() + timeout
    evidence = {}
    last_error = "No active executors observed"
    while time.monotonic() < deadline:
        try:
            master = _get_json(master_ui.rstrip("/") + "/json/")
            workers = [w for w in master.get("workers", []) if w.get("state") == "ALIVE"]
            app_id = spark.sparkContext.applicationId
            app_ui = spark.sparkContext.uiWebUrl
            executors = _get_json(f"{app_ui}/api/v1/applications/{app_id}/executors")
            active = [e for e in executors if e.get("id") != "driver" and e.get("isActive")]
            evidence = {"mode": "standalone", "requested_workers": worker_count,
                        "registered_workers": len(workers), "active_executors": len(active),
                        "workers": workers, "executors": executors,
                        "master_ui": master_ui, "application_id": app_id, "application_ui": app_ui,
                        "observed_at": datetime.now(timezone.utc).isoformat()}
            resource_match = (all(int(w.get("cores", 0)) == 1 for w in workers)
                              and all(int(e.get("totalCores", 0)) == 1 for e in active))
            if len(workers) == worker_count and len(active) == worker_count and resource_match:
                return evidence
            last_error = f"Expected {worker_count} workers/executors; saw {len(workers)}/{len(active)}"
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(0.5)
    raise RuntimeError(f"Cluster attestation failed: {last_error}; evidence={evidence}")


class ResourceSampler:
    """Host utilization and driver process tree RSS, not invented per-worker RSS."""
    def __enter__(self):
        import psutil
        self.psutil = psutil
        self.stop_event = threading.Event()
        self.cpu, self.ram, self.rss = [], [], []
        self.process = psutil.Process()
        psutil.cpu_percent()
        self.thread = threading.Thread(target=self._sample, daemon=True)
        self.thread.start()
        return self

    def _sample(self):
        while not self.stop_event.wait(0.2):
            self.cpu.append(self.psutil.cpu_percent())
            self.ram.append(self.psutil.virtual_memory().used)
            total = 0
            for proc in [self.process] + self.process.children(recursive=True):
                try:
                    total += proc.memory_info().rss
                except self.psutil.Error:
                    pass
            self.rss.append(total)

    def __exit__(self, *_):
        self.stop_event.set()
        self.thread.join(timeout=2)

    def metrics(self):
        return {"host_cpu_percent_mean": statistics.mean(self.cpu) if self.cpu else None,
                "host_used_ram_peak_bytes": max(self.ram, default=None),
                "driver_tree_rss_peak_bytes": max(self.rss, default=None),
                "resource_samples": len(self.cpu)}


def _commit():
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else "uncommitted"


def _new_record(manifest, output_dir, master, workers, repetition, partitions, executor_memory, cluster_id):
    import psutil
    run_id = str(uuid.uuid4())
    return {"run_id": run_id, "timestamp": datetime.now(timezone.utc).isoformat(),
            "commit_hash": _commit(), "experiment": "worker_scaling" if master.startswith("spark://") else "local_data_scale",
            "master": master, "worker_count": workers if master.startswith("spark://") else None,
            "executor_cores": 1 if master.startswith("spark://") else None,
            "executor_memory": executor_memory, "offered_cores": workers if master.startswith("spark://") else None,
            "host_cpu_count": os.cpu_count(), "host_ram_bytes": psutil.virtual_memory().total,
            "disk_free_before_bytes": shutil.disk_usage(ROOT).free, "python_version": platform.python_version(),
            "platform": platform.platform(), "partitions": partitions, "driver_memory": "512m",
            "source_sha256": manifest["source_sha256"], "input_sha256": manifest["output_sha256"],
            "input_rows": manifest["rows"], "data_multiplier": manifest["data_multiplier"],
            "synthetic": True, "seed": manifest["seed"], "repetition": repetition,
            "metrics_path": str(Path(output_dir) / "runs" / run_id / "etl_metrics.json"),
            "attestation_path": str(Path(output_dir) / "runs" / run_id / "cluster.json"),
            "single_physical_host": True, "workload": WORKLOAD, "cluster_id": cluster_id,
            "base_rows": manifest["source_rows"], "base_sample_method": manifest.get("base_sample_method"),
            "base_source_kinds": json.dumps(manifest.get("base_source_kinds", {}), sort_keys=True)}


def measure_one(record, input_path, output_dir, master_ui=None):
    """Fresh session each repetition; excludes Spark startup and input expansion."""
    from pyspark.sql import SparkSession
    from nyc311.etl.build_silver import build_silver
    spark = None
    target = Path(output_dir) / "runs" / record["run_id"]
    target.mkdir(parents=True, exist_ok=True)
    try:
        startup = time.perf_counter()
        builder = (SparkSession.builder.master(record["master"]).appName("nyc311-benchmark")
                   .config("spark.sql.session.timeZone", "UTC")
                   .config("spark.sql.shuffle.partitions", str(record["partitions"]))
                   .config("spark.default.parallelism", str(record["partitions"]))
                   .config("spark.sql.adaptive.enabled", "false")
                   .config("spark.ui.enabled", "true")
                   .config("spark.ui.showConsoleProgress", "false")
                   .config("spark.driver.bindAddress", os.getenv("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1"))
                   .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "127.0.0.1"))
                   .config("spark.executor.memory", record["executor_memory"])
                   .config("spark.driver.memory", record["driver_memory"]))
        if record["worker_count"]:
            builder = (builder.config("spark.executor.cores", "1")
                       .config("spark.cores.max", str(record["worker_count"]))
                       .config("spark.dynamicAllocation.enabled", "false"))
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        # Materialize a JVM-only task before attestation; Python workers are not counted.
        spark.range(0, 1024, 1, record["partitions"]).selectExpr("id % 2 AS k").groupBy("k").count().collect()
        evidence = attest_cluster(spark, record["worker_count"], master_ui)
        write_json(record["attestation_path"], evidence)
        record.update({"registered_workers": evidence.get("registered_workers"),
                       "active_executors": evidence.get("active_executors"),
                       "spark_version": spark.version,
                       "spark_startup_seconds": time.perf_counter() - startup})
        # ETL has explicit actions for cleaning, dedup, Parquet and all aggregates.
        with ResourceSampler() as resources:
            start = time.perf_counter()
            metrics = build_silver(str(input_path), str(target / "silver"), record["metrics_path"], spark=spark)
            elapsed = time.perf_counter() - start
        processed = metrics.get("accepted_rows", metrics.get("output_rows"))
        if processed is None:
            raise RuntimeError(f"ETL did not report processed rows: {metrics.keys()}")
        record.update(resources.metrics())
        record.update({"processed_rows": processed, "elapsed_seconds": elapsed,
                       "throughput_rows_per_sec": processed / elapsed,
                       "run_status": "Succeeded", "reason": ""})
    except Exception as exc:
        record.update({"run_status": "Failed", "reason": f"{type(exc).__name__}: {exc}"})
    finally:
        record["disk_free_after_bytes"] = shutil.disk_usage(ROOT).free
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
        write_json(target / "result.json", record)
    return record


def append_result(output_dir, record):
    path = Path(output_dir) / "runs.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow(record)


def summarize(output_dir):
    path = Path(output_dir) / "runs.csv"
    if not path.exists():
        return []
    with path.open() as handle:
        records = list(csv.DictReader(handle))
    # Only equal data, workload, partition policy and executor resources compare.
    group_fields = ["experiment", "master", "source_sha256", "input_sha256", "data_multiplier", "partitions",
                    "executor_memory", "executor_cores", "workload", "cluster_id", "driver_memory", "base_rows", "base_sample_method", "base_source_kinds", "worker_count"]
    groups = {}
    for row in records:
        key = tuple(row.get(field, "") for field in group_fields)
        groups.setdefault(key, []).append(row)
    summary = []
    for key, runs in groups.items():
        successes = [r for r in runs if r["run_status"] == "Succeeded"]
        value = dict(zip(group_fields, key))
        value.update({"attempted_runs": len(runs), "successful_runs": len(successes),
                      "failed_runs": sum(r["run_status"] == "Failed" for r in runs),
                      "not_run": sum(r["run_status"] == "Not Run" for r in runs),
                      "median_elapsed_seconds": statistics.median(float(r["elapsed_seconds"]) for r in successes) if successes else None,
                      "median_throughput_rows_per_sec": statistics.median(float(r["throughput_rows_per_sec"]) for r in successes) if successes else None,
                      "input_rows": runs[0].get("input_rows"),
                      "repetitions_complete": len(successes) >= 3,
                      "speedup": None, "parallel_efficiency": None})
        summary.append(value)
    compare_fields = [f for f in group_fields if f != "worker_count"]
    for value in summary:
        if value["experiment"] != "worker_scaling" or not value["repetitions_complete"]:
            continue
        baseline = next((s for s in summary if s["worker_count"] == "1" and s["repetitions_complete"]
                         and all(s[f] == value[f] for f in compare_fields)), None)
        if baseline:
            value["speedup"] = baseline["median_elapsed_seconds"] / value["median_elapsed_seconds"]
            value["parallel_efficiency"] = value["speedup"] / int(value["worker_count"])
    write_json(Path(output_dir) / "summary.json", summary)
    if summary:
        with (Path(output_dir) / "summary.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(summary[0]))
            writer.writeheader()
            writer.writerows(summary)
    return summary


def record_not_run(input_path, output_dir, reason, workers=(1, 2, 4, 8), multiplier=1,
                   repetitions=3, partitions=8, cluster_id="local-single-host", base_rows=1000):
    manifest = expand_data(input_path, ROOT / "data/benchmark" / f"scale_{multiplier}x.jsonl", multiplier, max_base_rows=base_rows)
    for count in workers:
        for repetition in range(1, repetitions + 1):
            row = _new_record(manifest, output_dir, "spark://unavailable:7077", count,
                              repetition, partitions, "512m", cluster_id)
            row.update({"run_status": "Not Run", "reason": reason})
            append_result(output_dir, row)
    return summarize(output_dir)


def run_benchmark(input_path, output_dir="artifacts/benchmark", multipliers=(1, 10, 100),
                  repetitions=3, master="local[2]", worker_count=None, partitions=8,
                  master_ui=None, executor_memory="512m", seed=2026,
                  cluster_id="local-single-host", timeout_seconds=900, base_rows=1000):
    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for multiplier in multipliers:
        input_file = ROOT / "data/benchmark" / f"scale_{multiplier}x.jsonl"
        manifest = expand_data(input_path, input_file, int(multiplier), seed, max_base_rows=base_rows)
        for repetition in range(1, repetitions + 1):
            row = _new_record(manifest, output_dir, master, worker_count, repetition,
                              partitions, executor_memory, cluster_id)
            spec = output_dir / "runs" / row["run_id"] / "spec.json"
            write_json(spec, {"record": row, "input_path": str(input_file),
                              "output_dir": str(output_dir), "master_ui": master_ui})
            log_path = spec.parent / "spark.log"
            started = time.perf_counter()
            with log_path.open("w") as log:
                try:
                    result = subprocess.run([sys.executable, "-m", "nyc311.benchmark.run_benchmark",
                                             "--one-run", str(spec)], stdout=log, stderr=subprocess.STDOUT,
                                            timeout=timeout_seconds, check=False)
                    result_path = spec.parent / "result.json"
                    if result_path.exists():
                        row = json.loads(result_path.read_text())
                    else:
                        row.update({"run_status": "Failed", "reason": f"Spark child exited {result.returncode}; see {log_path}"})
                except subprocess.TimeoutExpired:
                    row.update({"run_status": "Failed", "reason": f"Timeout after {timeout_seconds}s; see {log_path}"})
            row["subprocess_elapsed_seconds"] = time.perf_counter() - started
            write_json(spec.parent / "result.json", row)
            append_result(output_dir, row)
    result = summarize(output_dir)
    from nyc311.benchmark.plot_scaling import plot_scaling
    plot_scaling(output_dir)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input")
    parser.add_argument("--output", default="artifacts/benchmark")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--workers", type=int)
    parser.add_argument("--master-ui")
    parser.add_argument("--multipliers", default="1,10,100")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--base-rows", type=int, default=1000)
    parser.add_argument("--cluster-id", default="local-single-host")
    parser.add_argument("--not-run-reason")
    parser.add_argument("--one-run")
    args = parser.parse_args()
    if args.one_run:
        spec = json.loads(Path(args.one_run).read_text())
        result = measure_one(**spec)
        raise SystemExit(0 if result["run_status"] == "Succeeded" else 1)
    if not args.input:
        parser.error("--input is required")
    if args.not_run_reason:
        record_not_run(args.input, args.output, args.not_run_reason,
                       cluster_id=args.cluster_id, repetitions=args.repetitions, base_rows=args.base_rows)
    else:
        run_benchmark(args.input, args.output, [int(x) for x in args.multipliers.split(",")],
                      args.repetitions, args.master, args.workers, args.partitions,
                      args.master_ui, cluster_id=args.cluster_id, base_rows=args.base_rows)
    from nyc311.benchmark.plot_scaling import plot_scaling
    plot_scaling(args.output)


if __name__ == "__main__":
    main()
