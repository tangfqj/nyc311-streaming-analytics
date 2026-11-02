import csv
import json
from pathlib import Path

import pytest

from nyc311.benchmark.expand_data import expand_data
from nyc311.benchmark.run_benchmark import append_result, summarize, attest_cluster


def test_expansion_unique_deterministic_and_labelled(tmp_path):
    raw = tmp_path / "official.jsonl"
    raw.write_text('\n'.join(json.dumps({"unique_key": str(i), "agency": "HPD", "created_date": "2025-01-01"}) for i in range(3)) + '\n')
    path = tmp_path / "benchmark/scale.jsonl"
    first = expand_data(raw, path, 10, seed=7)
    content = path.read_bytes()
    second = expand_data(raw, path, 10, seed=7)
    rows = [json.loads(line) for line in content.splitlines()]
    assert first == second
    assert len(rows) == len({r["unique_key"] for r in rows}) == 30
    assert content == path.read_bytes()
    assert first["source_rows"] == 3
    assert first["rows"] == 30
    assert all(r["synthetic"] and r["_source_kind"] == "synthetic_scale_replica" for r in rows)
    assert {r["replica_id"] for r in rows} == set(range(10))
    assert raw.read_text().count('"unique_key"') == 3


def test_expansion_refuses_scientific_directory_and_bad_factor(tmp_path):
    raw = tmp_path / "official.jsonl"
    raw.write_text('{"unique_key":"1"}\n')
    with pytest.raises(ValueError, match="isolated"):
        expand_data(raw, tmp_path / "silver/scale.jsonl")
    with pytest.raises(ValueError, match="positive"):
        expand_data(raw, tmp_path / "benchmark/scale.jsonl", 0)


def test_missing_key_stays_invalid(tmp_path):
    raw = tmp_path / "official.jsonl"
    raw.write_text('{"agency":"HPD"}\n')
    path = tmp_path / "benchmark/scale.jsonl"
    expand_data(raw, path, 2)
    assert all(not json.loads(line)["unique_key"] for line in path.read_text().splitlines())


def _run(workers, elapsed, status="Succeeded", multiplier=1, source="a", repetition=1):
    return {"experiment": "worker_scaling", "worker_count": workers,
            "elapsed_seconds": elapsed, "throughput_rows_per_sec": 120 / elapsed if elapsed else None,
            "source_sha256": source, "input_sha256": source + str(multiplier),
            "data_multiplier": multiplier, "partitions": 8, "executor_memory": "512m",
            "executor_cores": 1, "workload": "same", "cluster_id": "same-host",
            "run_status": status, "input_rows": 120, "repetition": repetition}


def test_summary_requires_repetitions_and_comparable_input(tmp_path):
    for workers, times in [(1, [10, 9, 11]), (2, [6, 5, 7])]:
        for rep, elapsed in enumerate(times):
            append_result(tmp_path, _run(workers, elapsed, repetition=rep))
    append_result(tmp_path, _run(4, None, "Failed"))
    append_result(tmp_path, _run(8, None, "Not Run"))
    for rep in range(3):
        append_result(tmp_path, _run(4, 3, source="different-data", repetition=rep))
    result = summarize(tmp_path)
    two = next(r for r in result if r["worker_count"] == "2")
    assert two["median_elapsed_seconds"] == 6
    assert two["speedup"] == pytest.approx(10 / 6)
    assert two["parallel_efficiency"] == pytest.approx(10 / 12)
    mismatch = next(r for r in result if r["source_sha256"] == "different-data")
    assert mismatch["speedup"] is None
    failed = next(r for r in result if r["worker_count"] == "4" and r["source_sha256"] == "a")
    assert failed["failed_runs"] == 1 and failed["median_elapsed_seconds"] is None
    unavailable = next(r for r in result if r["worker_count"] == "8")
    assert unavailable["not_run"] == 1 and unavailable["speedup"] is None


def test_local_threads_do_not_attest_workers():
    class Context:
        master = "local[8]"
    class Session:
        sparkContext = Context()
    result = attest_cluster(Session(), 8)
    assert result["worker_count"] is None
    assert result["active_executors"] is None


def test_standalone_requires_attestation_inputs():
    class Context:
        master = "spark://localhost:7077"
    class Session:
        sparkContext = Context()
    with pytest.raises(ValueError, match="verification"):
        attest_cluster(Session(), 8)


def test_bounded_base_selects_unique_valid_rows(tmp_path):
    source = tmp_path / "official.jsonl"
    records = [{"unique_key": "bad", "created_date": "nonsense"},
               {"unique_key": "1", "created_date": "2025-01-01"},
               {"unique_key": "1", "created_date": "2025-01-01"},
               {"unique_key": "2", "created_date": "2025-01-02"},
               {"unique_key": "3", "created_date": "2025-01-03"}]
    source.write_text(''.join(json.dumps(r) + '\n' for r in records))
    output = tmp_path / "benchmark/sample.jsonl"
    manifest = expand_data(source, output, 10, max_base_rows=2)
    assert manifest["source_rows"] == 2 and manifest["rows"] == 20
    assert manifest["source_rows_scanned"] == 4
    assert {json.loads(line)["benchmark_original_unique_key"] for line in output.read_text().splitlines()} == {"1", "2"}


def test_standalone_attests_distinct_worker_and_executor_records(monkeypatch):
    from nyc311.benchmark import run_benchmark as module
    class Context:
        master = "spark://localhost:7077"
        applicationId = "app-real"
        uiWebUrl = "http://localhost:4040"
    class Session:
        sparkContext = Context()
    def get_json(url):
        if url.endswith("/json/"):
            return {"workers": [{"id": "worker-a", "state": "ALIVE", "cores": 1},
                                {"id": "worker-b", "state": "ALIVE", "cores": 1}]}
        return [{"id": "driver", "isActive": True, "totalCores": 2},
                {"id": "0", "isActive": True, "totalCores": 1},
                {"id": "1", "isActive": True, "totalCores": 1},
                {"id": "old", "isActive": False, "totalCores": 1}]
    monkeypatch.setattr(module, "_get_json", get_json)
    result = attest_cluster(Session(), 2, "http://localhost:8080")
    assert result["registered_workers"] == result["active_executors"] == 2
    assert result["application_id"] == "app-real"


def _chart_run(experiment, x, repetition, elapsed=10.0, status="Succeeded", master=None):
    worker = experiment == "worker_scaling"
    multiplier = 1 if worker else x
    rows = 1000 * multiplier
    value = _run(x if worker else None, elapsed, status, multiplier, repetition=repetition)
    value.update(experiment=experiment, master=master or ("spark://single-host:7077" if worker else "local[2]"),
                 run_id=f"{experiment}-{x}-{repetition}", input_rows=rows, processed_rows=rows,
                 throughput_rows_per_sec=rows / elapsed if elapsed else None,
                 base_rows=1000, spark_version="3.5.3", python_version="3.11.14", driver_memory="512m")
    return value


def test_worker_charts_only_show_measured_family_and_all_repetitions(tmp_path, monkeypatch):
    import hashlib
    import matplotlib.axes
    from nyc311.benchmark.plot_scaling import plot_scaling
    for worker in [1, 2, 4, 8]:
        for rep, elapsed in enumerate([9.0, 10.0, 11.0], 1):
            append_result(tmp_path, _chart_run("worker_scaling", worker, rep, elapsed + worker))
    stale = ["runtime_vs_data_size.png", "throughput_vs_data_size.png"]
    for name in stale:
        (tmp_path / name).write_bytes(b"stale placeholder")
    original = (tmp_path / "runs.csv").read_bytes()
    scatter_sizes = []
    original_scatter = matplotlib.axes.Axes.scatter
    def scatter(axis, x, y, *args, **kwargs):
        scatter_sizes.append(len(y))
        return original_scatter(axis, x, y, *args, **kwargs)
    monkeypatch.setattr(matplotlib.axes.Axes, "scatter", scatter)
    figures = plot_scaling(tmp_path)
    assert {Path(p).name for p in figures} == {"runtime_vs_workers.png", "throughput_vs_workers.png", "speedup_efficiency.png"}
    assert sum(scatter_sizes) == 48  # 12 raw runs in each of four metric panels.
    manifest = json.loads((tmp_path / "plot_manifest.json").read_text())
    assert manifest["source_run_counts"] == {"total": 12, "Succeeded": 12, "Failed": 0, "Not Run": 0}
    assert manifest["source_csv_sha256"] == hashlib.sha256(original).hexdigest()
    assert (tmp_path / "runs.csv").read_bytes() == original
    assert set(manifest["removed_stale_files"]) == set(stale)
    assert all(not (tmp_path / name).exists() for name in stale)
    assert all(c["status"] == "NotApplicable" and c["path"] is None for c in manifest["charts"] if c["experiment"] == "local_data_scale")
    assert len(manifest["configuration_statuses"]) == 4
    for chart in manifest["charts"]:
        if chart["status"] == "Generated":
            assert chart["run_count"] == 12 and chart["complete_configurations"] == 4
            assert Path(chart["path"]).read_bytes().startswith(b"\x89PNG")
            assert Path(chart["path"]).stat().st_size > 15000
            assert [p["x"] for p in chart["series"][0]["points"]] == [1, 2, 4, 8]


def test_data_charts_label_workload_rows_and_remove_worker_placeholders(tmp_path, monkeypatch):
    import matplotlib.axes
    from nyc311.benchmark.plot_scaling import plot_scaling
    for multiplier in [1, 10, 100]:
        for rep in [1, 2, 3]:
            append_result(tmp_path, _chart_run("local_data_scale", multiplier, rep, elapsed=5 + rep))
    stale = ["runtime_vs_workers.png", "throughput_vs_workers.png", "speedup_efficiency.png"]
    for name in stale:
        (tmp_path / name).write_bytes(b"stale placeholder")
    tick_labels = []
    original_ticks = matplotlib.axes.Axes.set_xticklabels
    def set_ticks(axis, labels, *args, **kwargs):
        tick_labels.extend(labels)
        return original_ticks(axis, labels, *args, **kwargs)
    monkeypatch.setattr(matplotlib.axes.Axes, "set_xticklabels", set_ticks)
    figures = plot_scaling(tmp_path)
    assert {Path(p).name for p in figures} == {"runtime_vs_data_size.png", "throughput_vs_data_size.png"}
    assert "100x\n100,000 rows" in tick_labels
    manifest = json.loads((tmp_path / "plot_manifest.json").read_text())
    assert set(manifest["removed_stale_files"]) == set(stale)
    for chart in manifest["charts"]:
        if chart["status"] == "Generated":
            assert chart["run_count"] == 9
            assert [p["input_rows"] for p in chart["series"][0]["points"]] == [1000, 10000, 100000]
        else:
            assert chart["status"] == "NotApplicable"


def test_incomplete_charts_keep_failed_denominator_and_no_placeholder(tmp_path):
    from nyc311.benchmark.plot_scaling import plot_scaling
    for rep in [1, 2]:
        append_result(tmp_path, _chart_run("worker_scaling", 1, rep))
    append_result(tmp_path, _chart_run("worker_scaling", 1, 3, None, status="Failed"))
    (tmp_path / "runtime_vs_workers.png").write_bytes(b"old placeholder")
    assert plot_scaling(tmp_path) == []
    assert not list(tmp_path.glob("*.png"))
    manifest = json.loads((tmp_path / "plot_manifest.json").read_text())
    assert manifest["source_run_counts"] == {"total": 3, "Succeeded": 2, "Failed": 1, "Not Run": 0}
    config = manifest["configuration_statuses"][0]
    assert config["attempted_runs"] == 3 and config["successful_runs"] == 2 and config["failed_runs"] == 1
    assert config["status"] == "Incomplete"
    assert all(c["status"] == "NotRun" for c in manifest["charts"] if c["experiment"] == "worker_scaling")


def test_missing_data_creates_only_manifest(tmp_path):
    from nyc311.benchmark.plot_scaling import plot_scaling
    assert plot_scaling(tmp_path) == []
    assert not list(tmp_path.glob("*.png"))
    manifest = json.loads((tmp_path / "plot_manifest.json").read_text())
    assert all(c["status"] == "NotRun" for c in manifest["charts"])
    assert manifest["source_csv_sha256"] is None and manifest["source_run_counts"]["total"] == 0


def test_charts_do_not_pool_distinct_spark_configurations(tmp_path):
    from nyc311.benchmark.plot_scaling import plot_scaling
    for rep in [1, 2]:
        append_result(tmp_path, _chart_run("worker_scaling", 1, rep, master="spark://a:7077"))
    append_result(tmp_path, _chart_run("worker_scaling", 1, 3, master="spark://b:7077"))
    assert plot_scaling(tmp_path) == []
    manifest = json.loads((tmp_path / "plot_manifest.json").read_text())
    assert len(manifest["configuration_statuses"]) == 2
    assert all(not c["complete"] for c in manifest["configuration_statuses"])
