"""Render measured benchmark families without inventing missing experiments."""
import csv
import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import statistics

from nyc311.config import write_json

CHARTS = [
    ("runtime_vs_workers.png", "worker_scaling", "runtime"),
    ("throughput_vs_workers.png", "worker_scaling", "throughput"),
    ("speedup_efficiency.png", "worker_scaling", "ratios"),
    ("runtime_vs_data_size.png", "local_data_scale", "runtime"),
    ("throughput_vs_data_size.png", "local_data_scale", "throughput"),
]
CONFIG_FIELDS = (
    "master", "source_sha256", "partitions", "executor_memory", "executor_cores",
    "driver_memory", "workload", "cluster_id", "base_rows", "base_sample_method",
    "base_source_kinds", "seed", "spark_version", "python_version", "host_cpu_count",
    "host_ram_bytes", "platform", "single_physical_host",
)
TEAL, NAVY, MUTED = "#138B87", "#17384B", "#607A87"
PALETTE = [TEAL, "#326B96", "#7474A5", "#C38D4D"]
STYLE = {
    "font.family": "DejaVu Sans", "font.size": 10,
    "figure.facecolor": "white", "axes.facecolor": "#F6FAFA",
    "text.color": NAVY, "axes.labelcolor": MUTED,
    "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.edgecolor": "#D8E4E8", "axes.labelsize": 10,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": False, "savefig.facecolor": "white",
}


def _number(row, name):
    value = float(row[name])
    if not math.isfinite(value):
        raise ValueError(f"Non-finite {name}")
    return value


def _point(rows, x):
    elapsed = [_number(row, "elapsed_seconds") for row in rows]
    throughput = [_number(row, "throughput_rows_per_sec") for row in rows]
    ids = [row.get("run_id", "") for row in rows]
    repetitions = {row.get("repetition", "") for row in rows} - {""}
    independent_ids = len(set(ids)) if all(ids) else len(rows)
    complete = len(rows) >= 3 and len(repetitions) >= 3 and independent_ids >= 3
    return {
        "x": x, "input_rows": int(rows[0]["input_rows"]),
        "input_sha256": rows[0].get("input_sha256", ""),
        "run_count": len(rows), "distinct_repetitions": len(repetitions),
        "run_ids": ids, "repetitions": [r.get("repetition", "") for r in rows],
        "raw_elapsed_seconds": elapsed, "raw_throughput_rows_per_sec": throughput,
        "median_elapsed_seconds": statistics.median(elapsed),
        "median_throughput_rows_per_sec": statistics.median(throughput),
        "complete": complete,
        "reason": "At least three distinct successful repetitions" if complete else
                  "Requires at least three distinct successful repetitions and independent runs",
    }


def _families(records):
    groups, invalid = defaultdict(list), []
    for index, row in enumerate(records):
        if row.get("experiment") not in {c[1] for c in CHARTS}:
            continue
        try:
            x_name = "worker_count" if row["experiment"] == "worker_scaling" else "data_multiplier"
            x = int(row[x_name])
            if x < 1 or int(row["input_rows"]) < 1:
                raise ValueError("Worker/multiplier and input row counts must be positive")
            config = tuple(row.get(field, "") for field in CONFIG_FIELDS)
            if row["experiment"] == "worker_scaling":
                config += (row.get("input_sha256", ""), row.get("data_multiplier", ""), row["input_rows"])
            key = (row["experiment"], config, x, row.get("input_sha256", ""), row["input_rows"])
            groups[key].append((index, row))
        except (ValueError, KeyError, TypeError) as exc:
            if row.get("run_status") == "Succeeded":
                invalid.append({"row_number": index + 2, "run_id": row.get("run_id", ""), "reason": str(exc)})
    by_series, configurations = defaultdict(list), []
    for (experiment, config, x, input_sha, input_rows), indexed_rows in groups.items():
        rows = [row for _, row in indexed_rows]
        successes = []
        for index, row in indexed_rows:
            if row.get("run_status") != "Succeeded":
                continue
            try:
                if _number(row, "elapsed_seconds") <= 0 or _number(row, "throughput_rows_per_sec") < 0:
                    raise ValueError("Runtime must be positive; throughput must be nonnegative")
                successes.append(row)
            except (ValueError, KeyError, TypeError) as exc:
                invalid.append({"row_number": index + 2, "run_id": row.get("run_id", ""), "reason": str(exc)})
        successes.sort(key=lambda r: (r.get("repetition", ""), r.get("run_id", "")))
        point = _point(successes, x) if successes else {
            "x": x, "input_rows": int(input_rows), "input_sha256": input_sha,
            "run_count": 0, "distinct_repetitions": 0, "run_ids": [], "repetitions": [],
            "raw_elapsed_seconds": [], "raw_throughput_rows_per_sec": [],
            "median_elapsed_seconds": None, "median_throughput_rows_per_sec": None,
            "complete": False, "reason": "No valid successful runs for this configuration",
        }
        point.update(experiment=experiment, config=dict(zip(CONFIG_FIELDS, config)),
                     attempted_runs=len(rows), successful_runs=len(successes),
                     failed_runs=sum(r.get("run_status") == "Failed" for r in rows),
                     not_run=sum(r.get("run_status") == "Not Run" for r in rows),
                     status="Complete" if point["complete"] else "Incomplete")
        configurations.append(point)
        if point["complete"]:
            by_series[(experiment, config)].append(point)
    families = defaultdict(list)
    for (experiment, config), points in by_series.items():
        frequencies = defaultdict(int)
        for point in points:
            frequencies[point["x"]] += 1
        valid = []
        for point in points:
            if frequencies[point["x"]] > 1:
                point.update(complete=False, status="Incomplete",
                             reason="Conflicting input hashes/row counts at the same configuration; do not combine")
            else:
                valid.append(point)
        if not valid:
            continue
        points = sorted(valid, key=lambda p: p["x"])
        baseline = next((p for p in points if p["x"] == 1), None)
        if experiment == "worker_scaling" and baseline:
            baseline_time = baseline["median_elapsed_seconds"]
            for point in points:
                point["median_speedup"] = baseline_time / point["median_elapsed_seconds"]
                point["median_parallel_efficiency"] = point["median_speedup"] / point["x"]
                point["raw_speedups"] = [baseline_time / t for t in point["raw_elapsed_seconds"]]
                point["raw_parallel_efficiencies"] = [v / point["x"] for v in point["raw_speedups"]]
        families[experiment].append({"config": dict(zip(CONFIG_FIELDS, config)), "points": points})
    for series in families.values():
        series.sort(key=lambda s: tuple(str(v) for v in s["config"].values()))
    return families, configurations, invalid


def _raw_and_median(axis, series, metric, raw_metric, x_values, *, percent=False):
    from matplotlib.ticker import FuncFormatter
    positions = {value: index for index, value in enumerate(x_values)}
    maximum = 0
    for index, group in enumerate(series):
        color = PALETTE[index % len(PALETTE)]
        xs, ys = [], []
        for point in group["points"]:
            if metric not in point:
                continue
            x, values = positions[point["x"]], point[raw_metric]
            factor = 100 if percent else 1
            values = [v * factor for v in values]
            jitter = [0] if len(values) == 1 else [(-.07 + .14 * i / (len(values) - 1)) for i in range(len(values))]
            axis.scatter([x + j for j in jitter], values, s=47, color=color,
                         alpha=.35, edgecolors="none", zorder=3)
            value = point[metric] * factor
            xs.append(x)
            ys.append(value)
            maximum = max(maximum, value, *values)
            if len(series) <= 2:
                if percent:
                    text = f"{value:.1f}%"
                elif metric == "median_throughput_rows_per_sec":
                    text = f"{value:,.0f}"
                elif metric == "median_speedup":
                    text = f"{value:.2f}x"
                else:
                    text = f"{value:.2f} s"
                axis.annotate(text, (x, value), xytext=(0, 12), textcoords="offset points",
                              ha="center", va="bottom", fontsize=10, fontweight="bold", color=color)
        label = f"{group['config'].get('master', 'Spark')}, {group['config'].get('source_sha256', '')[:7]}"
        axis.plot(xs, ys, color=color, lw=2.4, marker="o", ms=7.5,
                  markerfacecolor="white", markeredgewidth=2.2, zorder=4, label=label)
    axis.set_xlim(-.38, max(len(x_values) - 1 + .38, .38))
    axis.set_ylim(0, max(maximum * 1.24, 1e-6))
    axis.set_xticks(range(len(x_values)))
    axis.grid(axis="y", color="#DEE8EB", linewidth=.8, zorder=0)
    axis.tick_params(axis="both", length=0, pad=10)
    axis.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}%" if percent else f"{v:,.0f}" if maximum >= 100 else f"{v:g}"))
    if len(series) > 1:
        axis.legend(frameon=False, fontsize=8, loc="best")


def _heading(fig, title, subtitle):
    from matplotlib.lines import Line2D
    fig.text(.085, .958, title, ha="left", va="top", fontsize=19, fontweight="bold", color=NAVY)
    fig.text(.085, .882, subtitle, ha="left", va="top", fontsize=10.5, color=MUTED)
    handles = [Line2D([], [], marker="o", ls="", color=TEAL, alpha=.4, ms=6),
               Line2D([], [], marker="o", color=TEAL, markerfacecolor="white", lw=2, ms=6)]
    fig.legend(handles, ["Individual run", "Median"], loc="upper right",
               bbox_to_anchor=(.98, .96), frameon=False, ncol=2, fontsize=9,
               handlelength=1.7, columnspacing=1.3)


def _render(output, filename, experiment, metric, series):
    import matplotlib.pyplot as plt
    values = sorted({point["x"] for group in series for point in group["points"]})
    rows = sorted({point["input_rows"] for group in series for point in group["points"]})
    counts = sorted({point["run_count"] for group in series for point in group["points"]})
    repetitions = str(counts[0]) if len(counts) == 1 else f"{counts[0]}-{counts[-1]}"
    row_label = f"{rows[0]:,} rows per run" if len(rows) == 1 else " / ".join(f"{n:,}" for n in rows) + " benchmark rows"
    if metric == "ratios":
        fig, axes = plt.subplots(1, 2, figsize=(12, 5.9))
        fig.subplots_adjust(left=.085, right=.97, bottom=.24, top=.75, wspace=.25)
        _heading(fig, "Speedup & parallel efficiency", f"{row_label}  |  {repetitions} measured repetitions  |  one physical host")
        for axis, name, raw, percent, label in [
            (axes[0], "median_speedup", "raw_speedups", False, "Speedup vs. 1-worker median"),
            (axes[1], "median_parallel_efficiency", "raw_parallel_efficiencies", True, "Parallel efficiency"),
        ]:
            _raw_and_median(axis, series, name, raw, values, percent=percent)
            axis.set_xticklabels([str(n) for n in values])
            axis.set(xlabel="Standalone workers", ylabel=label)
            axis.axhline(100 if percent else 1, ls=(0, (3, 3)), color=NAVY, lw=.9, alpha=.4)
        caption = "Individual ratios use the 1-worker median. Total allocated cores increase with workers; this is a single-host experiment."
    else:
        fig, axis = plt.subplots(figsize=(10.5, 5.9))
        fig.subplots_adjust(left=.085, right=.97, bottom=.25, top=.75)
        worker = experiment == "worker_scaling"
        title = "ETL runtime" if metric == "runtime" else "ETL throughput"
        title += " by worker count" if worker else " by data size"
        master_names = sorted({g["config"].get("master", "") for g in series})
        subtitle = f"{row_label}  |  {repetitions} measured repetitions"
        subtitle += "  |  one physical host" if worker else "  |  " + ", ".join(master_names)
        _heading(fig, title, subtitle)
        _raw_and_median(axis, series,
                        "median_elapsed_seconds" if metric == "runtime" else "median_throughput_rows_per_sec",
                        "raw_elapsed_seconds" if metric == "runtime" else "raw_throughput_rows_per_sec", values)
        if worker:
            axis.set_xticklabels([str(n) for n in values])
            axis.set_xlabel("Spark Standalone workers")
            caption = "1 core / worker, 512 MiB / executor. Startup excluded; additional workers add resources."
            resources = {(g["config"].get("executor_cores"), g["config"].get("executor_memory")) for g in series}
            if resources != {("1", "512m")}:
                caption = "ETL timing excludes startup. Allocation details are retained per configuration in plot_manifest.json."
        else:
            labels = []
            for value in values:
                workloads = {p["input_rows"] for g in series for p in g["points"] if p["x"] == value}
                suffix = f"\n{next(iter(workloads)):,} rows" if len(workloads) == 1 else ""
                labels.append(f"{value}x{suffix}")
            axis.set_xticklabels(labels)
            axis.set_xlabel("Synthetic performance workload multiplier", labelpad=12)
            official = True
            for group in series:
                try:
                    kinds = json.loads(group["config"].get("base_source_kinds", ""))
                    official = official and bool(kinds) and set(kinds) == {"official_nyc311_snapshot"}
                except (ValueError, TypeError):
                    official = False
            prefix = "Official-base stress replicas" if official else "Stress replicas; source provenance is retained in run metadata"
            caption = f"{prefix}; these counts describe benchmark workloads, not a NYC311 census."
        axis.set_ylabel("ETL runtime (seconds)" if metric == "runtime" else "Accepted rows / ETL second", labelpad=10)
    fig.text(.085, .055, caption, ha="left", va="bottom", color=MUTED, fontsize=9)
    fig.savefig(output / filename, dpi=170, facecolor="white")
    plt.close(fig)


def plot_scaling(output_dir="artifacts/benchmark"):
    """Return only applicable PNG paths; record absent/incomplete families as metadata.

    Raw CSV is the source of every point. Median comparisons require at least
    three distinct successful repetitions of the same input and configuration.
    Charts may be regenerated independently without launching Spark.
    """
    import matplotlib
    matplotlib.use("Agg")
    from nyc311.benchmark.run_benchmark import summarize
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    csv_path = output / "runs.csv"
    before = hashlib.sha256(csv_path.read_bytes()).hexdigest() if csv_path.exists() else None
    records = []
    if csv_path.exists():
        with csv_path.open() as handle:
            records = list(csv.DictReader(handle))
    summarize(output)
    families, configurations, invalid = _families(records)
    counts = {"total": len(records), **{status: sum(r.get("run_status") == status for r in records)
                                       for status in ["Succeeded", "Failed", "Not Run"]}}
    manifest = {
        "schema_version": 2, "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_csv": str(csv_path), "source_csv_sha256": before,
        "source_run_counts": counts,
        "rendering_code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "measurement_note": "Existing persisted measurements; rendering launches no Spark workloads and modifies no runs.csv values.",
        "minimum_distinct_successful_repetitions": 3,
        "configuration_statuses": configurations, "invalid_successful_rows": invalid,
        "charts": [], "removed_stale_files": [],
    }
    figures = []
    observed_experiments = {r.get("experiment") for r in records}
    with matplotlib.rc_context(STYLE):
        for filename, experiment, metric in CHARTS:
            series = families.get(experiment, [])
            if metric == "ratios":
                series = [g for g in series if len(g["points"]) >= 2 and all("median_speedup" in p for p in g["points"])]
            family_rows = [r for r in records if r.get("experiment") == experiment]
            chart = {"filename": filename, "relative_path": filename, "path": None,
                     "experiment": experiment, "run_count": sum(p["run_count"] for g in series for p in g["points"]),
                     "complete_configurations": sum(len(g["points"]) for g in series),
                     "available_successful_runs": sum(r.get("run_status") == "Succeeded" for r in family_rows),
                     "source_run_counts": {"total": len(family_rows), **{s: sum(r.get("run_status") == s for r in family_rows) for s in ["Succeeded", "Failed", "Not Run"]}},
                     "series": series}
            if series:
                _render(output, filename, experiment, metric, series)
                chart.update(status="Generated", path=str(output / filename),
                             reason="Only complete, matching configurations shown; small dots are individual measured runs and lines are medians")
                figures.append(str(output / filename))
            else:
                if not records:
                    status, reason = "NotRun", "No benchmark runs.csv records available"
                elif experiment not in observed_experiments:
                    status, reason = "NotApplicable", f"This directory contains {', '.join(sorted(str(e) for e in observed_experiments))}; {experiment} belongs to a different experiment family"
                elif metric == "ratios":
                    status, reason = "NotRun", "Requires a complete 1-worker baseline and at least one other matching complete worker configuration"
                else:
                    status, reason = "NotRun", "No matching configuration has at least three distinct successful repetitions; see configuration_statuses and raw run counts"
                chart.update(status=status, reason=reason)
                stale = output / filename
                if stale.exists():
                    stale.unlink()
                    manifest["removed_stale_files"].append(filename)
            manifest["charts"].append(chart)
    after = hashlib.sha256(csv_path.read_bytes()).hexdigest() if csv_path.exists() else None
    if before != after:
        raise RuntimeError("Benchmark runs.csv changed during chart generation; regenerate from a stable CSV")
    manifest["source_csv_unchanged"] = True
    write_json(output / "plot_manifest.json", manifest)
    return figures
