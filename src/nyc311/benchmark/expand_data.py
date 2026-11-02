"""Deterministic disk-backed scale data, kept separate from official records."""
import hashlib
import json
from pathlib import Path
from datetime import datetime
from collections import Counter

from nyc311.config import write_json


def source_files(input_path):
    path = Path(input_path)
    if path.is_file():
        return [path]
    files = sorted(path.rglob("*.jsonl"))
    if not files:
        raise ValueError(f"No JSONL records at {path}")
    return files


def input_digest(input_path):
    digest = hashlib.sha256()
    for path in source_files(input_path):
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def _valid_base_row(row):
    if not str(row.get("unique_key") or "").strip():
        return False
    try:
        created = datetime.fromisoformat(str(row["created_date"]).replace("Z", "+00:00"))
        closed = row.get("closed_date")
        if closed:
            end = datetime.fromisoformat(str(closed).replace("Z", "+00:00"))
            if end < created:
                return False
        return True
    except (KeyError, ValueError, TypeError):
        return False


def expand_data(input_path, output_path, multiplier=1, seed=2026, max_base_rows=None):
    """Copy rows with unique replica IDs; no randomness or false real provenance.

    All factors, including 1x, use the same metadata/schema. Every benchmark
    input is labelled synthetic, and its source count/provenance is retained.
    """
    if isinstance(multiplier, bool) or not isinstance(multiplier, int) or multiplier < 1:
        raise ValueError("multiplier must be a positive integer")
    output_path = Path(output_path)
    if "benchmark" not in output_path.parts:
        raise ValueError("Scale data must be isolated under a benchmark directory")
    if max_base_rows is not None and (isinstance(max_base_rows, bool) or max_base_rows < 1):
        raise ValueError("max_base_rows must be positive")
    files = source_files(input_path)
    if output_path.resolve() in [p.resolve() for p in files]:
        raise ValueError("Cannot overwrite official input")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    digest = input_digest(input_path)
    output_digest = hashlib.sha256()
    rows = 0
    originals = 0
    scanned = 0
    selected_keys = set()
    source_kinds = Counter()
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        with tmp.open("w") as output:
            for source in files:
                with source.open() as handle:
                    for line in handle:
                        if not line.strip():
                            continue
                        row = json.loads(line)
                        scanned += 1
                        if max_base_rows is not None:
                            if not _valid_base_row(row) or str(row["unique_key"]) in selected_keys:
                                continue
                            selected_keys.add(str(row["unique_key"]))
                        originals += 1
                        source_kinds[str(row.get("_source_kind", row.get("source_kind", "unknown")))] += 1
                        original_id = str(row.get("unique_key") or "")
                        for replica in range(multiplier):
                            replica_row = dict(row)
                            replica_row.update({
                                "unique_key": f"{original_id}__scale_{seed}_{replica}" if original_id else "",
                                "benchmark_original_unique_key": original_id,
                                "synthetic": True,
                                "_source_kind": "synthetic_scale_replica",
                                "replica_id": replica,
                                "benchmark_seed": seed,
                            })
                            serialized = json.dumps(replica_row, sort_keys=True, separators=(",", ":")) + "\n"
                            output.write(serialized)
                            output_digest.update(serialized.encode())
                            rows += 1
                        if max_base_rows is not None and originals >= max_base_rows:
                            break
                if max_base_rows is not None and originals >= max_base_rows:
                    break
        if not originals:
            raise ValueError("No valid base records selected")
        tmp.replace(output_path)
    finally:
        tmp.unlink(missing_ok=True)
    manifest = {
        "source_files": [str(p) for p in files], "source_sha256": digest,
        "source_rows": originals, "source_rows_scanned": scanned, "rows": rows,
        "max_base_rows": max_base_rows, "base_source_kinds": dict(source_kinds),
        "base_official_snapshot_only": set(source_kinds) == {"official_nyc311_snapshot"},
        "base_sample_method": "first valid unique key rows in sorted source-file order" if max_base_rows else "all input rows including invalid/duplicate rows", "data_multiplier": multiplier,
        "seed": seed, "synthetic": True, "purpose": "performance_only",
        "output": str(output_path), "output_sha256": output_digest.hexdigest(),
        "replica_id_strategy": "original_unique_key + __scale_seed_replica_id",
        "warning": "Synthetic replicas must never enter scientific or real 311 statistics.",
    }
    write_json(output_path.with_suffix(".manifest.json"), manifest)
    return manifest
