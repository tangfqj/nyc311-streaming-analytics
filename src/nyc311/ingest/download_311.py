"""Resumable, date-windowed downloads of the daily NYC 311 snapshot.

The optional numeric ID suffix is a deterministic systematic sample, not a
random or population-complete sample. It spans the requested dates without
taking only the first chronological records. Never interpret sampled daily
counts as citywide arrivals. No addresses are requested.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from nyc311.config import log_event, write_json

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
METADATA_URL = "https://data.cityofnewyork.us/api/views/erm2-nwe9.json"
FIELDS = ["unique_key", "created_date", "closed_date", "agency", "complaint_type",
          "descriptor", "borough", "status", "incident_zip", "latitude", "longitude"]
REQUIRED_FIELDS = {"unique_key", "created_date", "closed_date", "agency", "complaint_type", "borough"}


def _get_json(session, url, params=None, *, token=None, retries=4, timeout=45, pause=0.25):
    """Retry transient HTTP/network failures; credentials are never logged."""
    headers = {"X-App-Token": token} if token else {}
    for attempt in range(retries + 1):
        try:
            response = session.get(url, params=params, headers=headers, timeout=timeout)
            if response.status_code == 429 or response.status_code >= 500:
                response.raise_for_status()
            response.raise_for_status()
            result = response.json()
            if pause:
                time.sleep(pause)
            return result
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if attempt >= retries or (status is not None and status != 429 and status < 500):
                raise
            time.sleep(min(2 ** attempt, 16))


def validate_schema(session=None, *, token=None, retries=4, timeout=45, pause=0.25):
    session = session or requests.Session()
    metadata = _get_json(session, METADATA_URL, token=token, retries=retries,
                         timeout=timeout, pause=pause)
    columns = {c["fieldName"]: c.get("dataTypeName") for c in metadata.get("columns", [])
               if "fieldName" in c}
    missing = sorted(REQUIRED_FIELDS - columns.keys())
    if missing:
        raise ValueError(f"Official API schema missing required fields: {missing}")
    return {"dataset_id": "erm2-nwe9", "metadata_url": METADATA_URL,
            "verified_at": datetime.now(timezone.utc).isoformat(), "columns": columns,
            "selected_fields": [field for field in FIELDS if field in columns],
            "problem_api_field": "complaint_type"}


def checksum(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _windows(start, end, days):
    current = date.fromisoformat(start)
    finish = date.fromisoformat(end)
    if current >= finish or days < 1:
        raise ValueError("Require start_date < end_date and window_days >= 1")
    while current < finish:
        following = min(current + timedelta(days=days), finish)
        yield current, following
        current = following


def download_311(output_dir, start_date, end_date, *, max_rows=50000, page_size=1000,
                 window_days=1, sample_suffix="17", session=None, app_token=None,
                 retries=4, timeout=45, pause=0.25):
    """Download [start_date,end_date); resume verified successful window files.

    ``sample_suffix=None`` requests all IDs. A positive ``max_rows`` bounds the
    whole download; a partially downloaded window is marked Truncated and is
    never represented as a complete day. Reruns skip only checksummed Success
    windows. A manifest with another query configuration is rejected.
    """
    started = time.monotonic()
    if not 1 <= page_size <= 50000 or (max_rows is not None and max_rows < 1):
        raise ValueError("Invalid page_size or max_rows")
    if sample_suffix is not None and not re.fullmatch(r"[0-9]{1,8}", sample_suffix):
        raise ValueError("sample_suffix must contain 1–8 decimal digits or be None")
    windows = list(_windows(start_date, end_date, window_days))
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.json"
    token = app_token or os.environ.get("SOCRATA_APP_TOKEN")
    session = session or requests.Session()
    query_config = {"start_date": start_date, "end_date": end_date,
                    "sample_suffix": sample_suffix, "window_days": window_days,
                    "max_rows": max_rows, "page_size": page_size}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("query_config") != query_config:
            raise ValueError("Existing manifest has different configuration; use a new output directory")
    else:
        manifest = {"dataset_id": "erm2-nwe9", "source_url": API_URL,
                    "source_kind": "official_nyc311_snapshot", "query_config": query_config,
                    "source_semantics": "Daily published snapshot; not a CDC event feed",
                    "sampling": {"method": "systematic_numeric_id_suffix" if sample_suffix else "complete_date_windows",
                                 "suffix": sample_suffix, "population_complete": False,
                                 "limitations": "Suffix sampling is systematic, not random; sampled counts are not population counts."
                                 if sample_suffix else "Snapshot values can change between downloads."},
                    "windows": []}
    manifest["sampling"]["population_complete"] = False
    manifest["coverage"] = {"requested_windows": len(windows), "successful_windows": 0,
                            "complete_date_windows": False}
    try:
        manifest["schema"] = validate_schema(session, token=token, retries=retries,
                                              timeout=timeout, pause=pause)
    except Exception as exc:
        manifest.update(status="Failed", failure_reason=str(exc), elapsed_seconds=time.monotonic() - started)
        write_json(manifest_path, manifest)
        log_event("download_311", status="Failed", reason=str(exc))
        raise
    fields = manifest["schema"]["selected_fields"]
    entries = {entry["start_date"]: entry for entry in manifest["windows"]}
    total = 0
    for start, end in windows:
        key = start.isoformat()
        destination = output_dir / f"311_{start}_{end}.jsonl"
        old = entries.get(key)
        if (old and old.get("status") == "Success" and destination.exists()
                and checksum(destination) == old.get("sha256")):
            total += old["rows"]
            continue
        remaining = None if max_rows is None else max_rows - total
        if remaining is not None and remaining <= 0:
            break
        where = f"created_date >= '{start}T00:00:00' AND created_date < '{end}T00:00:00'"
        if sample_suffix:
            where += f" AND unique_key like '%{sample_suffix}'"
        params = {"$select": ",".join(fields), "$where": where, "$order": "unique_key ASC", "$limit": page_size}
        entry = {"start_date": key, "end_date": end.isoformat(), "file": destination.name,
                 "request": dict(params), "rows": 0, "status": "Running"}
        entries[key] = entry
        temporary = destination.with_suffix(".jsonl.partial")
        last_key = None
        try:
            with temporary.open("w") as handle:
                while remaining is None or entry["rows"] < remaining:
                    limit = page_size if remaining is None else min(page_size, remaining - entry["rows"])
                    page_params = {**params, "$limit": limit}
                    if last_key is not None:
                        escaped_key = last_key.replace("'", "''")
                        page_params["$where"] += f" AND unique_key > '{escaped_key}'"
                    page = _get_json(session, API_URL, page_params, token=token, retries=retries,
                                     timeout=timeout, pause=pause)
                    if not isinstance(page, list):
                        raise ValueError("Socrata response must be a JSON array")
                    observed = datetime.now(timezone.utc).isoformat()
                    for row in page:
                        if not REQUIRED_FIELDS.issubset(row.keys() | {"closed_date", "borough"}):
                            raise ValueError("Data page lacks required ticket fields")
                        row["_source_kind"] = "official_nyc311_snapshot"
                        row["_observed_at"] = observed
                        handle.write(json.dumps(row, separators=(",", ":")) + "\n")
                    entry["rows"] += len(page)
                    if len(page) < limit:
                        entry["status"] = "Success"
                        break
                    next_key = str(page[-1]["unique_key"])
                    if next_key == last_key:
                        raise ValueError("Keyset pagination did not advance")
                    last_key = next_key
                else:
                    entry["status"] = "Truncated"
            temporary.replace(destination)
            entry.update(sha256=checksum(destination), downloaded_at=datetime.now(timezone.utc).isoformat(),
                         bytes=destination.stat().st_size)
            total += entry["rows"]
        except Exception as exc:
            entry.update(status="Failed", failure_reason=str(exc))
            manifest["windows"] = list(entries.values())
            manifest.update(status="Failed", rows=total, elapsed_seconds=time.monotonic() - started)
            write_json(manifest_path, manifest)
            log_event("download_311", status="Failed", window=key, reason=str(exc))
            raise
        manifest["windows"] = list(entries.values())
        manifest.update(rows=total, status="Running", updated_at=datetime.now(timezone.utc).isoformat())
        write_json(manifest_path, manifest)
    complete = len(entries) == len(windows) and all(e["status"] == "Success" for e in entries.values())
    manifest["sampling"]["population_complete"] = sample_suffix is None and complete
    manifest["coverage"] = {"requested_windows": len(windows),
                            "successful_windows": sum(e["status"] == "Success" for e in entries.values()),
                            "complete_date_windows": complete}
    manifest.update(status="Success" if complete else "Truncated", rows=total,
                    token_used=bool(token), elapsed_seconds=time.monotonic() - started,
                    updated_at=datetime.now(timezone.utc).isoformat())
    write_json(manifest_path, manifest)
    log_event("download_311", status=manifest["status"], rows=total,
              windows=len(entries), sampling=manifest["sampling"], elapsed_seconds=manifest["elapsed_seconds"])
    return manifest
