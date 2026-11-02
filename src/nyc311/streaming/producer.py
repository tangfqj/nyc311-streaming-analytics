"""Publish replay files atomically; all adversarial injections are explicit."""
import json
import os
from pathlib import Path
import random
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from nyc311.config import log_event
from .event_builder import event_id, iso_timestamp, timestamp


def prepare_events(events, *, seed=311, duplicate_fraction=0, disorder_fraction=0,
                   late_fraction=0, synthetic_update_fraction=0):
    for value in (duplicate_fraction, disorder_fraction, late_fraction, synthetic_update_fraction):
        if not 0 <= value <= 1:
            raise ValueError("injection fractions must be in [0,1]")
    rng = random.Random(seed)
    result = [dict(e) for e in events]
    for event in list(result):
        if rng.random() < duplicate_fraction:
            result.append({**event, "injected_duplicate": True})
    requests = {}
    for event in events:
        requests.setdefault(event["request_id"], []).append(event)
    for request, group in sorted(requests.items()):
        if rng.random() < synthetic_update_fraction:
            latest = max(group, key=lambda e: e["event_version"])
            version = max(3, latest["event_version"] + 1)
            result.append({**latest, "event_id": event_id(request, version),
                           "event_version": version, "event_type": "UPDATED",
                           "event_time": iso_timestamp(timestamp(latest["event_time"]) + timedelta(seconds=1)),
                           "is_synthetic_update": True,
                           "source_kind": "synthetic_update_test"})
    result.sort(key=lambda e: (e["event_time"], e["request_id"], e["event_version"]))
    selected = [i for i in range(len(result)) if rng.random() < disorder_fraction]
    shuffled = [result[i] for i in selected]
    rng.shuffle(shuffled)
    for i, event in zip(selected, shuffled):
        result[i] = {**event, "injected_disorder": True}
    ontime, late = [], []
    for event in result:
        (late if rng.random() < late_fraction else ontime).append(event)
    # A late event keeps its original event_time. Only delivery order is changed.
    return ontime + [{**event, "injected_late": True} for event in late]


def produce(events, incoming, *, batch_size=100, speed=0, max_events=None, seed=311,
            duplicate_fraction=0, disorder_fraction=0, late_fraction=0,
            synthetic_update_fraction=0):
    """speed is an upper bound on delivery events/second; zero disables pacing."""
    if batch_size <= 0 or speed < 0 or (max_events is not None and max_events < 0):
        raise ValueError("invalid producer limits")
    started = time.monotonic()
    events = prepare_events(events, seed=seed, duplicate_fraction=duplicate_fraction,
                            disorder_fraction=disorder_fraction, late_fraction=late_fraction,
                            synthetic_update_fraction=synthetic_update_fraction)
    if max_events is not None:
        events = events[:max_events]
    incoming = Path(incoming)
    incoming.mkdir(parents=True, exist_ok=True)
    # Hidden staging files are ignored by Spark, even if writer and reader overlap.
    run = uuid4().hex
    paths = []
    for start in range(0, len(events), batch_size):
        if speed:
            delay = start / speed - (time.monotonic() - started)
            if delay > 0:
                time.sleep(delay)
        batch = events[start:start + batch_size]
        path = incoming / f"replay-{run}-{start:010d}.json"
        staging = incoming / f".{path.name}.tmp"
        ingest = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with staging.open("w") as handle:
            for event in batch:
                handle.write(json.dumps({**event, "ingest_time": ingest}, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(staging, path)
        paths.append(str(path))
    return log_event("producer", rows=len(events), files=len(paths), paths=paths, seed=seed,
                     duplicate_rows=sum(e.get("injected_duplicate", False) for e in events),
                     disorder_rows=sum(e.get("injected_disorder", False) for e in events),
                     late_rows=sum(e.get("injected_late", False) for e in events),
                     synthetic_update_rows=sum(e.get("is_synthetic_update", False) for e in events),
                     elapsed_seconds=time.monotonic() - started,
                     source_semantics="snapshot timestamp reconstruction, not CDC")
