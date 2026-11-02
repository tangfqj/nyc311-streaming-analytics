"""Reconstruct only events evidenced by snapshot timestamps (never source CDC)."""
from datetime import datetime, timezone
from hashlib import sha256


def is_missing(value):
    """Recognize Python null and pandas/NumPy missing scalar timestamps."""
    if value is None:
        return True
    if str(value).strip() in {"", "NaT", "nan", "<NA>"}:
        return True
    try:
        return bool(value != value)
    except (ValueError, TypeError):
        return False


def timestamp(value):
    if is_missing(value):
        raise ValueError("missing timestamp")
    if isinstance(value, datetime):
        result = value
    elif value is not None and str(value).strip():
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    else:
        raise ValueError("missing timestamp")
    if result.tzinfo is None:
        # Silver timestamps are normalized UTC; raw API wall times must pass ETL first.
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def iso_timestamp(value):
    return timestamp(value).isoformat().replace("+00:00", "Z")


def event_id(request_id, version):
    return sha256(f"{request_id}:{version}".encode()).hexdigest()


def reconstruct_events(rows, *, as_of=None, source_kind="official_snapshot"):
    """Rows must be cleaned silver records, with timestamps already in UTC.

    The API snapshot's categorical values may have changed since creation; they
    are snapshot approximations, not evidence of original event payloads.
    """
    cutoff = timestamp(as_of) if as_of is not None else None
    events = []
    seen = set()
    for row in rows:
        if hasattr(row, "asDict"):
            row = row.asDict(recursive=True)
        request = str(row.get("unique_key") or row.get("request_id") or "").strip()
        if not request or request in seen:
            raise ValueError("silver input requires nonempty unique request IDs")
        seen.add(request)
        created = timestamp(row.get("created_date"))
        closed = None if is_missing(row.get("closed_date")) else timestamp(row["closed_date"])
        if closed is not None and closed < created:
            raise ValueError(f"negative resolution interval for {request}")
        if cutoff is not None and created > cutoff:
            continue
        common = {
            "request_id": request, "created_date": iso_timestamp(created),
            "agency": str(row.get("agency") or "UNKNOWN"),
            "complaint_type": str(row.get("complaint_type") or "UNKNOWN"),
            "borough": str(row.get("borough") or "UNKNOWN"),
            "source_kind": "synthetic_fixture" if row.get("is_fixture") else
                           str(row.get("source_kind") or row.get("_source_kind") or source_kind),
            "is_synthetic_update": False, "injected_duplicate": False,
            "injected_disorder": False, "injected_late": False,
        }
        for kind, version, when in [("CREATED", 1, created), ("CLOSED", 2, closed)]:
            if when is None or (cutoff is not None and when > cutoff):
                continue
            events.append({**common, "event_id": event_id(request, version),
                           "event_type": kind, "event_version": version,
                           "event_time": iso_timestamp(when),
                           "status": "CLOSED" if kind == "CLOSED" else "OPEN"})
    return sorted(events, key=lambda e: (e["event_time"], e["request_id"], e["event_version"]))
