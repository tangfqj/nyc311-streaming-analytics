"""PostgreSQL owns business idempotency; Spark checkpoints own input progress.

References: Spark foreachBatch is at-least-once, so the event ledger, state,
prediction, and batch marker are committed in one PostgreSQL transaction.
"""
from datetime import timedelta
from hashlib import sha256
import json
from pathlib import Path
import time

from nyc311.config import log_event
from .event_builder import event_id, iso_timestamp, timestamp


class BatchTooLarge(ValueError):
    pass


class AfterCommitFailure(RuntimeError):
    """Deliberate failure after commit and before Spark checkpoint progress."""


def json_safe(value):
    # PostgreSQL JSONB rejects nonstandard NaN/Infinity literals. Preserve their
    # diagnostic spelling as strings so invalid records can still be quarantined.
    return json.loads(json.dumps(value, default=str), parse_constant=lambda token: f"INVALID_{token}")


def validate_event(row):
    row = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    if row.get("_corrupt_record"):
        raise ValueError("malformed JSON or incompatible field type")
    request = str(row.get("request_id") or "").strip()
    if not request:
        raise ValueError("request_id missing")
    version = row.get("event_version")
    if isinstance(version, bool) or not str(version).isdigit() or not 1 <= int(version) <= 10000:
        raise ValueError("event_version must be an integer in 1..10000")
    version = int(version)
    kind = row.get("event_type")
    if (kind == "CREATED" and version != 1) or (kind == "CLOSED" and version != 2):
        raise ValueError("reconstructed CREATED/CLOSED versions must be 1/2")
    if kind not in {"CREATED", "CLOSED", "UPDATED"}:
        raise ValueError("unsupported event_type")
    if kind == "UPDATED" and (version < 3 or row.get("is_synthetic_update") is not True):
        raise ValueError("UPDATED must be explicitly synthetic and version >=3")
    if row.get("event_id") != event_id(request, version):
        raise ValueError("event_id does not match request_id/version")
    created, occurred = timestamp(row.get("created_date")), timestamp(row.get("event_time"))
    if occurred < created or (kind == "CREATED" and occurred != created):
        raise ValueError("inconsistent created_date/event_time")
    for column in ("agency", "complaint_type", "borough", "source_kind"):
        if not isinstance(row.get(column), str) or not row[column].strip():
            raise ValueError(f"{column} missing")
    if kind == "UPDATED" and row.get("status") not in {"OPEN", "CLOSED"}:
        raise ValueError("synthetic update status missing")
    result = {**row, "request_id": request, "event_version": version,
              "created_date": iso_timestamp(created), "event_time": iso_timestamp(occurred),
              "status": "OPEN" if kind == "CREATED" else "CLOSED" if kind == "CLOSED" else row["status"]}
    return result


def payload_hash(event):
    fields = ("request_id", "event_type", "event_version", "event_time", "created_date",
              "agency", "complaint_type", "borough", "status", "is_synthetic_update", "source_kind")
    return sha256(json.dumps({k: event.get(k) for k in fields}, sort_keys=True).encode()).hexdigest()


def initialize_database(dsn):
    import psycopg
    with psycopg.connect(dsn) as connection:
        connection.execute(Path(__file__).with_name("db_schema.sql").read_text())


class PostgresSink:
    def __init__(self, dsn, pipeline_id, predictor=None, max_batch_rows=10000,
                 fail_after_commit_once=None):
        if max_batch_rows <= 0 or not pipeline_id:
            raise ValueError("positive batch limit and nonempty pipeline_id required")
        self.dsn = dsn
        self.pipeline_id = pipeline_id
        self.predictor = predictor
        self.max_batch_rows = max_batch_rows
        self.fail_after_commit_once = Path(fail_after_commit_once) if fail_after_commit_once else None
        initialize_database(dsn)

    def write_batch(self, rows, batch_id):
        import psycopg
        from psycopg.types.json import Jsonb
        started = time.monotonic()
        bounded = []
        for row in rows:
            if len(bounded) == self.max_batch_rows:
                raise BatchTooLarge(f"batch {batch_id} exceeds max_batch_rows={self.max_batch_rows}; split producer files")
            bounded.append(json_safe(row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)))
        stats = {"input_rows": len(bounded), "accepted_rows": 0, "duplicate_rows": 0,
                 "quarantined_rows": 0, "predictions_rows": 0, "replayed_batch": False}
        with psycopg.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                # Serialize concurrent retries of this pipeline before looking up its marker.
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (self.pipeline_id,))
                cursor.execute("SELECT 1 FROM batch_commits WHERE pipeline_id=%s AND batch_id=%s",
                               (self.pipeline_id, batch_id))
                if cursor.fetchone():
                    return {**stats, "replayed_batch": True}

                def quarantine(row, reason):
                    stats["quarantined_rows"] += 1
                    sanitized = json_safe(row)
                    key = sha256(json.dumps([self.pipeline_id, batch_id, sanitized, reason], sort_keys=True).encode()).hexdigest()
                    cursor.execute("INSERT INTO quarantined_events(quarantine_id,pipeline_id,batch_id,reason,payload) "
                                   "VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                   (key, self.pipeline_id, batch_id, reason, Jsonb(sanitized)))

                accepted, canonical_created = [], {}
                for row in bounded:
                    try:
                        event = validate_event(row)
                    except (ValueError, TypeError, OverflowError) as exc:
                        quarantine(row, str(exc))
                        continue
                    request = event["request_id"]
                    if request not in canonical_created:
                        cursor.execute("SELECT created_at FROM ticket_current WHERE request_id=%s", (request,))
                        stored = cursor.fetchone()
                        canonical_created[request] = iso_timestamp(stored[0]) if stored else event["created_date"]
                    if canonical_created[request] != event["created_date"]:
                        quarantine(row, "request_id has inconsistent immutable created_date")
                        continue
                    digest = payload_hash(event)
                    cursor.execute("INSERT INTO processed_events(event_id,request_id,event_type,event_version,event_time,"
                                   "agency,borough,payload_hash,payload) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                                   "ON CONFLICT(event_id) DO NOTHING RETURNING event_id",
                                   (event["event_id"], event["request_id"], event["event_type"], event["event_version"],
                                    event["event_time"], event["agency"], event["borough"], digest, Jsonb(event)))
                    if cursor.fetchone() is None:
                        cursor.execute("SELECT payload_hash FROM processed_events WHERE event_id=%s", (event["event_id"],))
                        if cursor.fetchone()[0] != digest:
                            quarantine(row, "event_id collision with conflicting authoritative payload")
                        else:
                            stats["duplicate_rows"] += 1
                        continue
                    stats["accepted_rows"] += 1
                    accepted.append(event)
                    if event["event_type"] == "CREATED" and self.predictor is not None:
                        prediction = self.predictor(event)
                        # Refuse NaN/Infinity rather than publishing a bogus numeric prediction.
                        json.dumps(prediction, allow_nan=False)
                        if not prediction.get("model_version"):
                            raise ValueError("prediction must identify model_version")
                        cursor.execute("INSERT INTO ticket_predictions(request_id,model_version,predicted_median_days,"
                                       "p_closed_30d,prediction) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING request_id",
                                       (event["request_id"], prediction["model_version"], prediction.get("predicted_median_days"),
                                        prediction.get("p_closed_30d"), Jsonb(prediction)))
                        if cursor.fetchone():
                            stats["predictions_rows"] += 1
                # Pick one highest version per request before upserting current state.
                current, closures = {}, {}
                for event in accepted:
                    if event["event_type"] == "CLOSED":
                        closures[event["request_id"]] = event["event_time"]
                    if event["request_id"] not in current or event["event_version"] > current[event["request_id"]]["event_version"]:
                        current[event["request_id"]] = event
                for event in current.values():
                    cursor.execute("INSERT INTO ticket_current(request_id,latest_event_version,status,created_at,closed_at,"
                                   "agency,complaint_type,borough,source_kind,last_event_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                                   "ON CONFLICT(request_id) DO UPDATE SET latest_event_version=EXCLUDED.latest_event_version,"
                                   "status=CASE WHEN ticket_current.status='CLOSED' THEN 'CLOSED' ELSE EXCLUDED.status END,"
                                   "closed_at=COALESCE(ticket_current.closed_at,EXCLUDED.closed_at),agency=EXCLUDED.agency,"
                                   "complaint_type=EXCLUDED.complaint_type,borough=EXCLUDED.borough,source_kind=EXCLUDED.source_kind,"
                                   "last_event_id=EXCLUDED.last_event_id WHERE EXCLUDED.latest_event_version>ticket_current.latest_event_version",
                                   (event["request_id"], event["event_version"], "CLOSED" if event["request_id"] in closures else event["status"], event["created_date"],
                                    closures.get(event["request_id"]),
                                    event["agency"], event["complaint_type"], event["borough"], event["source_kind"], event["event_id"]))
                # A valid late closure may arrive after a synthetic higher version.
                # Fill closure evidence without regressing the version or last payload.
                for request, closed_at in closures.items():
                    cursor.execute("UPDATE ticket_current SET status='CLOSED',closed_at=COALESCE(closed_at,%s) WHERE request_id=%s",
                                   (closed_at, request))
                stats["elapsed_seconds"] = time.monotonic() - started
                cursor.execute("INSERT INTO batch_commits(pipeline_id,batch_id,input_rows,accepted_rows,duplicate_rows,"
                               "quarantined_rows,predictions_rows,elapsed_seconds) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                               (self.pipeline_id, batch_id, stats["input_rows"], stats["accepted_rows"], stats["duplicate_rows"],
                                stats["quarantined_rows"], stats["predictions_rows"], stats["elapsed_seconds"]))
        log_event("postgres_sink", pipeline_id=self.pipeline_id, batch_id=batch_id,
                  prediction_model_available=self.predictor is not None, **stats)
        if self.fail_after_commit_once is not None and not self.fail_after_commit_once.exists():
            self.fail_after_commit_once.parent.mkdir(parents=True, exist_ok=True)
            self.fail_after_commit_once.write_text("database committed; deliberate pre-checkpoint failure\n")
            raise AfterCommitFailure("injected failure AFTER PostgreSQL commit and BEFORE Spark checkpoint")
        return stats

    def foreach_batch(self, frame, batch_id):
        # At most limit+1 rows cross to the driver. The extra row fails before any write.
        return self.write_batch(frame.limit(self.max_batch_rows + 1).collect(), batch_id)


def database_snapshot(dsn, *, as_of=None):
    """Exact ledger recomputation includes arbitrarily late events; no watermark drops.

    One-hour/day windows use event time and a fixed supplied as_of (or ledger max).
    The streaming current-state path never drops a ticket because it arrived late.
    """
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        if as_of is None:
            as_of = connection.execute("SELECT max(event_time) AS cutoff FROM processed_events").fetchone()["cutoff"]
        else:
            as_of = timestamp(as_of)
        counts = {}
        for table in ("processed_events", "ticket_current", "ticket_predictions", "batch_commits", "quarantined_events"):
            counts[table] = connection.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]
        tickets = connection.execute("SELECT * FROM ticket_current ORDER BY request_id").fetchall()
        batches = connection.execute("SELECT * FROM batch_commits ORDER BY pipeline_id,batch_id").fetchall()
        prediction_models = connection.execute("SELECT model_version,count(*) AS prediction_rows "
                                               "FROM ticket_predictions GROUP BY model_version ORDER BY model_version").fetchall()
        groups = connection.execute("SELECT agency,borough,count(*) FILTER(WHERE status='OPEN') AS open_tickets,"
                                    "count(*) FILTER(WHERE status='CLOSED') AS closed_tickets FROM ticket_current "
                                    "GROUP BY agency,borough ORDER BY agency,borough").fetchall()
        windows = []
        if as_of is not None:
            for label, length in (("1 hour", timedelta(hours=1)), ("1 day", timedelta(days=1))):
                records = connection.execute("SELECT agency,borough,event_type,count(*) AS event_count "
                                             "FROM processed_events WHERE event_time>%s AND event_time<=%s "
                                             "AND event_type IN ('CREATED','CLOSED') GROUP BY agency,borough,event_type "
                                             "ORDER BY agency,borough,event_type", (as_of - length, as_of)).fetchall()
                windows.extend({**record, "window": label} for record in records)
    return {"counts": counts, "tickets": tickets, "batches": batches, "agency_borough": groups,
            "prediction_models": prediction_models,
            "event_time_windows": windows, "window_as_of": as_of,
            "window_semantics": "Exact PostgreSQL event-ledger recomputation; late arrivals correct windows on refresh; no watermark dropping"}


def prediction_coverage(dsn, request_ids, pipeline_id):
    """Count actual selected-ticket coverage, including previously persisted versions."""
    import psycopg
    from psycopg.rows import dict_row
    requests = sorted(set(request_ids))
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        coverage = connection.execute("SELECT count(*) AS prediction_rows,count(DISTINCT request_id) AS predicted_tickets "
                                      "FROM ticket_predictions WHERE request_id=ANY(%s)", (requests,)).fetchone()
        versions = connection.execute("SELECT model_version,count(*) AS prediction_rows "
                                      "FROM ticket_predictions WHERE request_id=ANY(%s) "
                                      "GROUP BY model_version ORDER BY model_version", (requests,)).fetchall()
        written = connection.execute("SELECT COALESCE(sum(predictions_rows),0) AS n FROM batch_commits WHERE pipeline_id=%s",
                                     (pipeline_id,)).fetchone()["n"]
    return {"selected_ticket_count": len(requests), "selected_ticket_prediction_count": coverage["predicted_tickets"],
            "selected_ticket_prediction_rows": coverage["prediction_rows"], "model_versions": versions,
            "newly_written_prediction_rows": int(written),
            "current_callback_model_coverage": "Not verified: persisted model versions are shown explicitly"}
