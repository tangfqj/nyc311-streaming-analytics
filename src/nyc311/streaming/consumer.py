"""Bounded file-source Structured Streaming and a reproducible restart demo."""
from pathlib import Path
import json
import os
import time
from uuid import uuid4

from nyc311.config import log_event, write_json
from .event_builder import reconstruct_events, timestamp, iso_timestamp
from .producer import produce
from .sink import PostgresSink, database_snapshot, prediction_coverage
from .window_stream import run_window_stream


def event_schema():
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType
    text = ["event_id", "request_id", "event_type", "event_time", "ingest_time", "created_date",
            "event_version", "agency", "complaint_type", "borough", "status", "source_kind", "_corrupt_record"]
    flags = ["is_synthetic_update", "injected_duplicate", "injected_disorder", "injected_late"]
    return StructType([StructField(name, StringType(), True) for name in text] +
                      [StructField(name, BooleanType(), True) for name in flags])


def bind_checkpoint(checkpoint, dsn, pipeline_id=None):
    """Bind a stable DB batch namespace to exactly one checkpoint identity.

    Spark restarts batch IDs from zero when a checkpoint is discarded. Reusing a
    DB namespace in that case could silently skip data, so it is rejected.
    """
    import psycopg
    checkpoint = Path(checkpoint)
    checkpoint.mkdir(parents=True, exist_ok=True)
    metadata = checkpoint / ".nyc311-pipeline.json"
    if metadata.exists():
        identity = json.loads(metadata.read_text())
        if pipeline_id is not None and pipeline_id != identity["pipeline_id"]:
            raise ValueError("checkpoint is already bound to a different pipeline_id")
    else:
        if any(checkpoint.iterdir()):
            raise ValueError("existing checkpoint lacks identity metadata; use its original metadata or a new checkpoint/pipeline")
        identity = {"pipeline_id": pipeline_id or f"nyc311-{uuid4().hex}",
                    "checkpoint_identity": uuid4().hex}
        staging = checkpoint / f".pipeline-{uuid4().hex}.tmp"
        staging.write_text(json.dumps(identity))
        os.replace(staging, metadata)
    with psycopg.connect(dsn) as connection:
        connection.execute("INSERT INTO stream_pipelines(pipeline_id,checkpoint_identity) VALUES(%s,%s) "
                           "ON CONFLICT DO NOTHING", (identity["pipeline_id"], identity["checkpoint_identity"]))
        registered = connection.execute("SELECT checkpoint_identity FROM stream_pipelines WHERE pipeline_id=%s",
                                        (identity["pipeline_id"],)).fetchone()
        if registered is None or registered[0] != identity["checkpoint_identity"]:
            raise ValueError("pipeline_id was used with another checkpoint; retain that checkpoint or choose a new pipeline_id")
    return identity["pipeline_id"]


def start_stream(spark, incoming, checkpoint, dsn, *, pipeline_id=None,
                 predictor=None, max_batch_rows=10000, fail_after_commit_once=None,
                 available_now=True):
    """State updates accept all valid late events; exact event-time windows use PG.

    No watermark is applied to the ticket ledger: dropping old updates here would
    corrupt ticket state. PostgreSQL recomputes exact 1h/1d windows from its unique
    event ledger at a declared as_of, including corrections after very late input.
    This deliberately trades unbounded persistent ledger storage for correctness
    and avoids claiming watermarked Spark aggregates are complete after eviction.
    """
    Path(incoming).mkdir(parents=True, exist_ok=True)
    from .sink import initialize_database
    initialize_database(dsn)
    pipeline_id = bind_checkpoint(checkpoint, dsn, pipeline_id)
    sink = PostgresSink(dsn, pipeline_id, predictor, max_batch_rows, fail_after_commit_once)
    frame = (spark.readStream.schema(event_schema()).option("mode", "PERMISSIVE")
             .option("columnNameOfCorruptRecord", "_corrupt_record")
             .option("maxFilesPerTrigger", 1).json(str(incoming)))
    writer = frame.writeStream.foreachBatch(sink.foreach_batch).option("checkpointLocation", str(checkpoint))
    return (writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="2 seconds")).start()


def run_stream(spark, incoming, checkpoint, dsn, *, pipeline_id=None, predictor=None,
               max_batch_rows=10000, fail_after_commit_once=None, available_now=True):
    started = time.monotonic()
    query = start_stream(spark, incoming, checkpoint, dsn, pipeline_id=pipeline_id, predictor=predictor,
                         max_batch_rows=max_batch_rows, fail_after_commit_once=fail_after_commit_once,
                         available_now=available_now)
    if not available_now:
        return query
    query.awaitTermination()
    progress = query.recentProgress
    return log_event("structured_streaming", pipeline_id=pipeline_id, run_status="Passed",
                     elapsed_seconds=time.monotonic() - started, progress=progress,
                     checkpoint=str(checkpoint), input_directory=str(incoming))


def selection_metadata(events, min_created_at=None):
    created = [timestamp(event["created_date"]) for event in events if event["event_type"] == "CREATED"]
    if not created:
        raise ValueError("stream demo has no selected CREATED events")
    cutoff = timestamp(min_created_at) if min_created_at is not None else None
    if cutoff is not None and min(created) < cutoff:
        raise ValueError("stream demo selection precedes requested creation-time cutoff")
    return {"selected_created_min": iso_timestamp(min(created)),
            "selected_created_max": iso_timestamp(max(created)),
            "min_created_at": iso_timestamp(cutoff) if cutoff is not None else None}


def stream_demo(spark, silver_path, output_dir, dsn, *, predictor=None, max_tickets=100,
                source_kind="official_snapshot", as_of=None, min_created_at=None,
                window_watermark="2 days", run_windows=True):
    """Run a real PostgreSQL demo with deliberate failure and same-checkpoint restart.

    Fixture and official demos use distinct schemas. Event uniqueness within each
    source survives new runs, and fixture tickets never enter the official ledger.
    A new checkpoint always gets a new pipeline_id, preventing stale batch IDs.
    """
    started = time.monotonic()
    import psycopg
    from psycopg.conninfo import make_conninfo
    from psycopg import sql
    output = Path(output_dir)
    run_id = uuid4().hex[:12]
    work = output / run_id
    incoming, checkpoint = work / "incoming", work / "checkpoint"
    frame = spark.read.parquet(str(silver_path))
    if min_created_at is not None:
        from pyspark.sql import functions as F
        frame = frame.filter(F.col("created_date") >= F.lit(timestamp(min_created_at)))
    rows = frame.orderBy("unique_key").limit(max_tickets).collect()
    events = reconstruct_events(rows, as_of=as_of, source_kind=source_kind)
    if not events:
        raise ValueError("stream demo has no eligible events")
    selection = selection_metadata(events, min_created_at)
    sources = {event["source_kind"] for event in events}
    if sources == {"official_snapshot"}:
        source_kind, schema = "official_snapshot", "demo_official"
    elif sources == {"synthetic_fixture"}:
        source_kind, schema = "synthetic_fixture", "demo_fixture"
    else:
        raise ValueError(f"stream demo requires one supported unmixed provenance; got {sorted(sources)}")
    with psycopg.connect(dsn) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    dsn = make_conninfo(dsn, options=f"-csearch_path={schema}")
    produced = produce(events, incoming, batch_size=25, seed=311, duplicate_fraction=0.5,
                       disorder_fraction=1.0, late_fraction=0.3, synthetic_update_fraction=0.2)
    # Publish a poison record separately; permissive parsing must quarantine it.
    invalid = incoming / "invalid-record.json"
    staging = incoming / ".invalid-record.tmp"
    staging.write_text('{"event_type":"CREATED","request_id":"intentionally-invalid-fixture"}\n')
    staging.replace(invalid)
    pipeline = f"nyc311-demo-{run_id}"
    fault_marker = work / "after-commit-fault-triggered"
    fault_observed = False
    try:
        run_stream(spark, incoming, checkpoint, dsn, pipeline_id=pipeline, predictor=predictor,
                   fail_after_commit_once=fault_marker)
    except Exception as exc:
        if not fault_marker.exists():
            raise
        fault_observed = True
        log_event("stream_demo", run_status="Expected Failure", fault="after_database_commit_before_checkpoint",
                  error=str(exc)[:2000], run_id=run_id)
    before_restart = database_snapshot(dsn)
    progress = run_stream(spark, incoming, checkpoint, dsn, pipeline_id=pipeline, predictor=predictor,
                          fail_after_commit_once=fault_marker)
    final = database_snapshot(dsn, as_of=as_of)
    counts_before_second_restart = final["counts"]
    run_stream(spark, incoming, checkpoint, dsn, pipeline_id=pipeline, predictor=predictor)
    after_second_restart = database_snapshot(dsn, as_of=as_of)
    if counts_before_second_restart != after_second_restart["counts"]:
        raise AssertionError("empty checkpoint restart changed business counts")
    ticket_by_id = {ticket["request_id"]: ticket for ticket in final["tickets"]}
    closed_ids = {event["request_id"] for event in events if event["event_type"] == "CLOSED"}
    if any(ticket_by_id[request]["status"] != "CLOSED" for request in closed_ids):
        raise AssertionError("out-of-order arrival regressed CLOSED state")
    coverage = prediction_coverage(dsn, [event["request_id"] for event in events], pipeline)
    if predictor is None:
        prediction_status = "Not Run: trained model callback not provided; any existing persisted predictions are reported separately"
    elif coverage["newly_written_prediction_rows"] == 0:
        prediction_status = ("No new predictions written: deduplicated CREATED events do not retrigger scoring; "
                             "persisted coverage and model versions are reported, without claiming current-model coverage")
    else:
        prediction_status = "Passed: new CREATED-event predictions committed; actual selected-ticket coverage and model versions reported"
    windows = (run_window_stream(spark, incoming, work / "window-checkpoint", work / "windows", window_watermark)
               if run_windows else {"run_status": "Not Run", "reason": "run_windows=False explicitly requested"})
    result = {"run_status": "Passed", "run_id": run_id, "pipeline_id": pipeline,
              "source_kind": source_kind, "database_schema": schema, **selection,
              "snapshot_tickets": len(rows), "reconstructed_events": len(events),
              "producer": produced, "fault_after_commit_observed": fault_observed,
              "before_restart_counts": before_restart["counts"], "final_database": final,
              "second_restart_unchanged": True,
              "prediction_status": prediction_status, "prediction_summary": coverage,
              "elapsed_seconds": time.monotonic() - started, "stream_progress": progress,
              "watermarked_windows": windows}
    write_json(output / "streaming_metrics.json", result)
    write_json(output / "database_snapshot.json", final)
    log_event("stream_demo", run_status="Passed", run_id=run_id,
              snapshot_tickets=len(rows), reconstructed_events=len(events), counts=final["counts"],
              elapsed_seconds=result["elapsed_seconds"])
    return result
