"""Real PostgreSQL integration is skipped explicitly if DATABASE_URL is absent."""
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest

from nyc311.streaming.event_builder import event_id, reconstruct_events
from nyc311.streaming.producer import prepare_events, produce
from nyc311.streaming.sink import (
    AfterCommitFailure, BatchTooLarge, PostgresSink, database_snapshot, validate_event,
    prediction_coverage,
)


def sample_events():
    return reconstruct_events([
        {"unique_key": "fixture-1", "created_date": "2025-01-01T00:00:00Z",
         "closed_date": "2025-01-02T00:00:00Z", "agency": "HPD",
         "complaint_type": "HEAT/HOT WATER", "borough": "BRONX", "is_fixture": True},
        {"unique_key": "fixture-2", "created_date": "2025-01-03T00:00:00Z",
         "closed_date": None, "agency": "DOT", "complaint_type": "Street Condition",
         "borough": "QUEENS", "is_fixture": True},
    ], source_kind="synthetic_fixture")


def format_predictor(event):
    # Used only to test sink schema/atomic trigger. Never a scientific model score.
    assert event["event_type"] == "CREATED"
    return {"model_version": "fixture-format-test", "predicted_median_days": None,
            "p_closed_30d": None, "prediction_status": "test schema only, not a model"}


def test_reconstruct_only_evidenced_events_and_fixed_as_of():
    events = sample_events()
    assert [e["event_type"] for e in events] == ["CREATED", "CLOSED", "CREATED"]
    assert all(not e["is_synthetic_update"] for e in events)
    assert all(e["source_kind"] == "synthetic_fixture" for e in events)
    before_closure = reconstruct_events([{"unique_key": "a", "created_date": "2025-01-01T00:00:00Z",
                                          "closed_date": "2025-02-01T00:00:00Z"}], as_of="2025-01-15T00:00:00Z")
    assert len(before_closure) == 1
    assert before_closure[0]["event_id"] == event_id("a", 1)
    with pytest.raises(ValueError, match="negative"):
        reconstruct_events([{"unique_key": "a", "created_date": "2025-01-02", "closed_date": "2025-01-01"}])


def test_pandas_nat_open_ticket_reconstructs_only_created():
    import pandas as pd
    records = pd.DataFrame({"unique_key": ["open"], "created_date": [pd.Timestamp("2025-01-01")],
                            "closed_date": [pd.NaT]}).to_dict("records")
    events = reconstruct_events(records, source_kind="synthetic_fixture")
    assert len(events) == 1 and events[0]["event_type"] == "CREATED"


def test_silver_provenance_is_preserved_without_explicit_override():
    records = [{"unique_key": "fixture", "created_date": "2025-01-01", "closed_date": None,
                "source_kind": "synthetic_fixture"}]
    assert reconstruct_events(records)[0]["source_kind"] == "synthetic_fixture"


def test_creation_cutoff_metadata_enforces_heldout_demo_selection():
    from nyc311.streaming.consumer import selection_metadata
    events = sample_events()
    result = selection_metadata(events, "2025-01-01T00:00:00Z")
    assert result["selected_created_min"] == "2025-01-01T00:00:00Z"
    assert result["selected_created_max"] == "2025-01-03T00:00:00Z"
    with pytest.raises(ValueError, match="precedes"):
        selection_metadata(events, "2025-02-01T00:00:00Z")


def test_window_validation_and_progress_summary(tmp_path):
    from nyc311.streaming.window_stream import valid_window_event, summarize_progress, bind_window_config
    assert valid_window_event(sample_events()[0])
    assert not valid_window_event({"event_type": "CREATED"})
    synthetic = prepare_events(sample_events(), synthetic_update_fraction=1)
    assert all(not valid_window_event(event) for event in synthetic if event["event_type"] == "UPDATED")
    summary = summarize_progress([{"numInputRows": 3, "eventTime": {"watermark": "2025-01-10"},
                                   "stateOperators": [{"numRowsDroppedByWatermark": 1,
                                                       "customMetrics": {"numDroppedDuplicateRows": 1}}]}])
    assert summary["watermark_dropped_rows_in_retained_progress"] == 1
    assert summary["duplicate_dropped_rows_in_retained_progress"] == 1
    checkpoint = tmp_path / "checkpoint"
    bind_window_config(tmp_path / "in", checkpoint, tmp_path / "out", "2 days")
    with pytest.raises(ValueError, match="configuration changed"):
        bind_window_config(tmp_path / "in", checkpoint, tmp_path / "out", "3 days")


def test_producer_atomic_files_and_explicit_adversarial_markers(tmp_path):
    events = sample_events()
    injected = prepare_events(events, seed=17, duplicate_fraction=1, disorder_fraction=1,
                              late_fraction=1, synthetic_update_fraction=1)
    assert injected == prepare_events(events, seed=17, duplicate_fraction=1, disorder_fraction=1,
                                     late_fraction=1, synthetic_update_fraction=1)
    assert sum(e["injected_duplicate"] for e in injected) == len(events)
    assert all(e["injected_disorder"] and e["injected_late"] for e in injected)
    assert sum(e["is_synthetic_update"] for e in injected) == 2
    originals = {e["event_id"]: e["event_time"] for e in events}
    assert all(e["event_time"] == originals[e["event_id"]] for e in injected if e["event_id"] in originals)
    result = produce(events, tmp_path, batch_size=2, max_events=3)
    assert result["rows"] == 3 and result["files"] == 2
    assert not list(tmp_path.glob("*.tmp"))
    assert sum(len(p.read_text().splitlines()) for p in tmp_path.glob("*.json")) == 3


@pytest.mark.parametrize("change", [
    {"request_id": ""}, {"event_time": "yesterday"}, {"event_version": "1.1"},
    {"event_id": "wrong"}, {"event_type": "UPDATED", "event_version": 3},
    {"_corrupt_record": "broken"}, {"agency": None},
])
def test_validation_rejects_invalid_events(change):
    with pytest.raises((ValueError, TypeError)):
        validate_event({**sample_events()[0], **change})


@pytest.fixture
def postgres_dsn():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        pytest.skip("Not Run: DATABASE_URL absent; start PostgreSQL and export DATABASE_URL for real integration")
    import psycopg
    from psycopg.conninfo import make_conninfo
    from psycopg import sql
    schema = "test_stream_" + uuid4().hex
    try:
        connection = psycopg.connect(dsn, connect_timeout=5, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.fail(f"DATABASE_URL was configured but real PostgreSQL is unavailable: {exc}")
    with connection:
        connection.execute(sql.SQL("CREATE SCHEMA {} ").format(sql.Identifier(schema)))
    scoped = make_conninfo(dsn, options=f"-csearch_path={schema}")
    try:
        yield scoped
    finally:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.postgres
def test_postgres_duplicates_closed_before_created_and_exact_late_windows(postgres_dsn):
    created, closed, other = sample_events()
    sink = PostgresSink(postgres_dsn, "semantics", format_predictor)
    sink.write_batch([closed, closed], 0)
    sink.write_batch([other], 1)
    late = {**created, "injected_late": True}
    stats = sink.write_batch([late, created, {"request_id": "bad"}], 2)
    assert stats["accepted_rows"] == 1
    assert stats["duplicate_rows"] == 1
    assert stats["quarantined_rows"] == 1
    snapshot = database_snapshot(postgres_dsn, as_of="2025-01-01T12:00:00Z")
    assert snapshot["counts"] == {"processed_events": 3, "ticket_current": 2,
                                   "ticket_predictions": 2, "batch_commits": 3, "quarantined_events": 1}
    assert snapshot["tickets"][0]["status"] == "CLOSED"
    assert snapshot["tickets"][0]["latest_event_version"] == 2
    assert sum(e["event_count"] for e in snapshot["event_time_windows"] if e["window"] == "1 day") == 1
    collision = {**created, "agency": "CHANGED"}
    assert sink.write_batch([collision], 3)["quarantined_rows"] == 1


@pytest.mark.postgres
def test_postgres_commit_then_crash_replay_is_atomic(postgres_dsn, tmp_path):
    sink = PostgresSink(postgres_dsn, "crash", format_predictor,
                        fail_after_commit_once=tmp_path / "fault")
    with pytest.raises(AfterCommitFailure, match="AFTER PostgreSQL commit"):
        sink.write_batch(sample_events(), 0)
    before = database_snapshot(postgres_dsn)["counts"]
    assert sink.write_batch(sample_events(), 0)["replayed_batch"]
    assert database_snapshot(postgres_dsn)["counts"] == before
    # Same events in a different Spark batch are also individually idempotent.
    assert sink.write_batch(sample_events(), 1)["duplicate_rows"] == 3
    after = database_snapshot(postgres_dsn)["counts"]
    assert after["processed_events"] == 3 and after["ticket_predictions"] == 2


@pytest.mark.postgres
def test_postgres_prediction_failure_and_limit_roll_back_everything(postgres_dsn):
    def fail(_):
        raise RuntimeError("injected prediction failure")
    sink = PostgresSink(postgres_dsn, "rollback", fail, max_batch_rows=2)
    with pytest.raises(BatchTooLarge):
        sink.write_batch(sample_events(), 0)
    assert database_snapshot(postgres_dsn)["counts"]["processed_events"] == 0
    with pytest.raises(RuntimeError, match="prediction failure"):
        sink.write_batch(sample_events()[:1], 0)
    snapshot = database_snapshot(postgres_dsn)
    assert all(value == 0 for value in snapshot["counts"].values())
    sink.predictor = format_predictor
    assert sink.write_batch(sample_events()[:1], 0)["predictions_rows"] == 1


@pytest.mark.postgres
def test_synthetic_update_then_late_close_does_not_regress(postgres_dsn):
    created, closed, _ = sample_events()
    update = {**created, "event_id": event_id(created["request_id"], 3), "event_version": 3,
              "event_type": "UPDATED", "is_synthetic_update": True,
              "source_kind": "synthetic_update_test", "event_time": "2025-01-04T00:00:00Z"}
    sink = PostgresSink(postgres_dsn, "updates", format_predictor)
    sink.write_batch([update], 0)
    sink.write_batch([closed, created], 1)
    ticket = database_snapshot(postgres_dsn)["tickets"][0]
    assert ticket["latest_event_version"] == 3 and ticket["status"] == "CLOSED"
    assert ticket["closed_at"].isoformat().startswith("2025-01-02")


@pytest.mark.postgres
def test_cross_event_immutable_creation_conflict_is_quarantined(postgres_dsn):
    created, closed, _ = sample_events()
    sink = PostgresSink(postgres_dsn, "immutable", format_predictor)
    sink.write_batch([{**created, "created_date": "2025-01-03T00:00:00Z",
                       "event_time": "2025-01-03T00:00:00Z"}], 0)
    stats = sink.write_batch([closed], 1)
    assert stats["quarantined_rows"] == 1 and stats["accepted_rows"] == 0
    assert database_snapshot(postgres_dsn)["tickets"][0]["status"] == "OPEN"


@pytest.mark.postgres
def test_new_checkpoint_cannot_reuse_old_batch_namespace(postgres_dsn, tmp_path):
    from nyc311.streaming.consumer import bind_checkpoint
    from nyc311.streaming.sink import initialize_database
    initialize_database(postgres_dsn)
    first, second = tmp_path / "first", tmp_path / "second"
    assert bind_checkpoint(first, postgres_dsn, "same") == "same"
    assert bind_checkpoint(first, postgres_dsn) == "same"
    with pytest.raises(ValueError, match="another checkpoint"):
        bind_checkpoint(second, postgres_dsn, "same")
    with pytest.raises(ValueError, match="different pipeline_id"):
        bind_checkpoint(first, postgres_dsn, "different")
    assert bind_checkpoint(tmp_path / "auto", postgres_dsn).startswith("nyc311-")


@pytest.mark.postgres
def test_nonfinite_invalid_fields_are_quarantined(postgres_dsn):
    sink = PostgresSink(postgres_dsn, "nonfinite")
    stats = sink.write_batch([{**sample_events()[0], "event_version": float("nan")}], 0)
    assert stats["quarantined_rows"] == 1 and stats["accepted_rows"] == 0


@pytest.mark.postgres
def test_prediction_coverage_does_not_claim_replay_rescored(postgres_dsn):
    first = PostgresSink(postgres_dsn, "before-model")
    first.write_batch(sample_events(), 0)
    later = PostgresSink(postgres_dsn, "after-model", format_predictor)
    later.write_batch(sample_events(), 0)
    coverage = prediction_coverage(postgres_dsn, [e["request_id"] for e in sample_events()], "after-model")
    assert coverage["selected_ticket_count"] == 2
    assert coverage["selected_ticket_prediction_count"] == 0
    assert coverage["newly_written_prediction_rows"] == 0
    assert coverage["model_versions"] == []


@pytest.mark.postgres
@pytest.mark.spark
@pytest.mark.integration
def test_real_structured_stream_checkpoint_restart(postgres_dsn, tmp_path):
    from nyc311.config import spark_session
    from nyc311.streaming.consumer import run_stream
    spark = spark_session("nyc311-stream-restart-test", "local[1]")
    incoming, checkpoint = tmp_path / "incoming", tmp_path / "checkpoint"
    try:
        produce(sample_events(), incoming, batch_size=1, duplicate_fraction=1,
                disorder_fraction=1, late_fraction=1, seed=9)
        fault = tmp_path / "fault"
        with pytest.raises(Exception, match="AFTER PostgreSQL commit"):
            run_stream(spark, incoming, checkpoint, postgres_dsn, pipeline_id="spark-restart",
                       predictor=format_predictor, fail_after_commit_once=fault)
        assert fault.exists()
        run_stream(spark, incoming, checkpoint, postgres_dsn, pipeline_id="spark-restart", predictor=format_predictor)
        before = database_snapshot(postgres_dsn)["counts"]
        assert before["processed_events"] == 3 and before["ticket_predictions"] == 2
        run_stream(spark, incoming, checkpoint, postgres_dsn, pipeline_id="spark-restart", predictor=format_predictor)
        assert database_snapshot(postgres_dsn)["counts"] == before
        # A newly delivered, malformed record is quarantined on an existing checkpoint.
        (incoming / "invalid.json").write_text('{"event_type":"CREATED","event_time":"bad"}\nnot-json\n')
        run_stream(spark, incoming, checkpoint, postgres_dsn, pipeline_id="spark-restart", predictor=format_predictor)
        assert database_snapshot(postgres_dsn)["counts"]["quarantined_events"] == 2
    finally:
        spark.stop()


@pytest.mark.postgres
@pytest.mark.spark
@pytest.mark.integration
def test_watermarked_window_drops_late_close_but_authoritative_state_repairs(postgres_dsn, tmp_path):
    from nyc311.config import spark_session
    from nyc311.streaming.consumer import run_stream, run_window_stream
    created, closed, _ = sample_events()
    future = reconstruct_events([{"unique_key": "window-future", "created_date": "2025-01-20T00:00:00Z",
                                   "closed_date": None, "source_kind": "synthetic_fixture"}])[0]
    spark = spark_session("nyc311-watermark-test", "local[1]")
    incoming, checkpoint = tmp_path / "incoming", tmp_path / "authoritative-checkpoint"
    window_checkpoint, output = tmp_path / "window-checkpoint", tmp_path / "window-output"
    try:
        produce([created, future], incoming)
        run_stream(spark, incoming, checkpoint, postgres_dsn, pipeline_id="late-window")
        initial = run_window_stream(spark, incoming, window_checkpoint, output, "2 days")
        assert initial["run_status"] == "Passed"
        # Event-time Jan20 has advanced the watermark; Jan2 closure is now very late.
        produce([closed, future], incoming)
        run_stream(spark, incoming, checkpoint, postgres_dsn, pipeline_id="late-window")
        windows = run_window_stream(spark, incoming, window_checkpoint, output, "2 days")
        assert windows["deduplication"]["watermark_dropped_rows_in_retained_progress"] >= 1
        assert windows["deduplication"]["duplicate_dropped_rows_in_retained_progress"] >= 1
        snapshot = database_snapshot(postgres_dsn, as_of="2025-01-02T12:00:00Z")
        ticket = next(row for row in snapshot["tickets"] if row["request_id"] == created["request_id"])
        assert ticket["status"] == "CLOSED"
        assert any(row["event_type"] == "CLOSED" and row["event_count"] == 1
                   for row in snapshot["event_time_windows"] if row["window"] == "1 day")
        assert not any(row["event_type"] == "CLOSED" for row in windows["finalized_window_preview"])
    finally:
        spark.stop()
