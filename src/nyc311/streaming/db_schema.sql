CREATE TABLE IF NOT EXISTS processed_events (
 event_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, event_type TEXT NOT NULL,
 event_version BIGINT NOT NULL, event_time TIMESTAMPTZ NOT NULL,
 agency TEXT NOT NULL, borough TEXT NOT NULL, payload_hash TEXT NOT NULL,
 payload JSONB NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS processed_events_time_idx ON processed_events(event_time);
CREATE TABLE IF NOT EXISTS ticket_current (
 request_id TEXT PRIMARY KEY, latest_event_version BIGINT NOT NULL,
 status TEXT NOT NULL CHECK (status IN ('OPEN','CLOSED')),
 created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ,
 agency TEXT NOT NULL, complaint_type TEXT NOT NULL, borough TEXT NOT NULL,
 source_kind TEXT NOT NULL, last_event_id TEXT NOT NULL,
 CHECK (closed_at IS NULL OR closed_at >= created_at)
);
CREATE TABLE IF NOT EXISTS ticket_predictions (
 request_id TEXT NOT NULL, model_version TEXT NOT NULL,
 prediction_time TIMESTAMPTZ NOT NULL DEFAULT now(),
 predicted_median_days DOUBLE PRECISION, p_closed_30d DOUBLE PRECISION,
 prediction JSONB NOT NULL, PRIMARY KEY(request_id,model_version)
);
CREATE TABLE IF NOT EXISTS batch_commits (
 pipeline_id TEXT NOT NULL, batch_id BIGINT NOT NULL,
 input_rows BIGINT NOT NULL, accepted_rows BIGINT NOT NULL,
 duplicate_rows BIGINT NOT NULL, quarantined_rows BIGINT NOT NULL,
 predictions_rows BIGINT NOT NULL, elapsed_seconds DOUBLE PRECISION NOT NULL,
 committed_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(pipeline_id,batch_id)
);
CREATE TABLE IF NOT EXISTS quarantined_events (
 quarantine_id TEXT PRIMARY KEY, pipeline_id TEXT NOT NULL, batch_id BIGINT NOT NULL,
 reason TEXT NOT NULL, payload JSONB NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS stream_pipelines (
 pipeline_id TEXT PRIMARY KEY, checkpoint_identity TEXT NOT NULL UNIQUE,
 registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
