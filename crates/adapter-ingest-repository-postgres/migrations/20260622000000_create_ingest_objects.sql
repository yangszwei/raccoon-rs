CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE ingest_objects
(
    ingest_object_id       uuid   PRIMARY KEY,
    upload_id              uuid   NOT NULL,
    object_key             text   NOT NULL UNIQUE,
    content_length         bigint NOT NULL,
    etag                   text   NULL,
    checksum_algorithm     text   NULL,
    checksum_value         text   NULL,
    sop_class_uid          text   NULL,
    study_instance_uid     text   NULL,
    series_instance_uid    text   NULL,
    sop_instance_uid       text   NULL UNIQUE,
    payload_representation text   NOT NULL,
    transfer_syntax_uid    text   NULL,
    source_ae              text   NULL,
    outcome_kind           text   NOT NULL,
    outcome_reason         text   NULL,
    received_at_unix_ms    bigint NOT NULL
);

CREATE INDEX idx_ingest_objects_upload_id
    ON ingest_objects (upload_id);

CREATE INDEX idx_ingest_objects_study_series_sop
    ON ingest_objects (study_instance_uid, series_instance_uid, sop_instance_uid);

CREATE TABLE ingest_object_sync_states
(
    ingest_object_id              uuid PRIMARY KEY,
    sync_state                    text   NOT NULL,
    received_at_unix_ms           bigint NOT NULL,
    sync_claim_token              text   NULL,
    sync_claimed_by               text   NULL,
    sync_claim_expires_at_unix_ms bigint NULL,
    synced_at_unix_ms             bigint NULL,
    terminal_at_unix_ms           bigint NULL,
    FOREIGN KEY (ingest_object_id)
        REFERENCES ingest_objects (ingest_object_id)
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_ingest_object_sync_states_active_claim_token
    ON ingest_object_sync_states (sync_claim_token)
    WHERE sync_claim_token IS NOT NULL;

CREATE INDEX idx_ingest_object_sync_states_pending_order
    ON ingest_object_sync_states (received_at_unix_ms, ingest_object_id)
    WHERE sync_state = 'pending';

CREATE INDEX idx_ingest_object_sync_states_pending_expiry
    ON ingest_object_sync_states (sync_claim_expires_at_unix_ms, received_at_unix_ms, ingest_object_id)
    WHERE sync_state = 'pending'
      AND sync_claim_token IS NOT NULL;

CREATE INDEX idx_ingest_object_sync_states_terminal
    ON ingest_object_sync_states (sync_state, terminal_at_unix_ms)
    WHERE terminal_at_unix_ms IS NOT NULL;

CREATE TABLE ingest_object_quarantines
(
    ingest_object_id       uuid   PRIMARY KEY,
    category               text   NOT NULL,
    reason                 text   NOT NULL,
    original_object_key    text   NOT NULL,
    quarantine_object_key  text   NOT NULL UNIQUE,
    quarantined_at_unix_ms bigint NOT NULL,
    FOREIGN KEY (ingest_object_id)
        REFERENCES ingest_objects (ingest_object_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_ingest_object_quarantines_category
    ON ingest_object_quarantines (category, quarantined_at_unix_ms);
