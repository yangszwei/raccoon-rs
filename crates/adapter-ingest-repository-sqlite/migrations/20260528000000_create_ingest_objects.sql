CREATE TABLE ingest_objects
(
    ingest_object_id       TEXT    PRIMARY KEY,
    upload_id              TEXT    NOT NULL,
    object_key             TEXT    NOT NULL UNIQUE,
    content_length         INTEGER NOT NULL,
    etag                   TEXT    NULL,
    checksum_algorithm     TEXT    NULL,
    checksum_value         TEXT    NULL,
    sop_class_uid          TEXT    NULL,
    study_instance_uid     TEXT    NULL,
    series_instance_uid    TEXT    NULL,
    sop_instance_uid       TEXT    NULL UNIQUE,
    payload_representation TEXT    NOT NULL,
    transfer_syntax_uid    TEXT    NULL,
    source_ae              TEXT    NULL,
    outcome_kind           TEXT    NOT NULL,
    outcome_reason         TEXT    NULL,
    received_at_unix_ms    INTEGER NOT NULL
);

CREATE INDEX idx_ingest_objects_upload_id
    ON ingest_objects (upload_id);

CREATE INDEX idx_ingest_objects_received_at
    ON ingest_objects (received_at_unix_ms);

CREATE INDEX idx_ingest_objects_study_series_sop
    ON ingest_objects (study_instance_uid, series_instance_uid, sop_instance_uid);

CREATE TABLE ingest_object_sync_states
(
    ingest_object_id              TEXT PRIMARY KEY,
    sync_state                    TEXT    NOT NULL,
    sync_claim_token              TEXT    NULL UNIQUE,
    sync_claimed_by               TEXT    NULL,
    sync_claim_expires_at_unix_ms INTEGER NULL,
    synced_at_unix_ms             INTEGER NULL,
    terminal_at_unix_ms           INTEGER NULL,
    FOREIGN KEY (ingest_object_id)
        REFERENCES ingest_objects (ingest_object_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_ingest_object_sync_states_claim
    ON ingest_object_sync_states (sync_state, sync_claim_expires_at_unix_ms);

CREATE INDEX idx_ingest_object_sync_states_terminal
    ON ingest_object_sync_states (sync_state, terminal_at_unix_ms);

CREATE TABLE ingest_object_quarantines
(
    ingest_object_id        TEXT    PRIMARY KEY,
    category                TEXT    NOT NULL,
    reason                  TEXT    NOT NULL,
    original_object_key     TEXT    NOT NULL,
    quarantine_object_key   TEXT    NOT NULL UNIQUE,
    quarantined_at_unix_ms  INTEGER NOT NULL,
    FOREIGN KEY (ingest_object_id)
        REFERENCES ingest_objects (ingest_object_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_ingest_object_quarantines_category
    ON ingest_object_quarantines (category, quarantined_at_unix_ms);
