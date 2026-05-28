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
