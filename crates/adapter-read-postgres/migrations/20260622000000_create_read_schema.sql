CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- Read-side DICOM catalog schema for Postgres.
--
-- The CQRS sync process populates these tables from the ingest write database.
-- `synced_at_unix_ms` supports both sync cursors and stable newest-first query
-- ordering.

CREATE TABLE studies
(
    study_instance_uid                text   PRIMARY KEY,
    patient_id                        text   NULL,
    patient_name                      text   NULL,
    patient_birth_date                text   NULL,
    patient_sex                       text   NULL,
    study_date                        text   NULL,
    study_time                        text   NULL,
    accession_number                  text   NULL,
    study_id                          text   NULL,
    study_description                 text   NULL,
    referring_physician_name          text   NULL,
    number_of_study_related_series    bigint NULL,
    number_of_study_related_instances bigint NULL,
    synced_at_unix_ms                 bigint NOT NULL DEFAULT 0
);

CREATE INDEX idx_studies_patient_id
    ON studies (patient_id);
CREATE INDEX idx_studies_patient_name_pattern
    ON studies (patient_name text_pattern_ops);
CREATE INDEX idx_studies_study_date
    ON studies (study_date);
CREATE INDEX idx_studies_synced_uid
    ON studies (synced_at_unix_ms DESC, study_instance_uid);
CREATE INDEX idx_studies_patient_synced
    ON studies (patient_id, synced_at_unix_ms DESC, study_instance_uid);

CREATE TABLE series
(
    series_instance_uid                text   PRIMARY KEY,
    study_instance_uid                 text   NOT NULL REFERENCES studies (study_instance_uid),
    modality                           text   NULL,
    series_number                      bigint NULL,
    series_date                        text   NULL,
    series_time                        text   NULL,
    series_description                 text   NULL,
    body_part_examined                 text   NULL,
    number_of_series_related_instances bigint NULL,
    synced_at_unix_ms                  bigint NOT NULL DEFAULT 0
);

CREATE INDEX idx_series_study_instance_uid
    ON series (study_instance_uid);
CREATE INDEX idx_series_modality
    ON series (modality);
CREATE INDEX idx_series_synced_uid
    ON series (synced_at_unix_ms DESC, series_instance_uid);
CREATE INDEX idx_series_study_series
    ON series (study_instance_uid, series_instance_uid);
CREATE INDEX idx_series_study_synced
    ON series (study_instance_uid, synced_at_unix_ms DESC, series_instance_uid);

CREATE TABLE instances
(
    sop_instance_uid      text   PRIMARY KEY,
    sop_class_uid         text   NOT NULL,
    series_instance_uid   text   NOT NULL REFERENCES series (series_instance_uid),
    study_instance_uid    text   NOT NULL REFERENCES studies (study_instance_uid),
    instance_number       bigint NULL,
    acquisition_date_time text   NULL,
    transfer_syntax_uid   text   NULL,
    object_key            text   NULL,
    object_size_bytes     bigint NULL,
    attributes            jsonb  NOT NULL DEFAULT '{}'::jsonb,
    synced_at_unix_ms     bigint NOT NULL DEFAULT 0
);

CREATE INDEX idx_instances_series_instance_uid
    ON instances (series_instance_uid);
CREATE INDEX idx_instances_study_instance_uid
    ON instances (study_instance_uid);
CREATE INDEX idx_instances_sop_class_uid
    ON instances (sop_class_uid);
CREATE INDEX idx_instances_acquisition_dt
    ON instances (acquisition_date_time);
CREATE INDEX idx_instances_synced_uid
    ON instances (synced_at_unix_ms DESC, sop_instance_uid);
CREATE INDEX idx_instances_study_series_instance
    ON instances (study_instance_uid, series_instance_uid, sop_instance_uid);
CREATE INDEX idx_instances_series_sop_uid
    ON instances (series_instance_uid, sop_instance_uid);
CREATE INDEX idx_instances_study_synced
    ON instances (study_instance_uid, synced_at_unix_ms DESC, sop_instance_uid);
CREATE INDEX idx_instances_series_synced
    ON instances (series_instance_uid, synced_at_unix_ms DESC, sop_instance_uid);
CREATE INDEX idx_instances_object_key_present
    ON instances (study_instance_uid, series_instance_uid, sop_instance_uid)
    INCLUDE (sop_class_uid, transfer_syntax_uid, object_key, object_size_bytes)
    WHERE object_key IS NOT NULL;
CREATE INDEX idx_instances_series_object_key_present
    ON instances (series_instance_uid, sop_instance_uid)
    INCLUDE (study_instance_uid, sop_class_uid, transfer_syntax_uid, object_key, object_size_bytes)
    WHERE object_key IS NOT NULL;
CREATE INDEX idx_instances_sop_object_key_present
    ON instances (sop_instance_uid)
    INCLUDE (study_instance_uid, series_instance_uid, sop_class_uid, transfer_syntax_uid, object_key, object_size_bytes)
    WHERE object_key IS NOT NULL;
CREATE INDEX idx_instances_attributes_gin
    ON instances USING gin (attributes jsonb_path_ops);

CREATE TABLE read_model_state
(
    id                 integer PRIMARY KEY CHECK (id = 1),
    revision           bigint  NOT NULL,
    updated_at_unix_ms bigint  NOT NULL
);

INSERT INTO read_model_state (id, revision, updated_at_unix_ms)
VALUES (1, 0, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint);
