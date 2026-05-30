-- Read-side DICOM catalog schema.
--
-- All three tables are populated by the CQRS sync process from the ingest
-- write database.  `synced_at_unix_ms` on every table serves two purposes:
--
--   1. **Sync cursor** — the poller advances its high-water mark by
--      querying `WHERE synced_at_unix_ms > ?` on whichever table it syncs.
--
--   2. **Return-order stability** — queries order results by
--      `<table>_synced_at DESC` (newest synced entity first) with the
--      scope-level unique key as a tiebreaker so that paginated clients
--      always see the same ordering across pages.

-- ── Studies ───────────────────────────────────────────────────────────────────

CREATE TABLE studies
(
    study_instance_uid                TEXT    PRIMARY KEY,
    patient_id                        TEXT    NULL,
    patient_name                      TEXT    NULL,
    patient_birth_date                TEXT    NULL,
    patient_sex                       TEXT    NULL,
    study_date                        TEXT    NULL,
    study_time                        TEXT    NULL,
    accession_number                  TEXT    NULL,
    study_id                          TEXT    NULL,
    study_description                 TEXT    NULL,
    referring_physician_name          TEXT    NULL,
    number_of_study_related_series    INTEGER NULL,
    number_of_study_related_instances INTEGER NULL,
    synced_at_unix_ms                 INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_studies_patient_id   ON studies (patient_id);
CREATE INDEX idx_studies_patient_name ON studies (patient_name);
CREATE INDEX idx_studies_study_date   ON studies (study_date);
CREATE INDEX idx_studies_synced_at    ON studies (synced_at_unix_ms);

-- ── Series ────────────────────────────────────────────────────────────────────

CREATE TABLE series
(
    series_instance_uid                TEXT    PRIMARY KEY,
    study_instance_uid                 TEXT    NOT NULL REFERENCES studies (study_instance_uid),
    modality                           TEXT    NULL,
    series_number                      INTEGER NULL,
    series_date                        TEXT    NULL,
    series_time                        TEXT    NULL,
    series_description                 TEXT    NULL,
    body_part_examined                 TEXT    NULL,
    number_of_series_related_instances INTEGER NULL,
    synced_at_unix_ms                  INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_series_study_instance_uid ON series (study_instance_uid);
CREATE INDEX idx_series_modality           ON series (modality);
CREATE INDEX idx_series_synced_at          ON series (synced_at_unix_ms);

-- ── Instances ─────────────────────────────────────────────────────────────────
--
-- `attributes` stores the full instance dataset as DICOM JSON (PS3.18 format),
-- keyed by 8-char uppercase hex tag strings ("GGGGEEEE").  Attributes that have
-- dedicated columns are still present in the blob so that `Projection::All`
-- queries can reconstruct the full dataset without an extra join.  The dedicated
-- columns exist purely for indexed filtering and sorting.
--
-- `object_key` is the blob-store key used by service-retrieve to stream the
-- raw DICOM object.  NULL until the object is available in the object store.

CREATE TABLE instances
(
    sop_instance_uid      TEXT    PRIMARY KEY,
    sop_class_uid         TEXT    NOT NULL,
    series_instance_uid   TEXT    NOT NULL REFERENCES series (series_instance_uid),
    study_instance_uid    TEXT    NOT NULL REFERENCES studies (study_instance_uid),
    instance_number       INTEGER NULL,
    acquisition_date_time TEXT    NULL,
    transfer_syntax_uid   TEXT    NULL,
    object_key            TEXT    NULL,
    object_size_bytes     INTEGER NULL,
    attributes            TEXT    NOT NULL DEFAULT '{}',
    synced_at_unix_ms     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_instances_series_instance_uid ON instances (series_instance_uid);
CREATE INDEX idx_instances_study_instance_uid  ON instances (study_instance_uid);
CREATE INDEX idx_instances_sop_class_uid       ON instances (sop_class_uid);
CREATE INDEX idx_instances_acquisition_dt      ON instances (acquisition_date_time);
CREATE INDEX idx_instances_synced_at           ON instances (synced_at_unix_ms);
