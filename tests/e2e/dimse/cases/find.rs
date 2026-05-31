use crate::assertions::{assert_output_contains, command_succeeded};
use crate::dcmtk::{self, Key, QueryModel};
use crate::harness::DimseEndpoint;

pub fn patient_root_levels(ctx: &impl DimseEndpoint) {
    ctx.expect_find_contains(
        "C-FIND Patient Root PATIENT level",
        QueryModel::PatientRoot,
        &[
            Key::new("QueryRetrieveLevel", "PATIENT"),
            Key::new("PatientID", &ctx.fixture().patient_id),
            Key::new("PatientName", ""),
        ],
        &ctx.fixture().patient_id,
    );
    ctx.expect_find_contains(
        "C-FIND Patient Root STUDY level",
        QueryModel::PatientRoot,
        &[
            Key::new("QueryRetrieveLevel", "STUDY"),
            Key::new("PatientID", &ctx.fixture().patient_id),
            Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
            Key::new("StudyDate", ""),
        ],
        &ctx.fixture().study_instance_uid,
    );
    ctx.expect_find_contains(
        "C-FIND Patient Root SERIES level",
        QueryModel::PatientRoot,
        &[
            Key::new("QueryRetrieveLevel", "SERIES"),
            Key::new("PatientID", &ctx.fixture().patient_id),
            Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
            Key::new("SeriesInstanceUID", &ctx.fixture().series_instance_uid),
            Key::new("Modality", ""),
        ],
        &ctx.fixture().series_instance_uid,
    );
    ctx.expect_find_contains(
        "C-FIND Patient Root IMAGE level",
        QueryModel::PatientRoot,
        &[
            Key::new("QueryRetrieveLevel", "IMAGE"),
            Key::new("PatientID", &ctx.fixture().patient_id),
            Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
            Key::new("SeriesInstanceUID", &ctx.fixture().series_instance_uid),
            Key::new("SOPInstanceUID", &ctx.fixture().sop_instance_uid),
        ],
        &ctx.fixture().sop_instance_uid,
    );
}

pub fn study_root_levels(ctx: &impl DimseEndpoint) {
    ctx.expect_find_contains(
        "C-FIND Study Root STUDY level",
        QueryModel::StudyRoot,
        &[
            Key::new("QueryRetrieveLevel", "STUDY"),
            Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
            Key::new("StudyDate", ""),
        ],
        &ctx.fixture().study_instance_uid,
    );
    ctx.expect_find_contains(
        "C-FIND Study Root SERIES level",
        QueryModel::StudyRoot,
        &[
            Key::new("QueryRetrieveLevel", "SERIES"),
            Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
            Key::new("SeriesInstanceUID", &ctx.fixture().series_instance_uid),
            Key::new("Modality", ""),
        ],
        &ctx.fixture().series_instance_uid,
    );
    ctx.expect_find_contains(
        "C-FIND Study Root IMAGE level",
        QueryModel::StudyRoot,
        &[
            Key::new("QueryRetrieveLevel", "IMAGE"),
            Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
            Key::new("SeriesInstanceUID", &ctx.fixture().series_instance_uid),
            Key::new("SOPInstanceUID", &ctx.fixture().sop_instance_uid),
        ],
        &ctx.fixture().sop_instance_uid,
    );
}

pub fn matching_keys(ctx: &impl DimseEndpoint) {
    for (name, key) in [
        (
            "exact Patient ID",
            Key::new("PatientID", &ctx.fixture().patient_id),
        ),
        (
            "exact Study UID",
            Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
        ),
        (
            "exact Series UID",
            Key::new("SeriesInstanceUID", &ctx.fixture().series_instance_uid),
        ),
        (
            "exact SOP UID",
            Key::new("SOPInstanceUID", &ctx.fixture().sop_instance_uid),
        ),
        (
            "wildcard Patient Name",
            Key::new(
                "PatientName",
                wildcard_patient_name(&ctx.fixture().patient_name),
            ),
        ),
        (
            "Study Date range",
            Key::new(
                "StudyDate",
                format!("{}-{}", ctx.fixture().study_date, ctx.fixture().study_date),
            ),
        ),
        (
            "Study Time range",
            Key::new(
                "StudyTime",
                format!("{}-{}", ctx.fixture().study_time, ctx.fixture().study_time),
            ),
        ),
        (
            "UID list",
            Key::new(
                "SOPInstanceUID",
                format!(
                    "{}\\1.2.826.0.1.3680043.10.999.404",
                    ctx.fixture().sop_instance_uid
                ),
            ),
        ),
    ] {
        let mut keys = image_level_keys(ctx);
        keys.push(key);
        ctx.expect_find_contains(
            &format!("C-FIND Study Root matching key {name}"),
            QueryModel::StudyRoot,
            &keys,
            &ctx.fixture().sop_instance_uid,
        );
    }
}

pub fn projections(ctx: &impl DimseEndpoint) {
    let mut keys = image_level_keys(ctx);
    keys.push(Key::new("Modality", ""));
    let output = dcmtk::findscu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        QueryModel::StudyRoot,
        &keys,
    );
    command_succeeded("C-FIND explicit projection includes Modality tag", &output);
    assert_output_contains(
        "C-FIND explicit projection includes Modality tag",
        &output,
        "Modality",
    );

    let mut unsupported = image_level_keys(ctx);
    unsupported.push(Key::new("InstitutionName", ""));
    let output = dcmtk::findscu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        QueryModel::StudyRoot,
        &unsupported,
    );
    command_succeeded("C-FIND unsupported optional projection succeeds", &output);
    assert_output_contains(
        "C-FIND unsupported optional projection returns requested tag",
        &output,
        "InstitutionName",
    );
}

pub fn negative_cases(ctx: &impl DimseEndpoint) {
    let missing_required = dcmtk::findscu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        QueryModel::StudyRoot,
        &[Key::new("QueryRetrieveLevel", "IMAGE")],
    );
    command_succeeded(
        "C-FIND missing required unique key transport",
        &missing_required,
    );
    assert_output_contains(
        "C-FIND missing required unique key",
        &missing_required,
        "DataSetDoesNotMatchSOPClass",
    );

    let invalid_level = dcmtk::findscu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        QueryModel::StudyRoot,
        &[
            Key::new("QueryRetrieveLevel", "PATIENT"),
            Key::new("PatientID", &ctx.fixture().patient_id),
        ],
    );
    command_succeeded(
        "C-FIND invalid Study Root PATIENT level transport",
        &invalid_level,
    );
    assert_output_contains(
        "C-FIND invalid Study Root PATIENT level",
        &invalid_level,
        "DataSetDoesNotMatchSOPClass",
    );
}

fn image_level_keys(ctx: &impl DimseEndpoint) -> Vec<Key> {
    vec![
        Key::new("QueryRetrieveLevel", "IMAGE"),
        Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
        Key::new("SeriesInstanceUID", &ctx.fixture().series_instance_uid),
        Key::new("SOPInstanceUID", &ctx.fixture().sop_instance_uid),
    ]
}

fn wildcard_patient_name(name: &str) -> String {
    let prefix = name.split('^').next().unwrap_or(name);
    format!("{prefix}*")
}
