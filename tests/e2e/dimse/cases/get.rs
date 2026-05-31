use crate::assertions::{assert_no_store_output, assert_one_received_instance, command_succeeded};
use crate::dcmtk::{self, Key, Priority, QueryModel};
use crate::harness::DimseEndpoint;

pub fn patient_root_retrieve_levels(ctx: &impl DimseEndpoint) {
    for (name, model, keys) in [
        (
            "C-GET Patient Root PATIENT level",
            QueryModel::PatientRoot,
            vec![
                Key::new("QueryRetrieveLevel", "PATIENT"),
                Key::new("PatientID", &ctx.fixture().patient_id),
            ],
        ),
        (
            "C-GET Patient Root STUDY level",
            QueryModel::PatientRoot,
            vec![
                Key::new("QueryRetrieveLevel", "STUDY"),
                Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
            ],
        ),
        (
            "C-GET Patient Root SERIES level",
            QueryModel::PatientRoot,
            vec![
                Key::new("QueryRetrieveLevel", "SERIES"),
                Key::new("SeriesInstanceUID", &ctx.fixture().series_instance_uid),
            ],
        ),
        (
            "C-GET Patient Root IMAGE level",
            QueryModel::PatientRoot,
            vec![
                Key::new("QueryRetrieveLevel", "IMAGE"),
                Key::new("SOPInstanceUID", &ctx.fixture().sop_instance_uid),
            ],
        ),
    ] {
        run_get_case(ctx, name, model, &keys, Priority::Default);
    }
}

pub fn study_root_retrieve_levels(ctx: &impl DimseEndpoint) {
    for (name, model, keys) in [
        (
            "C-GET Study Root STUDY level",
            QueryModel::StudyRoot,
            vec![
                Key::new("QueryRetrieveLevel", "STUDY"),
                Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
            ],
        ),
        (
            "C-GET Study Root SERIES level",
            QueryModel::StudyRoot,
            vec![
                Key::new("QueryRetrieveLevel", "SERIES"),
                Key::new("SeriesInstanceUID", &ctx.fixture().series_instance_uid),
            ],
        ),
        (
            "C-GET Study Root IMAGE level",
            QueryModel::StudyRoot,
            vec![
                Key::new("QueryRetrieveLevel", "IMAGE"),
                Key::new("SOPInstanceUID", &ctx.fixture().sop_instance_uid),
            ],
        ),
    ] {
        run_get_case(ctx, name, model, &keys, Priority::Default);
    }
}

pub fn priorities(ctx: &impl DimseEndpoint) {
    for (name, priority) in [
        ("C-GET default priority", Priority::Default),
        ("C-GET high priority", Priority::High),
        ("C-GET low priority", Priority::Low),
    ] {
        run_get_case(
            ctx,
            name,
            QueryModel::StudyRoot,
            &[
                Key::new("QueryRetrieveLevel", "IMAGE"),
                Key::new("SOPInstanceUID", &ctx.fixture().sop_instance_uid),
            ],
            priority,
        );
    }
}

pub fn missing_uid(ctx: &impl DimseEndpoint) {
    let out_dir = ctx.path("get-missing");
    let output = dcmtk::getscu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        QueryModel::StudyRoot,
        &[
            Key::new("QueryRetrieveLevel", "IMAGE"),
            Key::new("SOPInstanceUID", "1.2.826.0.1.3680043.10.999.404"),
        ],
        Priority::Default,
        &out_dir,
    );
    command_succeeded("C-GET missing UID completes without crash", &output);
    assert_no_store_output("C-GET missing UID", &out_dir);
}

fn run_get_case(
    ctx: &impl DimseEndpoint,
    name: &str,
    model: QueryModel,
    keys: &[Key],
    priority: Priority,
) {
    let out_dir = ctx.path(&format!("get-{}", sanitize(name)));
    let output = dcmtk::getscu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        model,
        keys,
        priority,
        &out_dir,
    );
    command_succeeded(name, &output);
    assert_one_received_instance(name, &out_dir, ctx.fixture());
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect()
}
