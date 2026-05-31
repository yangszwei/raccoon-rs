use crate::assertions::{assert_output_contains, command_succeeded, dicom_files};
use crate::dcmtk::{self, Key, Priority, QueryModel};
use crate::harness::DimseEndpoint;

pub fn with_move_destination(ctx: &impl DimseEndpoint, run: impl FnOnce(&std::path::Path)) {
    let out_dir = ctx.path("move-destination");
    let mut storescp = ctx.start_storescp(&out_dir);
    run(&out_dir);
    storescp.kill_and_wait().expect("stop storescp");
}

pub fn patient_root_move_levels(ctx: &impl DimseEndpoint, out_dir: &std::path::Path) {
    for (name, model, keys) in [
        (
            "C-MOVE Patient Root PATIENT level",
            QueryModel::PatientRoot,
            vec![
                Key::new("QueryRetrieveLevel", "PATIENT"),
                Key::new("PatientID", &ctx.fixture().patient_id),
            ],
        ),
        (
            "C-MOVE Patient Root STUDY level",
            QueryModel::PatientRoot,
            vec![
                Key::new("QueryRetrieveLevel", "STUDY"),
                Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
            ],
        ),
        (
            "C-MOVE Patient Root SERIES level",
            QueryModel::PatientRoot,
            vec![
                Key::new("QueryRetrieveLevel", "SERIES"),
                Key::new("SeriesInstanceUID", &ctx.fixture().series_instance_uid),
            ],
        ),
        (
            "C-MOVE Patient Root IMAGE level",
            QueryModel::PatientRoot,
            vec![
                Key::new("QueryRetrieveLevel", "IMAGE"),
                Key::new("SOPInstanceUID", &ctx.fixture().sop_instance_uid),
            ],
        ),
    ] {
        run_move_case(ctx, out_dir, name, model, &keys, Priority::Default);
    }
}

pub fn study_root_move_levels(ctx: &impl DimseEndpoint, out_dir: &std::path::Path) {
    for (name, model, keys) in [
        (
            "C-MOVE Study Root STUDY level",
            QueryModel::StudyRoot,
            vec![
                Key::new("QueryRetrieveLevel", "STUDY"),
                Key::new("StudyInstanceUID", &ctx.fixture().study_instance_uid),
            ],
        ),
        (
            "C-MOVE Study Root SERIES level",
            QueryModel::StudyRoot,
            vec![
                Key::new("QueryRetrieveLevel", "SERIES"),
                Key::new("SeriesInstanceUID", &ctx.fixture().series_instance_uid),
            ],
        ),
        (
            "C-MOVE Study Root IMAGE level",
            QueryModel::StudyRoot,
            vec![
                Key::new("QueryRetrieveLevel", "IMAGE"),
                Key::new("SOPInstanceUID", &ctx.fixture().sop_instance_uid),
            ],
        ),
    ] {
        run_move_case(ctx, out_dir, name, model, &keys, Priority::Default);
    }
}

pub fn priorities(ctx: &impl DimseEndpoint, out_dir: &std::path::Path) {
    for (name, priority) in [
        ("C-MOVE default priority", Priority::Default),
        ("C-MOVE high priority", Priority::High),
        ("C-MOVE low priority", Priority::Low),
    ] {
        run_move_case(
            ctx,
            out_dir,
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

pub fn unknown_destination(ctx: &impl DimseEndpoint) {
    let output = dcmtk::movescu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        "UNKNOWN",
        QueryModel::StudyRoot,
        &[
            Key::new("QueryRetrieveLevel", "IMAGE"),
            Key::new("SOPInstanceUID", &ctx.fixture().sop_instance_uid),
        ],
        Priority::Default,
    );
    assert!(
        !output.status.success(),
        "C-MOVE unknown destination unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        output.stdout,
        output.stderr
    );
    assert_output_contains(
        "C-MOVE unknown destination",
        &output,
        "MoveDestinationUnknown",
    );
}

fn run_move_case(
    ctx: &impl DimseEndpoint,
    out_dir: &std::path::Path,
    name: &str,
    model: QueryModel,
    keys: &[Key],
    priority: Priority,
) {
    let before = dicom_files(out_dir).len();
    let output = dcmtk::movescu(
        ctx.host(),
        ctx.port(),
        ctx.called_ae(),
        ctx.move_destination_ae(),
        model,
        keys,
        priority,
    );
    command_succeeded(name, &output);
    assert_output_contains(name, &output, "Success");
    let after_files = dicom_files(out_dir);
    assert!(
        after_files.len() > before,
        "{name} did not deliver a new moved instance to {}",
        out_dir.display()
    );
    let last = after_files.last().expect("new file exists");
    let received = crate::fixture::DicomFixture::from_file(last);
    assert_eq!(
        received.sop_instance_uid,
        ctx.fixture().sop_instance_uid,
        "{name} moved unexpected SOP Instance UID"
    );
}
