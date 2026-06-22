mod assertions;
mod cases;
mod dcmtk;
mod fixture;
mod harness;

use std::env;
use std::sync::{Mutex, OnceLock};

use harness::DimseEndpoint;

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_store_baseline_e2e() {
    serial(run_store_baseline);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_store_association_and_transfer_e2e() {
    serial(run_store_association_and_transfer_parameters);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_store_batch_benchmark_e2e() {
    serial(run_store_batch_benchmark);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_find_patient_root_levels_e2e() {
    serial(run_find_patient_root_levels);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_find_study_root_levels_e2e() {
    serial(run_find_study_root_levels);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_find_matching_keys_e2e() {
    serial(run_find_matching_keys);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_find_projection_and_errors_e2e() {
    serial(run_find_projection_and_errors);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_get_patient_root_levels_e2e() {
    serial(run_get_patient_root_levels);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_get_study_root_levels_e2e() {
    serial(run_get_study_root_levels);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_get_priorities_and_missing_uid_e2e() {
    serial(run_get_priorities_and_missing_uid);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_move_patient_root_levels_e2e() {
    serial(run_move_patient_root_levels);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_move_study_root_levels_e2e() {
    serial(run_move_study_root_levels);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_move_priorities_e2e() {
    serial(run_move_priorities);
}

#[test]
#[ignore = "requires DCMTK, loopback ports, and DICOM_FILE"]
fn c_move_unknown_destination_e2e() {
    serial(run_move_unknown_destination);
}

pub fn run_store_baseline() {
    let ctx = harness::RaccoonDimseTestContext::start();
    cases::store::baseline(&ctx);
}

pub fn run_store_association_and_transfer_parameters() {
    let ctx = harness::RaccoonDimseTestContext::start();
    cases::store::association_and_transfer_parameters(&ctx);
}

pub fn run_store_batch_benchmark() {
    let ctx = harness::RaccoonDimseTestContext::start();
    let batch_size = env::var("C_STORE_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    cases::store::batch_store_benchmark(&ctx, batch_size);
}

pub fn run_find_patient_root_levels() {
    let ctx = seeded_context();
    cases::find::patient_root_levels(&ctx);
}

pub fn run_find_study_root_levels() {
    let ctx = seeded_context();
    cases::find::study_root_levels(&ctx);
}

pub fn run_find_matching_keys() {
    let ctx = seeded_context();
    cases::find::matching_keys(&ctx);
}

pub fn run_find_projection_and_errors() {
    let ctx = seeded_context();
    cases::find::projections(&ctx);
    cases::find::negative_cases(&ctx);
}

pub fn run_get_patient_root_levels() {
    let ctx = seeded_context();
    cases::get::patient_root_retrieve_levels(&ctx);
}

pub fn run_get_study_root_levels() {
    let ctx = seeded_context();
    cases::get::study_root_retrieve_levels(&ctx);
}

pub fn run_get_priorities_and_missing_uid() {
    let ctx = seeded_context();
    cases::get::priorities(&ctx);
    cases::get::missing_uid(&ctx);
}

pub fn run_move_patient_root_levels() {
    let ctx = seeded_context();
    cases::mov::with_move_destination(&ctx, |out_dir| {
        cases::mov::patient_root_move_levels(&ctx, out_dir);
    });
}

pub fn run_move_study_root_levels() {
    let ctx = seeded_context();
    cases::mov::with_move_destination(&ctx, |out_dir| {
        cases::mov::study_root_move_levels(&ctx, out_dir);
    });
}

pub fn run_move_priorities() {
    let ctx = seeded_context();
    cases::mov::with_move_destination(&ctx, |out_dir| {
        cases::mov::priorities(&ctx, out_dir);
    });
}

pub fn run_move_unknown_destination() {
    let ctx = seeded_context();
    cases::mov::unknown_destination(&ctx);
}

fn seeded_context() -> harness::RaccoonDimseTestContext {
    let ctx = harness::RaccoonDimseTestContext::start();
    ctx.seed_fixture();
    ctx
}

fn serial(run: fn()) {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("DIMSE E2E serial lock poisoned");
    run();
}
