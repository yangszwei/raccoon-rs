use std::fs;
use std::path::{Path, PathBuf};

use super::dcmtk::CommandOutput;
use super::fixture::DicomFixture;

pub fn command_succeeded(case_name: &str, output: &CommandOutput) {
    assert!(
        output.status.success(),
        "{case_name} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        output.stdout,
        output.stderr
    );
}

pub fn assert_output_contains(case_name: &str, output: &CommandOutput, needle: &str) {
    let combined = output.combined();
    assert!(
        combined.contains(needle),
        "{case_name} output did not contain {needle:?}\nstdout:\n{}\nstderr:\n{}",
        output.stdout,
        output.stderr
    );
}

pub fn assert_no_store_output(case_name: &str, dir: &Path) {
    let files = dicom_files(dir);
    assert!(
        files.is_empty(),
        "{case_name} expected no received DICOM files in {}, found {:?}",
        dir.display(),
        files
    );
}

pub fn assert_one_received_instance(
    case_name: &str,
    dir: &Path,
    fixture: &DicomFixture,
) -> PathBuf {
    let files = dicom_files(dir);
    assert_eq!(
        files.len(),
        1,
        "{case_name} expected exactly one received file in {}, found {:?}",
        dir.display(),
        files
    );
    let received = files.into_iter().next().expect("one file");
    let received_fixture = DicomFixture::from_file(&received);
    assert_eq!(
        received_fixture.sop_instance_uid, fixture.sop_instance_uid,
        "{case_name} received unexpected SOP Instance UID"
    );
    received
}

pub fn dicom_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_files(dir, &mut files);
    files.sort();
    files
}

fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) {
    if !dir.exists() {
        return;
    }
    for entry in fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("failed to read output directory {}: {err}", dir.display()))
    {
        let entry = entry.expect("directory entry");
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files);
        } else if path.is_file() {
            files.push(path);
        }
    }
}
