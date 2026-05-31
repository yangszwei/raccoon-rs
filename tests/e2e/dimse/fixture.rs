use std::env;
use std::path::{Path, PathBuf};

use super::assertions::command_succeeded;
use super::dcmtk;

#[derive(Debug, Clone)]
pub struct DicomFixture {
    pub path: PathBuf,
    pub sop_class_uid: String,
    pub sop_instance_uid: String,
    pub patient_name: String,
    pub patient_id: String,
    pub study_instance_uid: String,
    pub series_instance_uid: String,
    pub study_date: String,
    pub study_time: String,
}

impl DicomFixture {
    pub fn default_file() -> PathBuf {
        env::var_os("DICOM_FILE")
            .map(PathBuf::from)
            .expect("DICOM_FILE must be set to the DICOM fixture path")
    }

    pub fn from_file(path: &Path) -> Self {
        assert!(
            path.exists(),
            "DICOM fixture does not exist: {}",
            path.display()
        );
        let output = dcmtk::dcmdump(
            path,
            [
                "0008,0016",
                "0008,0018",
                "0010,0010",
                "0010,0020",
                "0020,000D",
                "0020,000E",
                "0008,0020",
                "0008,0030",
            ],
        );
        command_succeeded("dcmdump fixture metadata", &output);
        let lines = output.combined();
        let fixture = Self {
            path: path.to_path_buf(),
            sop_class_uid: value_for_tag(&lines, "0008,0016"),
            sop_instance_uid: value_for_tag(&lines, "0008,0018"),
            patient_name: value_for_tag(&lines, "0010,0010"),
            patient_id: value_for_tag(&lines, "0010,0020"),
            study_instance_uid: value_for_tag(&lines, "0020,000d"),
            series_instance_uid: value_for_tag(&lines, "0020,000e"),
            study_date: value_for_tag(&lines, "0008,0020"),
            study_time: value_for_tag(&lines, "0008,0030"),
        };
        assert!(
            !fixture.sop_class_uid.is_empty(),
            "fixture SOP Class UID must not be empty"
        );
        fixture
    }
}

fn value_for_tag(output: &str, tag: &str) -> String {
    let needle = format!("({tag})");
    let line = output
        .lines()
        .find(|line| line.to_ascii_lowercase().contains(&needle))
        .unwrap_or_else(|| panic!("dcmdump output missing tag {tag}:\n{output}"));
    parse_dcmdump_value(line).unwrap_or_else(|| panic!("failed to parse tag {tag} from {line:?}"))
}

fn parse_dcmdump_value(line: &str) -> Option<String> {
    if let Some(start) = line.find('[') {
        let rest = &line[start + 1..];
        let end = rest.find(']')?;
        return Some(rest[..end].to_string());
    }
    let start = line.find('=')?;
    let rest = line[start + 1..].trim_start();
    let end = rest
        .find(|ch: char| ch.is_whitespace() || ch == '#')
        .unwrap_or(rest.len());
    Some(rest[..end].trim().to_string())
}
