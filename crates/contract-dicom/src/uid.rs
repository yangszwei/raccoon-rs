use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// DICOM UID validation failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DicomUidError {
    /// UID must not be empty.
    #[error("DICOM UID must not be empty")]
    Empty,

    /// UID exceeds the 64-character limit defined by the DICOM standard.
    #[error("DICOM UID must not exceed 64 characters")]
    TooLong,

    /// UID contains a character outside the allowed digit-and-dot character set.
    #[error("DICOM UID must contain only digits and dots")]
    InvalidCharacter,

    /// UID contains an empty component (consecutive dots, or a leading or trailing dot).
    #[error("DICOM UID must not contain empty components")]
    EmptyComponent,
}

fn validate_uid(value: &str) -> Result<(), DicomUidError> {
    if value.is_empty() {
        return Err(DicomUidError::Empty);
    }

    if value.len() > 64 {
        return Err(DicomUidError::TooLong);
    }

    if !value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return Err(DicomUidError::InvalidCharacter);
    }

    if value.split('.').any(str::is_empty) {
        return Err(DicomUidError::EmptyComponent);
    }

    Ok(())
}

macro_rules! dicom_uid_type {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Creates a validated DICOM UID.
            pub fn new(value: impl Into<String>) -> Result<Self, DicomUidError> {
                let value = value.into();
                validate_uid(&value)?;
                Ok(Self(value))
            }

            /// Returns the UID as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the UID into its string representation.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = DicomUidError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

dicom_uid_type!(
    StudyInstanceUid,
    "Validated DICOM Study Instance UID (0020,000D)."
);
dicom_uid_type!(
    SeriesInstanceUid,
    "Validated DICOM Series Instance UID (0020,000E)."
);
dicom_uid_type!(
    SopInstanceUid,
    "Validated DICOM SOP Instance UID (0008,0018)."
);
dicom_uid_type!(SopClassUid, "Validated DICOM SOP Class UID (0008,0016).");
dicom_uid_type!(
    TransferSyntaxUid,
    "Validated DICOM Transfer Syntax UID (0002,0010)."
);

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn uid_accepts_valid_value() {
        let uid = StudyInstanceUid::new("1.2.840.10008.5.1.4.1.1.2").expect("valid UID");

        assert_eq!(uid.as_str(), "1.2.840.10008.5.1.4.1.1.2");
    }

    #[test]
    fn uid_as_ref_returns_str() {
        let uid = SeriesInstanceUid::new("1.2.3").expect("valid UID");

        assert_eq!(uid.as_ref(), "1.2.3");
    }

    #[test]
    fn uid_display_matches_value() {
        let uid = SopInstanceUid::new("1.2.3.4").expect("valid UID");

        assert_eq!(uid.to_string(), "1.2.3.4");
    }

    #[test]
    fn uid_into_string_consumes_value() {
        let uid = SopClassUid::new("1.2.3").expect("valid UID");

        assert_eq!(uid.into_string(), "1.2.3");
    }

    #[test]
    fn uid_from_str_round_trips() {
        let uid = TransferSyntaxUid::from_str("1.2.840.10008.1.2.1").expect("valid UID");

        assert_eq!(uid.as_str(), "1.2.840.10008.1.2.1");
    }

    #[test]
    fn uid_rejects_empty_value() {
        let err = StudyInstanceUid::new("").expect_err("invalid UID");

        assert_eq!(err, DicomUidError::Empty);
    }

    #[test]
    fn uid_rejects_value_exceeding_64_characters() {
        let err = StudyInstanceUid::new("1.".repeat(33)).expect_err("invalid UID");

        assert_eq!(err, DicomUidError::TooLong);
    }

    #[test]
    fn uid_rejects_non_digit_non_dot_characters() {
        let err = StudyInstanceUid::new("1.2.abc").expect_err("invalid UID");

        assert_eq!(err, DicomUidError::InvalidCharacter);
    }

    #[test]
    fn uid_rejects_consecutive_dots() {
        let err = StudyInstanceUid::new("1..2").expect_err("invalid UID");

        assert_eq!(err, DicomUidError::EmptyComponent);
    }

    #[test]
    fn uid_rejects_leading_dot() {
        let err = StudyInstanceUid::new(".1.2").expect_err("invalid UID");

        assert_eq!(err, DicomUidError::EmptyComponent);
    }

    #[test]
    fn uid_rejects_trailing_dot() {
        let err = StudyInstanceUid::new("1.2.").expect_err("invalid UID");

        assert_eq!(err, DicomUidError::EmptyComponent);
    }

    #[test]
    fn distinct_uid_types_are_not_interchangeable() {
        // Compile-time check: this test existing in the test suite is the
        // assertion. The types below must not unify.
        let _study: StudyInstanceUid = StudyInstanceUid::new("1.2.3").expect("valid UID");
        let _series: SeriesInstanceUid = SeriesInstanceUid::new("1.2.3").expect("valid UID");
        // Uncommenting the line below must not compile:
        // let _study: StudyInstanceUid = _series;
    }
}
