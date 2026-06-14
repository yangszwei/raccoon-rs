use dicom_encoding::text::{SpecificCharacterSet as DicomEncodingSpecificCharacterSet, TextCodec};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct SpecificCharacterSet {
    terms: Vec<String>,
    codec: Option<DicomEncodingSpecificCharacterSet>,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SpecificCharacterSetError {
    #[error("Specific Character Set list must not be empty")]
    Empty,
    #[error("single-value Specific Character Set must not be empty")]
    EmptySingleValue,
    #[error("Specific Character Set value contains invalid characters: {0:?}")]
    InvalidTerm(String),
    #[error("ISO_IR 192 (UTF-8) must be the only Specific Character Set value")]
    Utf8MustBeSoleValue,
    #[error("Specific Character Set values after the first must not be empty")]
    EmptyNonInitialValue,
    #[error(
        "when the first Specific Character Set value is empty, subsequent values must be ISO 2022 extension terms; got: {0:?}"
    )]
    InvalidIso2022Extension(String),
    #[error("unsupported Specific Character Set: {0}")]
    Unsupported(String),
    #[error("value is not valid for Specific Character Set {charset}: {reason}")]
    InvalidValue {
        charset: String,
        reason: &'static str,
    },
}

impl SpecificCharacterSet {
    pub fn default_repertoire() -> Self {
        Self {
            terms: Vec::new(),
            codec: Some(DicomEncodingSpecificCharacterSet::ISO_IR_6),
        }
    }

    pub fn parse_terms<I, S>(terms: I) -> Result<Self, SpecificCharacterSetError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let terms = terms
            .into_iter()
            .map(|term| term.into().trim().to_string())
            .collect::<Vec<_>>();
        if terms.is_empty() {
            return Err(SpecificCharacterSetError::Empty);
        }
        validate_terms(&terms)?;

        let codec = if terms.len() == 1 {
            DicomEncodingSpecificCharacterSet::from_code(&terms[0])
        } else {
            None
        };

        Ok(Self { terms, codec })
    }

    pub fn terms(&self) -> &[String] {
        &self.terms
    }

    pub fn is_supported(&self) -> bool {
        self.codec.is_some()
    }

    pub fn is_default_repertoire(&self) -> bool {
        self.codec
            .as_ref()
            .is_some_and(|codec| codec.name().as_ref() == "ISO_IR 6")
    }

    pub fn is_utf8(&self) -> bool {
        self.codec
            .as_ref()
            .is_some_and(|codec| codec.name().as_ref() == "ISO_IR 192")
    }

    pub fn label(&self) -> String {
        if self.terms.is_empty() {
            "absent".to_string()
        } else {
            self.terms.join("\\")
        }
    }

    pub fn decode_bytes(&self, bytes: &[u8]) -> Result<String, SpecificCharacterSetError> {
        if self.is_default_repertoire() && !bytes.iter().all(u8::is_ascii) {
            return Err(SpecificCharacterSetError::InvalidValue {
                charset: self.label(),
                reason: "input bytes are not default repertoire ASCII",
            });
        }
        let Some(codec) = self.codec.as_ref() else {
            return Err(SpecificCharacterSetError::Unsupported(self.label()));
        };
        codec
            .decode(bytes)
            .map_err(|_| SpecificCharacterSetError::InvalidValue {
                charset: self.label(),
                reason: "input bytes are not valid for the declared character set",
            })
    }

    pub fn encode_text(&self, value: &str) -> Result<Vec<u8>, SpecificCharacterSetError> {
        if self.is_default_repertoire() && !default_repertoire_text(value) {
            return Err(SpecificCharacterSetError::InvalidValue {
                charset: self.label(),
                reason: "value contains characters outside default repertoire ASCII",
            });
        }
        let Some(codec) = self.codec.as_ref() else {
            return Err(SpecificCharacterSetError::Unsupported(self.label()));
        };
        codec
            .encode(value)
            .map_err(|_| SpecificCharacterSetError::InvalidValue {
                charset: self.label(),
                reason: "value contains characters outside the declared character set",
            })
    }
}

pub fn default_repertoire_text(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| matches!(byte, b'\n' | b'\r' | b'\t' | 0x20..=0x7e))
}

fn validate_terms(terms: &[String]) -> Result<(), SpecificCharacterSetError> {
    if terms.len() == 1 && terms[0].is_empty() {
        return Err(SpecificCharacterSetError::EmptySingleValue);
    }
    for term in terms {
        if !term
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
        {
            return Err(SpecificCharacterSetError::InvalidTerm(term.clone()));
        }
    }
    if terms.iter().any(|term| term == "ISO_IR 192") && terms.len() > 1 {
        return Err(SpecificCharacterSetError::Utf8MustBeSoleValue);
    }
    if terms.first().is_some_and(|term| term.is_empty()) {
        for term in terms.iter().skip(1) {
            if term.is_empty() || !term.starts_with("ISO 2022") {
                return Err(SpecificCharacterSetError::InvalidIso2022Extension(
                    term.clone(),
                ));
            }
        }
    } else {
        for term in terms.iter().skip(1) {
            if term.is_empty() {
                return Err(SpecificCharacterSetError::EmptyNonInitialValue);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_must_be_sole_value() {
        let error = SpecificCharacterSet::parse_terms(["ISO_IR 192", "ISO_IR 100"])
            .expect_err("UTF-8 cannot be combined");

        assert_eq!(error, SpecificCharacterSetError::Utf8MustBeSoleValue);
    }

    #[test]
    fn latin1_decodes_to_unicode() {
        let charset = SpecificCharacterSet::parse_terms(["ISO_IR 100"]).unwrap();

        assert_eq!(charset.decode_bytes(b"Caf\xe9").unwrap(), "Café");
    }

    #[test]
    fn default_repertoire_rejects_non_ascii() {
        let charset = SpecificCharacterSet::default_repertoire();

        assert!(charset.decode_bytes("Café".as_bytes()).is_err());
    }

    #[test]
    fn iso2022_jis_decodes_to_unicode() {
        let charset = SpecificCharacterSet::parse_terms(["ISO 2022 IR 87"]).unwrap();

        assert_eq!(charset.decode_bytes(b"\x1b$B8!::").unwrap(), "検査");
    }
}
