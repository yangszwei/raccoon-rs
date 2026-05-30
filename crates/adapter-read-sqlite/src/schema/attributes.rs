use dicom_core::Tag;
use dicom_dictionary_std::tags;

use super::tables::TableId;

/// How to interpret the column value when materializing a [`ResponseValue`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VrClass {
    /// Single-valued text VRs (CS, SH, LO, AE, UI, DA, TM, DT, PN, …).
    Text,
    /// Integer-string VRs (IS) stored as INTEGER; converted to text on read.
    Integer,
}

/// A DICOM attribute that has a dedicated indexed column in one of the three
/// read-side tables. Attributes not listed here fall back to the `attributes`
/// JSON blob on the `instances` table.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AttributeMapping {
    pub tag: Tag,
    pub table: TableId,
    /// Column name in the table (also the name returned by `sqlx` after SELECT).
    pub column: &'static str,
    pub vr_class: VrClass,
}

/// All mapped attributes in a stable iteration order.
///
/// The order here determines the order columns are selected in every query and
/// the order rows are materialised in [`crate::project`]. Keep it grouped by
/// table to make the SELECT list readable.
pub(crate) static ATTRIBUTE_MAPPINGS: &[AttributeMapping] = &[
    // ── Studies ───────────────────────────────────────────────────────────────
    AttributeMapping {
        tag: tags::STUDY_INSTANCE_UID,
        table: TableId::Study,
        column: "study_instance_uid",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::PATIENT_ID,
        table: TableId::Study,
        column: "patient_id",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::PATIENT_NAME,
        table: TableId::Study,
        column: "patient_name",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::PATIENT_BIRTH_DATE,
        table: TableId::Study,
        column: "patient_birth_date",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::PATIENT_SEX,
        table: TableId::Study,
        column: "patient_sex",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::STUDY_DATE,
        table: TableId::Study,
        column: "study_date",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::STUDY_TIME,
        table: TableId::Study,
        column: "study_time",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::ACCESSION_NUMBER,
        table: TableId::Study,
        column: "accession_number",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::STUDY_ID,
        table: TableId::Study,
        column: "study_id",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::STUDY_DESCRIPTION,
        table: TableId::Study,
        column: "study_description",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::REFERRING_PHYSICIAN_NAME,
        table: TableId::Study,
        column: "referring_physician_name",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::NUMBER_OF_STUDY_RELATED_SERIES,
        table: TableId::Study,
        column: "number_of_study_related_series",
        vr_class: VrClass::Integer,
    },
    AttributeMapping {
        tag: tags::NUMBER_OF_STUDY_RELATED_INSTANCES,
        table: TableId::Study,
        column: "number_of_study_related_instances",
        vr_class: VrClass::Integer,
    },
    // ── Series ────────────────────────────────────────────────────────────────
    AttributeMapping {
        tag: tags::SERIES_INSTANCE_UID,
        table: TableId::Series,
        column: "series_instance_uid",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::MODALITY,
        table: TableId::Series,
        column: "modality",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::SERIES_NUMBER,
        table: TableId::Series,
        column: "series_number",
        vr_class: VrClass::Integer,
    },
    AttributeMapping {
        tag: tags::SERIES_DATE,
        table: TableId::Series,
        column: "series_date",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::SERIES_TIME,
        table: TableId::Series,
        column: "series_time",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::SERIES_DESCRIPTION,
        table: TableId::Series,
        column: "series_description",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::BODY_PART_EXAMINED,
        table: TableId::Series,
        column: "body_part_examined",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::NUMBER_OF_SERIES_RELATED_INSTANCES,
        table: TableId::Series,
        column: "number_of_series_related_instances",
        vr_class: VrClass::Integer,
    },
    // ── Instances ─────────────────────────────────────────────────────────────
    AttributeMapping {
        tag: tags::SOP_INSTANCE_UID,
        table: TableId::Instance,
        column: "sop_instance_uid",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::SOP_CLASS_UID,
        table: TableId::Instance,
        column: "sop_class_uid",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::INSTANCE_NUMBER,
        table: TableId::Instance,
        column: "instance_number",
        vr_class: VrClass::Integer,
    },
    AttributeMapping {
        tag: tags::ACQUISITION_DATE_TIME,
        table: TableId::Instance,
        column: "acquisition_date_time",
        vr_class: VrClass::Text,
    },
    AttributeMapping {
        tag: tags::TRANSFER_SYNTAX_UID,
        table: TableId::Instance,
        column: "transfer_syntax_uid",
        vr_class: VrClass::Text,
    },
];
