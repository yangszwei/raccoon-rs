use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_core::value::{C, DataSetSequence};
use dicom_core::{Length, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::{StandardDataDictionary, tags};
use dicom_object::mem::{InMemDicomObject, InMemElement};
use raccoon_contract_dicom::{SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};
use raccoon_service_query::{
    AttributePathSegment, AttributeValue, ProjectedAttribute, QueryMatch, ResponseValue,
};
use serde_json::Value;
use tracing::info_span;

use crate::{DicomWebUrlBase, RetrieveUrl};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetrieveUrlLevel {
    Study,
    Series,
    Instance,
}

pub(crate) fn query_page_json(
    items: Vec<QueryMatch>,
    url_base: Option<&DicomWebUrlBase>,
    retrieve_url_level: RetrieveUrlLevel,
) -> Value {
    let span = info_span!(
        "qido.response.serialize_json",
        qido.item_count = items.len()
    );
    let _guard = span.enter();
    let objects: Vec<_> = items
        .into_iter()
        .map(|item| query_match_object_with_retrieve_url(item, url_base, retrieve_url_level))
        .collect();
    dicom_json::to_value(objects).expect("QIDO-RS response converts to DICOM JSON")
}

fn query_match_object_with_retrieve_url(
    item: QueryMatch,
    url_base: Option<&DicomWebUrlBase>,
    retrieve_url_level: RetrieveUrlLevel,
) -> InMemDicomObject {
    let retrieve_url = url_base.and_then(|base| retrieve_url(base, retrieve_url_level, &item));
    let mut elements: Vec<_> = item
        .into_attributes()
        .into_iter()
        .filter_map(projected_attribute_element)
        .collect();

    if let Some(retrieve_url) = retrieve_url {
        elements.push(InMemElement::new(
            tags::RETRIEVE_URL,
            VR::UR,
            PrimitiveValue::from(retrieve_url.to_string()),
        ));
    }

    InMemDicomObject::from_element_iter(elements)
}

fn retrieve_url(
    base: &DicomWebUrlBase,
    level: RetrieveUrlLevel,
    item: &QueryMatch,
) -> Option<RetrieveUrl> {
    let uids = MatchUids::from_match(item);
    match level {
        RetrieveUrlLevel::Study => {
            let study = StudyInstanceUid::new(uids.study?).ok()?;
            Some(base.study_retrieve_url(&study))
        }
        RetrieveUrlLevel::Series => {
            let study = StudyInstanceUid::new(uids.study?).ok()?;
            let series = SeriesInstanceUid::new(uids.series?).ok()?;
            Some(base.series_retrieve_url(&study, &series))
        }
        RetrieveUrlLevel::Instance => {
            let study = StudyInstanceUid::new(uids.study?).ok()?;
            let series = SeriesInstanceUid::new(uids.series?).ok()?;
            let instance = SopInstanceUid::new(uids.instance?).ok()?;
            Some(base.instance_retrieve_url(&study, &series, &instance))
        }
    }
}

#[derive(Default)]
struct MatchUids<'a> {
    study: Option<&'a str>,
    series: Option<&'a str>,
    instance: Option<&'a str>,
}

impl<'a> MatchUids<'a> {
    fn from_match(item: &'a QueryMatch) -> Self {
        let mut uids = Self::default();
        for attr in item.attributes() {
            let Some(tag) = single_tag(attr) else {
                continue;
            };
            let Some(value) = text_value(attr) else {
                continue;
            };
            match tag {
                tags::STUDY_INSTANCE_UID => uids.study = Some(value),
                tags::SERIES_INSTANCE_UID => uids.series = Some(value),
                tags::SOP_INSTANCE_UID => uids.instance = Some(value),
                _ => {}
            }
        }
        uids
    }
}

fn projected_attribute_element(attr: ProjectedAttribute) -> Option<InMemElement> {
    let tag = single_tag(&attr)?;
    let vr = vr_for_tag(tag);
    match attr.value {
        ResponseValue::Absent => None,
        value => Some(InMemElement::new(tag, vr, dicom_value(value))),
    }
}

fn dicom_value(value: ResponseValue) -> dicom_core::DicomValue<InMemDicomObject> {
    match value {
        ResponseValue::Absent | ResponseValue::ZeroLength => PrimitiveValue::Empty.into(),
        ResponseValue::Present(AttributeValue::Text(value)) => PrimitiveValue::from(value).into(),
        ResponseValue::Present(AttributeValue::Texts(values)) => {
            PrimitiveValue::Strs(C::from(values)).into()
        }
        ResponseValue::Present(AttributeValue::Sequence(items)) => DataSetSequence::new(
            items
                .into_iter()
                .map(|item| {
                    query_match_object_with_retrieve_url(item, None, RetrieveUrlLevel::Study)
                })
                .collect::<Vec<_>>(),
            Length::UNDEFINED,
        )
        .into(),
        ResponseValue::Present(AttributeValue::Binary(value)) => PrimitiveValue::from(value).into(),
    }
}

fn single_tag(attr: &ProjectedAttribute) -> Option<Tag> {
    match attr.path.segments() {
        [AttributePathSegment::Tag(tag)] => Some(*tag),
        _ => None,
    }
}

fn text_value(attr: &ProjectedAttribute) -> Option<&str> {
    match &attr.value {
        ResponseValue::Present(AttributeValue::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn vr_for_tag(tag: Tag) -> VR {
    StandardDataDictionary
        .by_tag(tag)
        .and_then(|entry| entry.vr().exact())
        .unwrap_or(VR::LO)
}
