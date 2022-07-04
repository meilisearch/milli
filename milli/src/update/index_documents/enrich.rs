use std::borrow::Cow;
use std::io::{Read, Seek};
use std::result::Result as StdResult;
use std::{fmt, iter};

use bumpalo::Bump;
use bumpalo_json::Value;
use serde::{Deserialize, Serialize};

use crate::documents::bumpalo_json::{self, Map};
use crate::documents::DocumentsBatchReader;
use crate::error::{GeoError, InternalError, UserError};
use crate::update::index_documents::enriched::EnrichedDocumentsBatchReader;
use crate::update::index_documents::writer_into_reader;
use crate::{Index, Result};

/// The symbol used to define levels in a nested primary key.
const PRIMARY_KEY_SPLIT_SYMBOL: char = '.';

/// The default primary that is used when not specified.
const DEFAULT_PRIMARY_KEY: &str = "id";

/// This function validates and enrich the documents by checking that:
///  - we can infer a primary key,
///  - all the documents id exist and are extracted,
///  - the validity of them but also,
///  - the validity of the `_geo` field depending on the settings.
pub fn enrich_documents_batch<R: Read + Seek>(
    rtxn: &heed::RoTxn,
    index: &Index,
    autogenerate_docids: bool,
    reader: DocumentsBatchReader<R>,
) -> Result<StdResult<EnrichedDocumentsBatchReader<R>, UserError>> {
    let mut cursor = reader.into_cursor();

    let mut external_ids = tempfile::tempfile().map(grenad::Writer::new)?;
    let mut uuid_buffer = [0; uuid::adapter::Hyphenated::LENGTH];

    // The primary key *field id* that has already been set for this index or the one
    // we will guess by searching for the first key that contains "id" as a substring.
    let primary_key = match index.primary_key(rtxn)? {
        Some(primary_key) if primary_key.contains(PRIMARY_KEY_SPLIT_SYMBOL) => {
            PrimaryKey::nested(primary_key)
        }
        Some(primary_key) => PrimaryKey::flat(primary_key),
        None => {
            // here we look at the first document to see if it has a primary key
            let mut primary_key_from_first_doc = None;
            if let Some(first_document) = cursor.next_document()? {
                for key in first_document.keys() {
                    if key.to_lowercase().contains(DEFAULT_PRIMARY_KEY) {
                        primary_key_from_first_doc = Some(key.clone());
                        break;
                    }
                }
            };
            if let Some(primary_key_from_first_doc) = primary_key_from_first_doc {
                PrimaryKey::flat_owned(primary_key_from_first_doc)
            } else if autogenerate_docids {
                PrimaryKey::flat(DEFAULT_PRIMARY_KEY)
            } else {
                return Ok(Err(UserError::MissingPrimaryKey));
            }
        }
    };
    cursor.reset();

    // If the settings specifies that a _geo field must be used therefore we must check the
    // validity of it in all the documents of this batch
    let look_for_geo_field = index.sortable_fields(rtxn)?.contains("_geo");
    let mut count = 0;

    let mut bump = Bump::new();
    loop {
        bump.reset();
        let document = if let Some(document) = cursor.next_bump_document(&bump)? {
            document
        } else {
            break;
        };
        let document: &_ = bump.alloc(document);

        let document_id = match fetch_or_generate_document_id(
            &document,
            &primary_key,
            autogenerate_docids,
            &mut uuid_buffer,
            count,
        )? {
            Ok(document_id) => document_id,
            Err(user_error) => return Ok(Err(user_error)),
        };

        if look_for_geo_field {
            if let Some(geo_value) = document.get("_geo") {
                if let Err(user_error) = validate_geo_from_json(&document_id, geo_value)? {
                    return Ok(Err(UserError::from(user_error)));
                }
            }
        }

        let document_id = serde_json::to_vec(&document_id).map_err(InternalError::SerdeJson)?;
        external_ids.insert(count.to_be_bytes(), document_id)?;
        count += 1;
    }

    let external_ids = writer_into_reader(external_ids)?;
    let reader = EnrichedDocumentsBatchReader::new(
        cursor.into_reader(),
        primary_key.name().to_string(),
        external_ids,
    )?;

    Ok(Ok(reader))
}

/// Retrieve the document id after validating it, returning a `UserError`
/// if the id is invalid or can't be guessed.
fn fetch_or_generate_document_id<'bump>(
    document: &'bump Map<'bump>,
    primary_key: &PrimaryKey,
    autogenerate_docids: bool,
    uuid_buffer: &mut [u8; uuid::adapter::Hyphenated::LENGTH],
    count: u32,
) -> Result<StdResult<DocumentId, UserError>> {
    match primary_key {
        PrimaryKey::Flat { name: primary_key } => match document.get(primary_key.as_ref()) {
            Some(document_id_value) => match validate_document_id_value(document_id_value)? {
                Ok(document_id) => Ok(Ok(DocumentId::retrieved(document_id))),
                Err(user_error) => Ok(Err(user_error)),
            },
            None if autogenerate_docids => {
                let uuid = uuid::Uuid::new_v4().to_hyphenated().encode_lower(uuid_buffer);
                Ok(Ok(DocumentId::generated(uuid.to_string(), count)))
            }
            None => Ok(Err(UserError::MissingDocumentId {
                primary_key: primary_key.to_string(),
                document: document.into(),
            })),
        },
        nested @ PrimaryKey::Nested { .. } => {
            let mut matching_documents_ids = Vec::new();
            for (first_level_name, right) in nested.possible_level_names() {
                if let Some(sub_value) = document.get(first_level_name) {
                    fetch_matching_values(sub_value, right, &mut matching_documents_ids);
                }
                if matching_documents_ids.len() >= 2 {
                    return Ok(Err(UserError::TooManyDocumentIds {
                        primary_key: nested.name().to_string(),
                        document: document.into(),
                    }));
                }
            }

            match matching_documents_ids.pop() {
                Some(document_id) => match validate_document_id_value(&document_id)? {
                    Ok(document_id) => Ok(Ok(DocumentId::retrieved(document_id))),
                    Err(user_error) => Ok(Err(user_error)),
                },
                None => Ok(Err(UserError::MissingDocumentId {
                    primary_key: nested.name().to_string(),
                    document: document.into(),
                })),
            }
        }
    }
}

/// A type that represent the type of primary key that has been set
/// for this index, a classic flat one or a nested one.
#[derive(Debug, Clone)]
enum PrimaryKey<'a> {
    Flat { name: Cow<'a, str> },
    Nested { name: Cow<'a, str> },
}

impl PrimaryKey<'_> {
    fn flat(name: &str) -> PrimaryKey {
        PrimaryKey::Flat { name: Cow::Borrowed(name) }
    }
    fn flat_owned<'a>(name: String) -> PrimaryKey<'a> {
        PrimaryKey::Flat { name: Cow::Owned(name) }
    }

    fn nested(name: &str) -> PrimaryKey {
        PrimaryKey::Nested { name: Cow::Borrowed(name) }
    }

    fn name(&self) -> &str {
        match self {
            PrimaryKey::Flat { name, .. } => name,
            PrimaryKey::Nested { name } => name,
        }
    }

    /// Returns an `Iterator` that gives all the possible fields names the primary key
    /// can have depending of the first level name and deepnes of the objects.
    fn possible_level_names(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        let name = self.name();
        iter::successors(Some((name, "")), |(curr, _)| curr.rsplit_once(PRIMARY_KEY_SPLIT_SYMBOL))
    }
}

/// A type that represents a document id that has been retrieved from a document or auto-generated.
///
/// In case the document id has been auto-generated, the document nth is kept to help
/// users debug if there is an issue with the document itself.
#[derive(Serialize, Deserialize, Clone)]
pub enum DocumentId {
    Retrieved { value: String },
    Generated { value: String, document_nth: u32 },
}

impl DocumentId {
    fn retrieved(value: String) -> DocumentId {
        DocumentId::Retrieved { value }
    }

    fn generated(value: String, document_nth: u32) -> DocumentId {
        DocumentId::Generated { value, document_nth }
    }

    fn debug(&self) -> String {
        format!("{:?}", self)
    }

    pub fn is_generated(&self) -> bool {
        matches!(self, DocumentId::Generated { .. })
    }

    pub fn value(&self) -> &str {
        match self {
            DocumentId::Retrieved { value } => value,
            DocumentId::Generated { value, .. } => value,
        }
    }
}

impl fmt::Debug for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocumentId::Retrieved { value } => write!(f, "{:?}", value),
            DocumentId::Generated { value, document_nth } => {
                write!(f, "{{{:?}}} of the {}nth document", value, document_nth)
            }
        }
    }
}

fn contained_in(selector: &str, key: &str) -> bool {
    selector.starts_with(key)
        && selector[key.len()..]
            .chars()
            .next()
            .map(|c| c == PRIMARY_KEY_SPLIT_SYMBOL)
            .unwrap_or(true)
}

pub fn fetch_matching_values<'bump>(
    value: &'bump Value<'bump>,
    selector: &str,
    output: &mut Vec<&'bump Value<'bump>>,
) {
    match value {
        Value::Map(object) => fetch_matching_values_in_object(object, selector, "", output),
        otherwise => output.push(otherwise),
    }
}

pub fn fetch_matching_values_in_object<'bump>(
    object: &'bump Map<'bump>,
    selector: &str,
    base_key: &str,
    output: &mut Vec<&'bump Value<'bump>>,
) {
    for (key, value) in object.0.iter() {
        let base_key = if base_key.is_empty() {
            key.to_string()
        } else {
            format!("{}{}{}", base_key, PRIMARY_KEY_SPLIT_SYMBOL, key)
        };

        // here if the user only specified `doggo` we need to iterate in all the fields of `doggo`
        // so we check the contained_in on both side.
        let should_continue =
            contained_in(selector, &base_key) || contained_in(&base_key, selector);

        if should_continue {
            match value.as_ref() {
                Value::Map(object) => {
                    fetch_matching_values_in_object(object, selector, &base_key, output)
                }
                value => output.push(value),
            }
        }
    }
}

/// Returns a trimmed version of the document id or `None` if it is invalid.
pub fn validate_document_id(document_id: &str) -> Option<&str> {
    let document_id = document_id.trim();
    if !document_id.is_empty()
        && document_id.chars().all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_'))
    {
        Some(document_id)
    } else {
        None
    }
}

/// Parses a Json encoded document id and validate it, returning a user error when it is one.
pub fn validate_document_id_value<'bump>(
    document_id: &'bump bumpalo_json::Value<'bump>,
) -> Result<StdResult<String, UserError>> {
    match document_id {
        bumpalo_json::Value::String(string) => match validate_document_id(&string) {
            Some(s) if s.len() == string.len() => Ok(Ok(string.to_string())),
            Some(s) => Ok(Ok(s.to_string())),
            None => Ok(Err(UserError::InvalidDocumentId {
                document_id: serde_json::Value::String(string.to_string()),
            })),
        },
        bumpalo_json::Value::UnsignedInteger(number) => Ok(Ok(number.to_string())),
        bumpalo_json::Value::SignedInteger(number) => Ok(Ok(number.to_string())),
        document_id => Ok(Err(UserError::InvalidDocumentId { document_id: document_id.into() })),
    }
}

/// Try to extract an `f64` from a JSON `Value` and return the `Value`
/// in the `Err` variant if it failed.
pub fn extract_float_from_value<'bump>(
    value: &'bump Value<'bump>,
) -> StdResult<f64, serde_json::Value> {
    match value {
        Value::UnsignedInteger(ref n) => Ok(*n as f64),
        Value::SignedInteger(ref n) => Ok(*n as f64),
        Value::Float(ref n) => Ok(*n),
        Value::String(ref s) => s.parse::<f64>().map_err(|_| todo!()),
        value => Err(value.into()),
    }
}

pub fn validate_geo_from_json<'bump>(
    id: &DocumentId,
    geo_value: &'bump Value<'bump>,
) -> Result<StdResult<(), GeoError>> {
    use GeoError::*;
    let debug_id = || serde_json::Value::from(id.debug());
    match geo_value {
        Value::Map(object) => match (object.get("lat"), object.get("lng")) {
            (Some(lat), Some(lng)) => {
                match (extract_float_from_value(lat), extract_float_from_value(lng)) {
                    (Ok(_), Ok(_)) => Ok(Ok(())),
                    (Err(value), Ok(_)) => {
                        Ok(Err(BadLatitude { document_id: debug_id(), value: value.into() }))
                    }
                    (Ok(_), Err(value)) => {
                        Ok(Err(BadLongitude { document_id: debug_id(), value: value.into() }))
                    }
                    (Err(lat), Err(lng)) => Ok(Err(BadLatitudeAndLongitude {
                        document_id: debug_id(),
                        lat: lat.into(),
                        lng: lng.into(),
                    })),
                }
            }
            (None, Some(_)) => Ok(Err(MissingLatitude { document_id: debug_id() })),
            (Some(_), None) => Ok(Err(MissingLongitude { document_id: debug_id() })),
            (None, None) => Ok(Err(MissingLatitudeAndLongitude { document_id: debug_id() })),
        },
        value => Ok(Err(NotAnObject { document_id: debug_id(), value: value.into() })),
    }
}
