use std::fs::File;
use std::io;

use concat_arrays::concat_arrays;
use serde_json::Value;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::error::GeoError;
use crate::update::index_documents::extract_finite_float_from_value;
use crate::{FieldId, InternalError, Result};

/// Extracts the geographical coordinates contained in each document under the `_geo` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the (latitude, longitude)
#[logging_timer::time]
pub fn extract_geo_points<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    primary_key_id: FieldId,
    (lat_fid, lng_fid): (FieldId, FieldId),
) -> Result<grenad::Reader<File>> {
    let mut writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((docid_bytes, value)) = cursor.move_on_next()? {
        let obkv = obkv::KvReader::new(value);
        // since we only needs the primary key when we throw an error we create this getter to
        // lazily get it when needed
        let document_id = || -> Value {
            let document_id = obkv.get(primary_key_id).unwrap();
            serde_json::from_slice(document_id).unwrap()
        };

        // first we get the two fields
        let lat = obkv.get(lat_fid);
        let lng = obkv.get(lng_fid);

        if let Some((lat, lng)) = lat.zip(lng) {
            // then we extract the values
            let lat = extract_finite_float_from_value(
                serde_json::from_slice(lat).map_err(InternalError::SerdeJson)?,
            )
            .map_err(|lat| GeoError::BadLatitude { document_id: document_id(), value: lat })?;

            let lng = extract_finite_float_from_value(
                serde_json::from_slice(lng).map_err(InternalError::SerdeJson)?,
            )
            .map_err(|lng| GeoError::BadLongitude { document_id: document_id(), value: lng })?;

            #[allow(clippy::drop_non_drop)]
            let bytes: [u8; 16] = concat_arrays![lat.to_ne_bytes(), lng.to_ne_bytes()];
            writer.insert(docid_bytes, bytes)?;
        } else if lat.is_none() && lng.is_some() {
            return Err(GeoError::MissingLatitude { document_id: document_id() })?;
        } else if lat.is_some() && lng.is_none() {
            return Err(GeoError::MissingLongitude { document_id: document_id() })?;
        }
    }

    writer_into_reader(writer)
}
