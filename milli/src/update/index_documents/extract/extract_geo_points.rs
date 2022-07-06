use std::fs::File;
use std::io;

use bumpalo::Bump;
use concat_arrays::concat_arrays;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::documents::bumpalo_json;
use crate::error::GeoError;
use crate::update::index_documents::extract_float_from_value;
use crate::{FieldId, InternalError, Result};

/// Extracts the geographical coordinates contained in each document under the `_geo` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the (latitude, longitude)
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
    let mut bump = Bump::new();
    while let Some((docid_bytes, value)) = cursor.move_on_next()? {
        bump.reset();
        let obkv = obkv::KvReader::new(value);
        // since we only needs the primary key when we throw an error we create this getter to
        // lazily get it when needed
        let document_id = || -> serde_json::Value {
            let document_id = obkv.get(primary_key_id).unwrap();
            let object: &_ =
                bump.alloc(bumpalo_json::deserialize_bincode_slice(document_id, &bump).unwrap());
            serde_json::Value::from(object)
        };

        // first we get the two fields
        let lat = obkv.get(lat_fid);
        let lng = obkv.get(lng_fid);

        if let Some((lat, lng)) = lat.zip(lng) {
            // then we extract the values
            let lat_value = bump.alloc(
                bumpalo_json::deserialize_bincode_slice(lat, &bump)
                    .map_err(InternalError::Bincode)?,
            );
            let lng_value = bump.alloc(
                bumpalo_json::deserialize_bincode_slice(lng, &bump)
                    .map_err(InternalError::Bincode)?,
            );

            let lat = extract_float_from_value(lat_value).map_err(|lat| GeoError::BadLatitude {
                document_id: document_id(),
                value: lat.into(),
            })?;

            let lng = extract_float_from_value(lng_value).map_err(|lng| {
                GeoError::BadLongitude { document_id: document_id(), value: lng.into() }
            })?;

            let bytes: [u8; 16] = concat_arrays![lat.to_ne_bytes(), lng.to_ne_bytes()];
            writer.insert(docid_bytes, bytes)?;
        } else if lat.is_none() && lng.is_some() {
            return Err(GeoError::MissingLatitude { document_id: document_id() })?;
        } else if lat.is_some() && lng.is_none() {
            return Err(GeoError::MissingLongitude { document_id: document_id() })?;
        }
    }

    Ok(writer_into_reader(writer)?)
}
