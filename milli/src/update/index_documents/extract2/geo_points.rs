use crate::{
    error::GeoError, update::index_documents::extract_finite_float_from_value, FieldId,
    InternalError, Result,
};
use concat_arrays::concat_arrays;
use obkv::KvReader;
use serde_json::Value;
use std::fs::File;

pub struct GeoPointsExtractor<'out> {
    docid: u32,
    primary_key_fid: u16,
    lat_fid: u16,
    lng_fid: u16,
    writer: &'out mut grenad::Writer<File>,
}
impl<'out> GeoPointsExtractor<'out> {
    pub fn new(
        docid: u32,
        primary_key_fid: u16,
        lat_fid: u16,
        lng_fid: u16,
        writer: &'out mut grenad::Writer<File>,
    ) -> Self {
        Self { docid, primary_key_fid, lat_fid, lng_fid, writer }
    }

    pub fn extract_from_obkv(&mut self, obkv: KvReader<FieldId>) -> Result<()> {
        // since we only needs the primary key when we throw an error we create this getter to
        // lazily get it when needed
        let document_id = || -> Value {
            let document_id = obkv.get(self.primary_key_fid).unwrap();
            serde_json::from_slice(document_id).unwrap()
        };

        // first we get the two fields
        let lat = obkv.get(self.lat_fid);
        let lng = obkv.get(self.lng_fid);

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

            let bytes: [u8; 16] = concat_arrays![lat.to_ne_bytes(), lng.to_ne_bytes()];
            self.writer.insert(self.docid.to_be_bytes(), bytes)?;
        } else if lat.is_none() && lng.is_some() {
            return Err(GeoError::MissingLatitude { document_id: document_id() })?;
        } else if lat.is_some() && lng.is_none() {
            return Err(GeoError::MissingLongitude { document_id: document_id() })?;
        }
        Ok(())
    }

    fn finish_docid(&mut self) {}
}

// To make sure we don't forget to call finish_docid?
impl<'out> Drop for GeoPointsExtractor<'out> {
    fn drop(&mut self) {
        self.finish_docid();
    }
}
