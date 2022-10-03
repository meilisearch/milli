use crate::{
    facet::value_encoding::f64_into_bytes,
    heed_codec::facet::{encode_prefix_string, FacetLevelValueF64Codec, FacetStringLevelZeroCodec},
    update::index_documents::MergeFn,
    Result,
};
use grenad::Sorter;
use heed::{zerocopy::AsBytes, BytesEncode};
use roaring::RoaringBitmap;
use serde_json::Value;
use std::iter::FromIterator;
use std::mem;

pub struct FidDocIdFacetValuesExtractor<'out> {
    docid: u32,
    docid_value_buffer: Vec<u8>,
    fid_docid_key_buffer: Vec<u8>,
    fid_docid_numbers: &'out mut Sorter<MergeFn>,
    fid_docid_strings: &'out mut Sorter<MergeFn>,
    fid_docid_exists: &'out mut Sorter<MergeFn>,
    string_docids: &'out mut Sorter<MergeFn>,
    numbers_docids: &'out mut Sorter<MergeFn>,
}
impl<'out> FidDocIdFacetValuesExtractor<'out> {
    pub fn new(
        docid: u32,
        fid_docid_numbers: &'out mut Sorter<MergeFn>,
        fid_docid_strings: &'out mut Sorter<MergeFn>,
        fid_docid_exists: &'out mut Sorter<MergeFn>,
        string_docids: &'out mut Sorter<MergeFn>,
        numbers_docids: &'out mut Sorter<MergeFn>,
    ) -> Self {
        let mut docid_value_buffer = vec![];
        docid_value_buffer.extend_from_slice(&docid.to_be_bytes());
        Self {
            docid,
            docid_value_buffer,
            fid_docid_key_buffer: vec![],

            fid_docid_numbers,
            fid_docid_strings,
            fid_docid_exists,
            string_docids,
            numbers_docids,
        }
    }

    pub fn enter_fid(&mut self, fid: u16) {}

    pub fn extract_from_field_id(&mut self, fid: u16, value: serde_json::Value) -> Result<()> {
        // self.key_buffer availabsle
        // let mut value_buffer = vec![];
        self.fid_docid_key_buffer.clear();

        self.fid_docid_key_buffer.extend(&fid.to_be_bytes());

        // EXISTS:
        // key: fid
        // value: CboRoaringBitmap of docids
        self.fid_docid_exists.insert(&self.fid_docid_key_buffer, &self.docid.to_ne_bytes())?;

        self.fid_docid_key_buffer.extend(&self.docid_value_buffer);

        let (numbers, strings) = extract_facet_values(&value);
        for number in numbers {
            self.fid_docid_key_buffer.truncate(mem::size_of::<u16>() + mem::size_of::<u32>());
            if let Some(value_bytes) = f64_into_bytes(number) {
                self.fid_docid_key_buffer.extend_from_slice(&value_bytes);
                self.fid_docid_key_buffer.extend_from_slice(&number.to_be_bytes());

                self.fid_docid_numbers.insert(&self.fid_docid_key_buffer, ().as_bytes())?;

                // FACET_NUMBERS_DOCIDS
                // key: field id, level 0, number, number
                // value: cboroaringbitmap of docids
                // TODO: buffer this, reuse fid, 0
                //       also reuse docid.to_ne_bytes buffer
                let key = (fid, 0, number, number);
                let key_bytes = FacetLevelValueF64Codec::bytes_encode(&key).unwrap();
                self.numbers_docids.insert(&key_bytes, &self.docid.to_ne_bytes())?;
            }
        }
        // insert  normalized and original facet string in sorter
        for (normalized, original) in strings.into_iter().filter(|(n, _)| !n.is_empty()) {
            self.fid_docid_key_buffer.truncate(mem::size_of::<u16>() + mem::size_of::<u32>());
            self.fid_docid_key_buffer.extend_from_slice(normalized.as_bytes());
            self.fid_docid_strings.insert(&self.fid_docid_key_buffer, original.as_bytes())?;

            // TODO: perf optimisations in this
            let mut key_buffer = vec![];
            FacetStringLevelZeroCodec::serialize_into(fid, normalized.as_str(), &mut key_buffer);
            let mut value_buffer = vec![];
            encode_prefix_string(original.as_str(), &mut value_buffer)?;
            let bitmap = RoaringBitmap::from_iter(Some(self.docid));
            bitmap.serialize_into(&mut value_buffer)?;
            self.string_docids.insert(&key_buffer, &value_buffer)?;
        }

        Ok(())
    }

    pub fn finish_fid(&mut self) {}

    fn finish_docid(&mut self) {}
}

// To make sure we don't forget to call finish_docid?
impl<'out> Drop for FidDocIdFacetValuesExtractor<'out> {
    fn drop(&mut self) {
        self.finish_docid();
    }
}

fn extract_facet_values(value: &Value) -> (Vec<f64>, Vec<(String, String)>) {
    fn inner_extract_facet_values(
        value: &Value,
        can_recurse: bool,
        output_numbers: &mut Vec<f64>,
        output_strings: &mut Vec<(String, String)>,
    ) {
        match value {
            Value::Null => (),
            Value::Bool(b) => output_strings.push((b.to_string(), b.to_string())),
            Value::Number(number) => {
                if let Some(float) = number.as_f64() {
                    output_numbers.push(float);
                }
            }
            Value::String(original) => {
                let normalized = original.trim().to_lowercase();
                output_strings.push((normalized, original.clone()));
            }
            Value::Array(values) => {
                if can_recurse {
                    for value in values {
                        inner_extract_facet_values(value, false, output_numbers, output_strings);
                    }
                }
            }
            Value::Object(_) => (),
        }
    }

    let mut facet_number_values = Vec::new();
    let mut facet_string_values = Vec::new();
    inner_extract_facet_values(value, true, &mut facet_number_values, &mut facet_string_values);

    (facet_number_values, facet_string_values)
}
