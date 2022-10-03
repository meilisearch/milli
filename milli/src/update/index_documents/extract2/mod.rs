use self::fid_word_count_docids::FidWordCountDocids;

use super::helpers::{
    concat_u32s_array, create_sorter, keep_first, keep_first_prefix_value_merge_roaring_bitmaps,
    sorter_into_reader, GrenadParameters, MergeableReader,
};
use super::{
    create_writer, merge_cbo_roaring_bitmaps, merge_roaring_bitmaps, writer_into_reader,
    ClonableMmap, MergeFn,
};
use crate::error::{InternalError, SerializationError};
use crate::{FieldId, Result};
use charabia::TokenizerBuilder;
use grenad::{Reader, SortAlgorithm, Sorter};
use obkv::KvReader;
use serde_json::Value;
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt::Write;
use std::fs::File;
use std::io::{Cursor, Read, Seek};
use std::{str, thread};

mod docid_word_positions;
mod facet_values;
mod fid_word_count_docids;
mod geo_points;
mod tokenize;
mod word_docids;
mod word_pair_proximity;
mod word_position_docid;

#[derive(Clone, Copy)]
pub struct Context<'a> {
    pub primary_key_fid: u16,
    pub geo_fields_ids: Option<(u16, u16)>,
    pub searchable_fields: &'a Option<HashSet<FieldId>>,
    pub faceted_fields: &'a HashSet<FieldId>,
    pub stop_words: Option<&'a fst::Set<&'a [u8]>>,
    pub max_positions_per_attributes: u32,
    pub exact_attributes: &'a HashSet<FieldId>,
    pub grenad_params: GrenadParameters,
}

pub struct ExtractingData {
    word_position_docids: Sorter<MergeFn>,
    word_pair_proximity_docids: Sorter<MergeFn>,
    docid_word_positions: Sorter<MergeFn>,
    word_docids: Sorter<MergeFn>,
    exact_word_docids: Sorter<MergeFn>,
    fid_word_count_docids: Sorter<MergeFn>,
    fid_docid_facet_exists: Sorter<MergeFn>,
    fid_docid_facet_numbers: Sorter<MergeFn>,
    fid_docid_facet_strings: Sorter<MergeFn>,
    facet_string_docids: Sorter<MergeFn>,
    facet_numbers_docids: Sorter<MergeFn>,
    geo_points: grenad::Writer<File>,
}

struct SkippingReaderCursor<R>
where
    R: Read + Seek,
{
    cursor: grenad::ReaderCursor<R>,
    skip: usize,
}
impl<R> SkippingReaderCursor<R>
where
    R: Read + Seek,
{
    fn new(cursor: grenad::ReaderCursor<R>, skip: usize) -> Self {
        Self { cursor, skip }
    }
    fn next(&mut self) -> Option<(&[u8], &[u8])> {
        for _ in 0..self.skip {
            let _ = self.cursor.move_on_next().unwrap()?;
        }
        self.cursor.move_on_next().unwrap()
    }
}

pub fn extract_data(
    max_memory: usize,
    num_threads: usize,
    flattened_documents: grenad::Reader<Cursor<ClonableMmap>>,
    ctx: Context<'_>,
) -> Result<Vec<ExtractedData>> {
    let mut cursor = flattened_documents.into_cursor()?;
    thread::scope(|s| {
        let cursors = (0..num_threads)
            .map(|_| {
                let skipping_cursor = SkippingReaderCursor::new(cursor.clone(), num_threads - 1);
                let _ = cursor.move_on_next();
                skipping_cursor
            })
            .collect::<Vec<_>>();

        let handles = cursors
            .into_iter()
            .map(|mut cursor| {
                let max_memory = max_memory / num_threads;
                // TODO: when an error is thrown, cancel everything
                s.spawn(move || {
                    let mut state = ExtractingData::new(max_memory)?;
                    loop {
                        if let Some((docid, obkv)) = cursor.next() {
                            state.extract_document(docid, obkv, ctx)?;
                        } else {
                            break;
                        };
                    }
                    let state = state.finish(ctx.grenad_params)?;
                    Ok(state)
                })
            })
            .collect::<Vec<_>>();

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

impl ExtractingData {
    pub fn new(max_memory: usize) -> Result<Self> {
        let word_docids = create_sorter(
            SortAlgorithm::Unstable,
            merge_roaring_bitmaps,
            grenad::CompressionType::None,
            None,
            None,
            Some(max_memory / 20),
        );
        let word_position_docids = create_sorter(
            SortAlgorithm::Unstable,
            merge_cbo_roaring_bitmaps,
            grenad::CompressionType::None,
            None,
            None,
            Some(max_memory / 20),
        );
        let word_pair_proximity_docids = create_sorter(
            SortAlgorithm::Unstable,
            merge_cbo_roaring_bitmaps,
            grenad::CompressionType::None,
            None,
            None,
            Some(max_memory / 2),
        );
        let docid_word_positions = create_sorter(
            grenad::SortAlgorithm::Stable,
            concat_u32s_array,
            grenad::CompressionType::None,
            None,
            None,
            Some(max_memory / 10),
        );
        // I used 14/20th of the memory so far
        let exact_word_docids = create_sorter(
            grenad::SortAlgorithm::Unstable,
            merge_roaring_bitmaps,
            grenad::CompressionType::None,
            None,
            None,
            Some(max_memory / 10),
        );
        // I used 16/20th of the memory so far
        let fid_word_count_docids = create_sorter(
            grenad::SortAlgorithm::Unstable,
            merge_cbo_roaring_bitmaps,
            grenad::CompressionType::None,
            None,
            None,
            Some(max_memory / 40),
        );
        // lost track of memory usage from here

        let fid_docid_facet_exists = create_sorter(
            grenad::SortAlgorithm::Stable,
            keep_first,
            grenad::CompressionType::None,
            None,
            None,
            // TODO: SorterPool so I don't have to specify an arbitrary amount of memory
            Some(max_memory / 40),
        );
        let fid_docid_facet_numbers = create_sorter(
            grenad::SortAlgorithm::Stable,
            keep_first,
            grenad::CompressionType::None,
            None,
            None,
            Some(max_memory / 40),
        );
        let fid_docid_facet_strings = create_sorter(
            grenad::SortAlgorithm::Stable,
            keep_first,
            grenad::CompressionType::None,
            None,
            None,
            Some(max_memory / 20),
        );
        let facet_string_docids = create_sorter(
            grenad::SortAlgorithm::Stable,
            keep_first_prefix_value_merge_roaring_bitmaps,
            grenad::CompressionType::None,
            None,
            None,
            Some(max_memory / 20),
        );
        // 19/20
        let facet_numbers_docids = create_sorter(
            grenad::SortAlgorithm::Unstable,
            merge_cbo_roaring_bitmaps,
            grenad::CompressionType::None,
            None,
            None,
            Some(max_memory / 20),
        );
        // 20/20 (I am not sure that's correct anymore, it might be more)
        let geo_points = create_writer(grenad::CompressionType::None, None, tempfile::tempfile()?);

        Ok(Self {
            word_position_docids,
            word_pair_proximity_docids,
            docid_word_positions,
            word_docids,
            exact_word_docids,
            fid_word_count_docids,
            fid_docid_facet_exists,
            fid_docid_facet_numbers,
            fid_docid_facet_strings,
            facet_string_docids,
            facet_numbers_docids,
            geo_points,
        })
    }

    /// ## Arguments
    /// - `docid`: the document id, a big-endian encoded u32
    /// - `obkv`: the content of the document encoded as object key-values
    /// - `ctx`: context needed to extract the document (e.g. milli settings)
    fn extract_document(&mut self, docid: &[u8], obkv: &[u8], ctx: Context<'_>) -> Result<()> {
        let mut key_buffer = Vec::new(); // this could be self.key_buffer instead
        let mut field_buffer = String::new(); // this could be self.field_buffer instead

        let Self {
            exact_word_docids,
            word_docids,
            word_position_docids,
            word_pair_proximity_docids,
            docid_word_positions,
            fid_word_count_docids,
            fid_docid_facet_exists,
            fid_docid_facet_numbers,
            fid_docid_facet_strings,
            facet_string_docids,
            facet_numbers_docids,
            geo_points,
        } = self;

        let mut tokenizer = TokenizerBuilder::new();
        if let Some(stop_words) = ctx.stop_words {
            tokenizer.stop_words(stop_words);
        }
        let tokenizer = tokenizer.build();

        let docid = docid
            .try_into()
            .map(u32::from_be_bytes)
            .map_err(|_| SerializationError::InvalidNumberSerialization)?;
        let obkv = KvReader::<FieldId>::new(obkv);
        let mut word_pair_proximity_extractor =
            word_pair_proximity::WordPairProximityDocidsExtractor::new(
                docid,
                word_pair_proximity_docids,
            );
        let mut word_position_extractor =
            word_position_docid::WordPositionExtractor::new(docid, word_position_docids);
        let mut docid_word_positions_extractor =
            docid_word_positions::DocidWordPositionsExtractor::new(docid, docid_word_positions);
        let mut word_docids_extractor =
            word_docids::WordDocidsExtractor::new(docid, word_docids, exact_word_docids)?;
        key_buffer.extend_from_slice(&docid.to_be_bytes());
        let mut fid_docid_facet_values_extractor = facet_values::FidDocIdFacetValuesExtractor::new(
            docid,
            fid_docid_facet_numbers,
            fid_docid_facet_strings,
            fid_docid_facet_exists,
            facet_string_docids,
            facet_numbers_docids,
        );
        let mut fid_word_count_docids_extractor =
            FidWordCountDocids::new(docid, fid_word_count_docids);

        if let Some((lat_fid, lng_fid)) = ctx.geo_fields_ids {
            let mut extractor = geo_points::GeoPointsExtractor::new(
                docid,
                ctx.primary_key_fid,
                lat_fid,
                lng_fid,
                geo_points,
            );
            extractor.extract_from_obkv(obkv)?;
        }

        'field_ids: for (field_id, field_bytes) in obkv.iter() {
            let is_searchable =
                ctx.searchable_fields.as_ref().map_or(true, |sf| sf.contains(&field_id));
            let is_faceted = ctx.faceted_fields.contains(&field_id);
            if !is_searchable && !is_faceted {
                continue 'field_ids;
            }

            let value = serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)?;
            field_buffer.clear();

            if is_searchable {
                if let Some(field) = json_to_string(&value, &mut field_buffer) {
                    let mut word_count = 0;
                    tokenize::tokenize(&ctx, field_id, field, &tokenizer, |position, token| {
                        word_position_extractor.extract_from_token_and_position(token, position)?;
                        docid_word_positions_extractor
                            .extract_from_token_and_position(token, position)?;
                        word_pair_proximity_extractor
                            .extract_from_token_and_position(token, position)?;
                        word_count = position + 1;
                        Ok(())
                    })?;

                    docid_word_positions_extractor.finish_fid()?;
                    word_docids_extractor.extract_from_docid_word_positions_extractor(
                        field_id,
                        &docid_word_positions_extractor,
                        ctx,
                    )?;
                    fid_word_count_docids_extractor
                        .extract_from_fid_and_word_count(field_id, word_count)?;
                }
            }
            if is_faceted {
                fid_docid_facet_values_extractor.extract_from_field_id(field_id, value)?;
            }
        }
        word_pair_proximity_extractor.finish_docid()?;
        Ok(())
    }
}

/// Transform a JSON value into a string that can be indexed.
fn json_to_string<'a>(value: &'a Value, buffer: &'a mut String) -> Option<&'a str> {
    fn inner(value: &Value, output: &mut String) -> bool {
        match value {
            Value::Null => false,
            Value::Bool(boolean) => write!(output, "{}", boolean).is_ok(),
            Value::Number(number) => write!(output, "{}", number).is_ok(),
            Value::String(string) => write!(output, "{}", string).is_ok(),
            Value::Array(array) => {
                let mut count = 0;
                for value in array {
                    if inner(value, output) {
                        output.push_str(". ");
                        count += 1;
                    }
                }
                // check that at least one value was written
                count != 0
            }
            Value::Object(object) => {
                let mut buffer = String::new();
                let mut count = 0;
                for (key, value) in object {
                    buffer.clear();
                    let _ = write!(&mut buffer, "{}: ", key);
                    if inner(value, &mut buffer) {
                        buffer.push_str(". ");
                        // We write the "key: value. " pair only when
                        // we are sure that the value can be written.
                        output.push_str(&buffer);
                        count += 1;
                    }
                }
                // check that at least one value was written
                count != 0
            }
        }
    }

    if let Value::String(string) = value {
        Some(&string)
    } else if inner(value, buffer) {
        Some(buffer)
    } else {
        None
    }
}

pub struct ExtractedData {
    pub word_position_docids: Reader<File>,
    pub word_pair_proximity_docids: Reader<File>,
    pub docid_word_positions: Reader<File>,
    pub word_docids: Reader<File>,
    pub exact_word_docids: Reader<File>,
    pub fid_word_count_docids: Reader<File>,
    pub fid_docid_facet_exists: Reader<File>,
    pub fid_docid_facet_numbers: Reader<File>,
    pub fid_docid_facet_strings: Reader<File>,
    pub facet_string_docids: Reader<File>,
    pub facet_numbers_docids: Reader<File>,
    pub geo_points: Reader<File>,
}
impl ExtractingData {
    fn finish(self, indexer: GrenadParameters) -> Result<ExtractedData> {
        Ok(ExtractedData {
            word_position_docids: sorter_into_reader(self.word_position_docids, indexer)?,
            word_pair_proximity_docids: sorter_into_reader(
                self.word_pair_proximity_docids,
                indexer,
            )?,
            docid_word_positions: sorter_into_reader(self.docid_word_positions, indexer)?,
            word_docids: sorter_into_reader(self.word_docids, indexer)?,
            exact_word_docids: sorter_into_reader(self.exact_word_docids, indexer)?,
            fid_word_count_docids: sorter_into_reader(self.fid_word_count_docids, indexer)?,
            fid_docid_facet_exists: sorter_into_reader(self.fid_docid_facet_exists, indexer)?,
            fid_docid_facet_numbers: sorter_into_reader(self.fid_docid_facet_numbers, indexer)?,
            fid_docid_facet_strings: sorter_into_reader(self.fid_docid_facet_strings, indexer)?,
            facet_string_docids: sorter_into_reader(self.facet_string_docids, indexer)?,
            facet_numbers_docids: sorter_into_reader(self.facet_numbers_docids, indexer)?,
            geo_points: writer_into_reader(self.geo_points)?,
        })
    }
}

pub struct MergedExtractedData {
    pub word_position_docids: Reader<File>,
    pub word_pair_proximity_docids: Reader<File>,
    pub docid_word_positions: Reader<File>,
    pub word_docids: Reader<File>,
    pub exact_word_docids: Reader<File>,
    pub fid_word_count_docids: Reader<File>,
    pub fid_docid_facet_exists: Reader<File>,
    pub fid_docid_facet_numbers: Reader<File>,
    pub fid_docid_facet_strings: Reader<File>,
    pub facet_string_docids: Reader<File>,
    pub facet_numbers_docids: Reader<File>,
    pub geo_points: Reader<File>,
}
impl MergedExtractedData {
    pub fn new(extracted: Vec<ExtractedData>, indexer: GrenadParameters) -> Result<Self> {
        let mut word_position_docids = vec![];
        let mut word_pair_proximity_docids = vec![];
        let mut docid_word_positions = vec![];
        let mut word_docids = vec![];
        let mut exact_word_docids = vec![];
        let mut fid_word_count_docids = vec![];
        let mut fid_docid_facet_exists = vec![];
        let mut fid_docid_facet_numbers = vec![];
        let mut fid_docid_facet_strings = vec![];
        let mut facet_string_docids = vec![];
        let mut facet_numbers_docids = vec![];
        let mut geo_points = vec![];

        for data in extracted {
            word_position_docids.push(data.word_position_docids);
            word_pair_proximity_docids.push(data.word_pair_proximity_docids);
            docid_word_positions.push(data.docid_word_positions);
            word_docids.push(data.word_docids);
            exact_word_docids.push(data.exact_word_docids);
            fid_word_count_docids.push(data.fid_word_count_docids);
            fid_docid_facet_exists.push(data.fid_docid_facet_exists);
            fid_docid_facet_numbers.push(data.fid_docid_facet_numbers);
            fid_docid_facet_strings.push(data.fid_docid_facet_strings);
            facet_string_docids.push(data.facet_string_docids);
            facet_numbers_docids.push(data.facet_numbers_docids);
            geo_points.push(data.geo_points);
        }

        let word_position_docids = word_position_docids.merge(merge_roaring_bitmaps, &indexer)?;
        let word_pair_proximity_docids =
            word_pair_proximity_docids.merge(merge_cbo_roaring_bitmaps, &indexer)?;
        let docid_word_positions = docid_word_positions.merge(concat_u32s_array, &indexer)?;
        let word_docids = word_docids.merge(merge_roaring_bitmaps, &indexer)?;
        let exact_word_docids = exact_word_docids.merge(merge_cbo_roaring_bitmaps, &indexer)?;
        let fid_word_count_docids =
            fid_word_count_docids.merge(merge_cbo_roaring_bitmaps, &indexer)?;
        let fid_docid_facet_exists = fid_docid_facet_exists.merge(keep_first, &indexer)?;
        let fid_docid_facet_numbers = fid_docid_facet_numbers.merge(keep_first, &indexer)?;
        let fid_docid_facet_strings = fid_docid_facet_strings.merge(keep_first, &indexer)?;
        let facet_string_docids =
            facet_string_docids.merge(keep_first_prefix_value_merge_roaring_bitmaps, &indexer)?;
        let facet_numbers_docids =
            facet_numbers_docids.merge(merge_cbo_roaring_bitmaps, &indexer)?;
        let geo_points = geo_points.merge(keep_first, &indexer)?;

        Ok(Self {
            word_position_docids,
            word_pair_proximity_docids,
            docid_word_positions,
            word_docids,
            exact_word_docids,
            fid_word_count_docids,
            fid_docid_facet_exists,
            fid_docid_facet_numbers,
            fid_docid_facet_strings,
            facet_string_docids,
            facet_numbers_docids,
            geo_points,
        })
    }
}
