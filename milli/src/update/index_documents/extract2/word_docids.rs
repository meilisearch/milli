use std::iter::FromIterator;

use crate::{
    update::index_documents::{helpers::serialize_roaring_bitmap, MergeFn},
    Result,
};
use grenad::Sorter;
use roaring::RoaringBitmap;

use super::{docid_word_positions::DocidWordPositionsExtractor, Context};

pub struct WordDocidsExtractor<'out> {
    value_buffer: Vec<u8>,
    word_docids_sorter: &'out mut Sorter<MergeFn>,
    exact_word_docids_sorter: &'out mut Sorter<MergeFn>,
}
impl<'out> WordDocidsExtractor<'out> {
    pub fn new(
        docid: u32,
        words_sorter: &'out mut Sorter<MergeFn>,
        exact_words_sorter: &'out mut Sorter<MergeFn>,
    ) -> Result<Self> {
        let mut value_buffer = vec![];
        let bitmap = RoaringBitmap::from_iter(Some(docid));
        serialize_roaring_bitmap(&bitmap, &mut value_buffer)?;

        Ok(Self {
            value_buffer,
            word_docids_sorter: words_sorter,
            exact_word_docids_sorter: exact_words_sorter,
        })
    }
    pub fn extract_from_docid_word_positions_extractor(
        &mut self,
        fid: u16,
        extractor: &DocidWordPositionsExtractor<'out>,
        ctx: Context,
    ) -> Result<()> {
        if ctx.exact_attributes.contains(&fid) {
            for word in extractor.word_positions.keys() {
                self.exact_word_docids_sorter.insert(word.as_slice(), &self.value_buffer)?;
            }
        } else {
            for word in extractor.word_positions.keys() {
                self.word_docids_sorter.insert(word.as_slice(), &self.value_buffer)?;
            }
        }
        Ok(())
    }
}
