use std::collections::HashMap;

use crate::{update::index_documents::MergeFn, Result};
use grenad::Sorter;

pub struct DocidWordPositionsExtractor<'out> {
    docid: u32,
    key_buffer: Vec<u8>,
    value_buffer: Vec<u8>,
    pub word_positions: HashMap<Vec<u8>, Vec<u32>>,
    sorter: &'out mut Sorter<MergeFn>,
}
impl<'out> DocidWordPositionsExtractor<'out> {
    pub fn new(docid: u32, sorter: &'out mut Sorter<MergeFn>) -> Self {
        Self {
            docid,
            key_buffer: vec![],
            value_buffer: vec![],
            sorter,
            word_positions: HashMap::default(),
        }
    }

    pub fn enter_fid(&mut self, fid: u16) {}

    pub fn extract_from_token_and_position(&mut self, token: &[u8], position: u32) -> Result<()> {
        let positions = self.word_positions.entry(token.to_vec()).or_default();
        positions.push(position);
        Ok(())
    }
    pub fn finish_fid(&mut self) -> crate::Result<()> {
        let Self { docid, key_buffer, value_buffer, word_positions, sorter } = self;

        key_buffer.clear();
        key_buffer.extend_from_slice(&docid.to_ne_bytes());

        for (word, positions) in word_positions.iter() {
            value_buffer.clear();

            key_buffer.truncate(std::mem::size_of::<u32>());
            key_buffer.extend_from_slice(word.as_slice());
            for pos in positions {
                value_buffer.extend_from_slice(&pos.to_ne_bytes());
            }
            sorter.insert(&key_buffer, &value_buffer)?;
        }
        word_positions.clear();
        Ok(())
    }

    fn finish_docid(&mut self) {}
}

// To make sure we don't forget to call finish_docid?
impl<'out> Drop for DocidWordPositionsExtractor<'out> {
    fn drop(&mut self) {
        self.finish_docid();
    }
}
