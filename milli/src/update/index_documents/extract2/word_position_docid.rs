use crate::{update::index_documents::MergeFn, Result};
use grenad::Sorter;

pub struct WordPositionExtractor<'out> {
    docid: u32,
    key_buffer: Vec<u8>,
    sorter: &'out mut Sorter<MergeFn>,
}
impl<'out> WordPositionExtractor<'out> {
    pub fn new(docid: u32, sorter: &'out mut Sorter<MergeFn>) -> Self {
        Self { docid, key_buffer: vec![], sorter }
    }

    pub fn extract_from_token_and_position(&mut self, token: &[u8], position: u32) -> Result<()> {
        self.key_buffer.clear();
        self.key_buffer.extend_from_slice(token);
        self.key_buffer.extend_from_slice(&position.to_be_bytes());
        self.sorter.insert(&self.key_buffer, &self.docid.to_ne_bytes())?;
        Ok(())
    }
}
