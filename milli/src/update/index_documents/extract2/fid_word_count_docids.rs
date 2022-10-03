use crate::{update::index_documents::MergeFn, Result};
use grenad::Sorter;

pub struct FidWordCountDocids<'out> {
    docid: u32,
    key_buffer: Vec<u8>,
    sorter: &'out mut Sorter<MergeFn>,
}
impl<'out> FidWordCountDocids<'out> {
    pub fn new(docid: u32, sorter: &'out mut Sorter<MergeFn>) -> Self {
        Self { docid, key_buffer: vec![], sorter }
    }

    pub fn extract_from_fid_and_word_count(&mut self, fid: u16, word_count: u32) -> Result<()> {
        if word_count <= 10 {
            self.key_buffer.clear();
            self.key_buffer.extend_from_slice(&fid.to_be_bytes());
            self.key_buffer.push(word_count as u8);

            self.sorter.insert(&self.key_buffer, self.docid.to_ne_bytes())?;
        }
        Ok(())
    }
}
