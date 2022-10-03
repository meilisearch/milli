use std::collections::HashMap;

use crate::{update::index_documents::MergeFn, Result};
use grenad::Sorter;

pub struct WordPairProximityDocidsExtractor<'out> {
    docid: u32,
    key_buffer: Vec<u8>,
    value_buffer: Vec<u8>,
    sorter: &'out mut Sorter<MergeFn>,
    // (word1, position) followed by (word2, position)
    window: Vec<(Vec<u8>, u32)>,
    // key is `word1 \0 word2 \0` as bytes, value is their proximity
    batch: HashMap<Vec<u8>, u8>,
}
impl<'out> WordPairProximityDocidsExtractor<'out> {
    pub fn new(docid: u32, sorter: &'out mut Sorter<MergeFn>) -> Self {
        Self {
            docid,
            key_buffer: vec![],
            value_buffer: vec![],
            sorter,
            window: vec![],
            batch: HashMap::default(), // TODO: use better hash function
        }
    }

    pub fn extract_from_token_and_position(&mut self, token: &[u8], position: u32) -> Result<()> {
        loop {
            if let Some((word1, pos1)) = self.window.first() {
                if position - pos1 <= 7 {
                    self.window.push((token.to_owned(), position));
                    return Ok(());
                } else {
                    let mut key = vec![];
                    // for each word1, word2 pair, add it to the hashmap
                    // then dequeue the word1
                    for (word2, pos2) in self.window.iter().skip(1) {
                        key.extend_from_slice(word1);
                        key.push(0);
                        key.extend_from_slice(word2);
                        key.push(0);
                        let distance = pos2 - pos1;
                        let prox = self.batch.entry(key.clone()).or_insert(u8::MAX);
                        *prox = std::cmp::min(*prox, distance as u8);

                        key.extend_from_slice(word2);
                        key.push(0);
                        key.extend_from_slice(word1);
                        key.push(0);
                        let distance = pos2 - pos1 + 1;
                        let prox = self.batch.entry(key.clone()).or_insert(u8::MAX);
                        *prox = std::cmp::min(*prox, distance as u8);
                    }
                    self.window.remove(0);
                }
            } else {
                self.window.push((token.to_owned(), position));
                return Ok(());
            }
        }
    }

    fn finish_docid(&mut self) -> Result<()> {
        let mut key_buffer = vec![];
        for (key, prox) in self.batch.iter() {
            key_buffer.clear();
            key_buffer.extend_from_slice(key);
            key_buffer.push(*prox);
            self.sorter.insert(&key_buffer, self.docid.to_ne_bytes())?;
        }
        Ok(())
    }
}
