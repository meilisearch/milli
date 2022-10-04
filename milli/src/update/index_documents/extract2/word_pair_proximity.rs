use std::collections::{HashMap, VecDeque};

use crate::{update::index_documents::MergeFn, Result};
use grenad::Sorter;

pub struct WordPairProximityDocidsExtractor<'out> {
    docid: u32,
    sorter: &'out mut Sorter<MergeFn>,
    // (word1, position) followed by (word2, position)
    window: VecDeque<(Vec<u8>, u32)>,
    // key is `word1 \0 word2 \0` as bytes, value is their proximity
    batch: HashMap<Vec<u8>, u8, fxhash::FxBuildHasher>,
}
impl<'out> WordPairProximityDocidsExtractor<'out> {
    pub fn new(docid: u32, sorter: &'out mut Sorter<MergeFn>) -> Self {
        Self {
            docid,
            sorter,
            window: VecDeque::new(),
            batch: HashMap::default(), // TODO: use better hash function
        }
    }

    pub fn extract_from_token_and_position(&mut self, token: &[u8], position: u32) -> Result<()> {
        loop {
            if let Some((word1, pos1)) = self.window.front() {
                if position - pos1 <= 7 {
                    self.window.push_back((token.to_owned(), position));
                    return Ok(());
                } else {
                    let mut key = vec![];
                    // for each word1, word2 pair, add it to the hashmap
                    // then dequeue the word1
                    for (word2, pos2) in self.window.iter().skip(1) {
                        insert_in_batch(&word1, &word2, *pos1, *pos2, &mut key, &mut self.batch);
                    }
                    self.window.pop_front();
                }
            } else {
                // let w = std::str::from_utf8(token).unwrap();
                // println!("push {w} at pos {position}");
                self.window.push_back((token.to_owned(), position));
                return Ok(());
            }
        }
    }

    pub fn finish_docid(&mut self) -> Result<()> {
        while let Some((word1, pos1)) = self.window.front() {
            let mut key = vec![];
            // for each word1, word2 pair, add it to the hashmap
            // then dequeue the word1
            for (word2, pos2) in self.window.iter().skip(1) {
                insert_in_batch(&word1, &word2, *pos1, *pos2, &mut key, &mut self.batch);
            }
            self.window.pop_front();
        }
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
fn insert_in_batch(
    word1: &[u8],
    word2: &[u8],
    pos1: u32,
    pos2: u32,
    key: &mut Vec<u8>,
    batch: &mut HashMap<Vec<u8>, u8, fxhash::FxBuildHasher>,
) {
    key.clear();

    key.extend_from_slice(word1);
    key.push(0);
    key.extend_from_slice(word2);
    key.push(0);
    let distance = pos2 - pos1;
    let prox = batch.entry(key.clone()).or_insert(u8::MAX);
    *prox = std::cmp::min(*prox, distance as u8);
    assert!(*prox <= 7);
    key.clear();

    if *prox == 7 {
        return;
    }

    key.extend_from_slice(word2);
    key.push(0);
    key.extend_from_slice(word1);
    key.push(0);
    let distance = pos2 - pos1 + 1;
    let prox = batch.entry(key.clone()).or_insert(u8::MAX);
    *prox = std::cmp::min(*prox, distance as u8);
    assert!(*prox <= 7);
}
