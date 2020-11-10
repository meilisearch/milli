use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use heed::types::*;
use heed::{PolyDatabase, Database, RwTxn, RoTxn};
use log::debug;
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use crate::Search;
use crate::{BEU32, DocumentId};
use crate::fields_ids_map::FieldsIdsMap;
use crate::{
    RoaringBitmapCodec, BEU32StrCodec, StrStrU8Codec, ObkvCodec,
    BoRoaringBitmapCodec, CboRoaringBitmapCodec,
};

pub const DISPLAYED_FIELDS_KEY: &str = "displayed-fields";
pub const DOCUMENTS_IDS_KEY: &str = "documents-ids";
pub const FIELDS_IDS_MAP_KEY: &str = "fields-ids-map";
pub const PRIMARY_KEY_KEY: &str = "primary-key";
pub const SEARCHABLE_FIELDS_KEY: &str = "searchable-fields";
pub const USERS_IDS_DOCUMENTS_IDS_KEY: &str = "users-ids-documents-ids";
pub const WORDS_FST_KEY: &str = "words-fst";

#[derive(Clone)]
pub struct Index {
    /// The LMDB environment which this index is associated with.
    pub env: heed::Env,
    /// Contains many different types (e.g. the fields ids map).
    pub main: PolyDatabase,
    /// A word and all the documents ids containing the word.
    pub word_docids: Database<Str, RoaringBitmapCodec>,
    /// Maps a word and a document id (u32) to all the positions where the given word appears.
    pub docid_word_positions: Database<BEU32StrCodec, BoRoaringBitmapCodec>,
    /// Maps the proximity between a pair of words with all the docids where this relation appears.
    pub word_pair_proximity_docids: Database<StrStrU8Codec, CboRoaringBitmapCodec>,
    /// Maps the document id to the document as an obkv store.
    pub documents: Database<OwnedType<BEU32>, ObkvCodec>,
}

impl Index {
    pub fn new<P: AsRef<Path>>(mut options: heed::EnvOpenOptions, path: P) -> anyhow::Result<Index> {
        options.max_dbs(5);

        let env = options.open(path)?;
        let main = env.create_poly_database(Some("main"))?;
        let word_docids = env.create_database(Some("word-docids"))?;
        let docid_word_positions = env.create_database(Some("docid-word-positions"))?;
        let word_pair_proximity_docids = env.create_database(Some("word-pair-proximity-docids"))?;
        let documents = env.create_database(Some("documents"))?;

        Ok(Index { env, main, word_docids, docid_word_positions, word_pair_proximity_docids, documents })
    }

    /// Create a write transaction to be able to write into the index.
    pub fn write_txn(&self) -> heed::Result<RwTxn> {
        self.env.write_txn()
    }

    /// Create a read transaction to be able to read the index.
    pub fn read_txn(&self) -> heed::Result<RoTxn> {
        self.env.read_txn()
    }

    /// Returns the canonicalized path where the heed `Env` of this `Index` lives.
    pub fn path(&self) -> &Path {
        self.env.path()
    }

    /// Returns an `EnvClosingEvent` that can be used to wait for the closing event,
    /// multiple threads can wait on this event.
    ///
    /// Make sure that you drop all the copies of `Index`es you have, env closing are triggered
    /// when all references are dropped, the last one will eventually close the environment.
    pub fn prepare_for_closing(self) -> heed::EnvClosingEvent {
        self.env.prepare_for_closing()
    }

    /* documents ids */

    /// Writes the documents ids that corresponds to the user-ids-documents-ids FST.
    pub fn put_documents_ids(&self, wtxn: &mut RwTxn, docids: &RoaringBitmap) -> heed::Result<()> {
        self.main.put::<_, Str, RoaringBitmapCodec>(wtxn, DOCUMENTS_IDS_KEY, docids)
    }

    /// Returns the internal documents ids.
    pub fn documents_ids(&self, rtxn: &RoTxn) -> heed::Result<RoaringBitmap> {
        Ok(self.main.get::<_, Str, RoaringBitmapCodec>(rtxn, DOCUMENTS_IDS_KEY)?.unwrap_or_default())
    }

    /* primary key */

    /// Writes the documents primary key, this is the field name that is used to store the id.
    pub fn put_primary_key(&self, wtxn: &mut RwTxn, primary_key: u8) -> heed::Result<()> {
        self.main.put::<_, Str, OwnedType<u8>>(wtxn, PRIMARY_KEY_KEY, &primary_key)
    }

    /// Deletes the primary key of the documents, this can be done to reset indexes settings.
    pub fn delete_primary_key(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, PRIMARY_KEY_KEY)
    }

    /// Returns the documents primary key, `None` if it hasn't been defined.
    pub fn primary_key(&self, rtxn: &RoTxn) -> heed::Result<Option<u8>> {
        self.main.get::<_, Str, OwnedType<u8>>(rtxn, PRIMARY_KEY_KEY)
    }

    /* users ids documents ids */

    /// Writes the users ids documents ids, a user id is a byte slice (i.e. `[u8]`)
    /// and refers to an internal id (i.e. `u32`).
    pub fn put_users_ids_documents_ids<A: AsRef<[u8]>>(&self, wtxn: &mut RwTxn, fst: &fst::Map<A>) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, USERS_IDS_DOCUMENTS_IDS_KEY, fst.as_fst().as_bytes())
    }

    /// Returns the user ids documents ids map which associate the user ids (i.e. `[u8]`)
    /// with the internal ids (i.e. `u32`).
    pub fn users_ids_documents_ids<'t>(&self, rtxn: &'t RoTxn) -> anyhow::Result<fst::Map<Cow<'t, [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, USERS_IDS_DOCUMENTS_IDS_KEY)? {
            Some(bytes) => Ok(fst::Map::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Map::default().map_data(Cow::Owned)?),
        }
    }

    /* fields ids map */

    /// Writes the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub fn put_fields_ids_map(&self, wtxn: &mut RwTxn, map: &FieldsIdsMap) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<FieldsIdsMap>>(wtxn, FIELDS_IDS_MAP_KEY, map)
    }

    /// Returns the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub fn fields_ids_map(&self, rtxn: &RoTxn) -> heed::Result<FieldsIdsMap> {
        Ok(self.main.get::<_, Str, SerdeJson<FieldsIdsMap>>(rtxn, FIELDS_IDS_MAP_KEY)?.unwrap_or_default())
    }

    /* displayed fields */

    /// Writes the fields ids that must be displayed in the defined order.
    /// There must be not be any duplicate field id.
    pub fn put_displayed_fields(&self, wtxn: &mut RwTxn, fields: &[u8]) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, DISPLAYED_FIELDS_KEY, fields)
    }

    /// Deletes the displayed fields ids, this will make the engine to display
    /// all the documents attributes in the order of the `FieldsIdsMap`.
    pub fn delete_displayed_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, DISPLAYED_FIELDS_KEY)
    }

    /// Returns the displayed fields ids in the order they must be returned. If it returns
    /// `None` it means that all the attributes are displayed in the order of the `FieldsIdsMap`.
    pub fn displayed_fields<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<&'t [u8]>> {
        self.main.get::<_, Str, ByteSlice>(rtxn, DISPLAYED_FIELDS_KEY)
    }

    /* searchable fields */

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    pub fn put_searchable_fields(&self, wtxn: &mut RwTxn, fields: &[u8]) -> heed::Result<()> {
        assert!(fields.windows(2).all(|win| win[0] < win[1])); // is sorted
        self.main.put::<_, Str, ByteSlice>(wtxn, SEARCHABLE_FIELDS_KEY, fields)
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    pub fn delete_searchable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the searchable fields ids, those are the fields that are indexed,
    /// if the searchable fields aren't there it means that **all** the fields are indexed.
    pub fn searchable_fields<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<&'t [u8]>> {
        self.main.get::<_, Str, ByteSlice>(rtxn, SEARCHABLE_FIELDS_KEY)
    }

    /* words fst */

    /// Writes the FST which is the words dictionnary of the engine.
    pub fn put_words_fst<A: AsRef<[u8]>>(&self, wtxn: &mut RwTxn, fst: &fst::Set<A>) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, WORDS_FST_KEY, fst.as_fst().as_bytes())
    }

    /// Returns the FST which is the words dictionnary of the engine.
    pub fn words_fst<'t>(&self, rtxn: &'t RoTxn) -> anyhow::Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, WORDS_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /// Returns a [`Vec`] of the requested documents. Returns an error if a document is missing.
    pub fn documents<'t>(
        &self,
        rtxn: &'t RoTxn,
        ids: impl IntoIterator<Item=DocumentId>,
    ) -> anyhow::Result<Vec<(DocumentId, obkv::KvReader<'t>)>>
    {
        let mut documents = Vec::new();

        for id in ids {
            let kv = self.documents.get(rtxn, &BEU32::new(id))?
                .with_context(|| format!("Could not find document {}", id))?;
            documents.push((id, kv));
        }

        Ok(documents)
    }

    /// Returns a [`Vec`] of at most `limit` documents that are similar to the given document.
    ///
    /// Similarity is based on the TF-IDF of all the terms in the given document, each of
    /// these TF-IDFs are classified, we iterate througt them and retrieve the the documents
    /// associated with the term with the best TF-IDF.
    pub fn similar_documents<'t>(
        &self,
        rtxn: &'t RoTxn,
        id: DocumentId,
        limit: usize,
    ) -> anyhow::Result<Vec<DocumentId>>
    {
        let number_of_documents = self.number_of_documents(&rtxn)?;
        let number_of_documents = number_of_documents as f64;

        // We iterate over all of the document's words and compute the TF-IDF of each of
        // the found term. We then save the result in a collection to be able iterate over
        // the terms in the TF-IDF descending order (higher is better).
        let mut tf_idfs = BTreeMap::new();
        for result in self.docid_word_positions.prefix_iter(rtxn, &(id, ""))? {
            let ((_docid, word), positions) = result?;

            if let Some(docids) = self.word_docids.get(rtxn, word)? {
                let tf = positions.len() as f64;

                let total_freq = docids.len() as f64;
                let idf = (number_of_documents / total_freq).log2();
                let tf_idf = tf * idf;

                tf_idfs.insert(Reverse(OrderedFloat(tf_idf)), word);
            }
        }

        // Create an iterator that will be consumed by the next two iterations loops.
        let mut tf_idfs_iter = tf_idfs.into_iter();

        // We first accumulate enough documents to work with, this help in the case
        // the least common word does only appear in the given document.
        let mut base = RoaringBitmap::new();
        while let Some((_tf_idf, word)) = tf_idfs_iter.next() {
            debug!("TF-IDF filling with {:?}", word);
            let mut docids = self.word_docids.get(rtxn, word)?.unwrap_or_default();
            docids.remove(id);
            base.union_with(&docids);
            if base.len() >= limit as u64 { break }
        }

        // We take the least common words and try to intersect them the more possible until
        // you don't find enough of them to satisfy the limit. At this point you return the
        // current and previous intersections.
        let mut tmp = base;
        let mut previous_tmp = RoaringBitmap::new();
        while let Some((_tf_idf, word)) = tf_idfs_iter.next() {
            debug!("TF-IDF intersecting with {:?}", word);
            let mut docids = self.word_docids.get(rtxn, word)?.unwrap_or_default();
            docids.remove(id);
            previous_tmp = tmp.clone();
            tmp.intersect_with(&docids);
            if tmp.len() <= limit as u64 { break }
        }

        // We remove the tmp documents from the previous_tmp documents.
        previous_tmp.difference_with(&tmp);

        // We also make sure we do not return the original docids.
        let similar_documents = tmp.iter()
            .chain(previous_tmp)
            .take(limit)
            .collect();

        Ok(similar_documents)
    }

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents(&self, rtxn: &RoTxn) -> anyhow::Result<usize> {
        Ok(self.documents_ids(rtxn).map(|docids| docids.len() as usize)?)
    }

    pub fn search<'a>(&'a self, rtxn: &'a RoTxn) -> Search<'a> {
        Search::new(rtxn, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use heed::EnvOpenOptions;
    use crate::update::{IndexDocuments, UpdateFormat};

    #[test]
    fn similar_documents() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with an id for only one of them.
        let mut wtxn = index.write_txn().unwrap();
        let content = &br#"[
            { "id": 0, "text": "kevin is cool but friend with benoit" },
            { "id": 1, "text": "kevina is dumb" },
            { "id": 2, "text": "bernard is dumb" },
            { "id": 3, "text": "christophe is dumb" },
            { "id": 4, "text": "benoit is also dumb" },
            { "id": 5, "text": "jacques is friend with benoit" }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that there is 3 documents now.
        let rtxn = index.read_txn().unwrap();

        // "dumb" word in common
        let similar_ids = index.similar_documents(&rtxn, 1, 3 /* limit */).unwrap();
        assert_eq!(similar_ids, vec![2, 3, 4]); // they all contain "dumb"

        // "friend" and "benoit" in common
        let similar_ids = index.similar_documents(&rtxn, 4, 2 /* limit */).unwrap();
        assert_eq!(similar_ids, vec![0, 5]); // they all contain "friend" and "benoit"

        drop(rtxn);
    }
}
