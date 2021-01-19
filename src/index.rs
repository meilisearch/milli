use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;
use heed::types::*;
use heed::{PolyDatabase, Database, RwTxn, RoTxn};
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::fields_ids_map::FieldsIdsMap;
use crate::{default_criteria, Criterion, Search};
use crate::{BEU32, DocumentId, FieldId, ExternalDocumentsIds};
use crate::{
    RoaringBitmapCodec, BEU32StrCodec, StrStrU8Codec, ObkvCodec,
    BoRoaringBitmapCodec, CboRoaringBitmapCodec,
};

pub const CRITERIA_KEY: &str = "criteria";
pub const DISPLAYED_FIELDS_KEY: &str = "displayed-fields";
pub const DOCUMENTS_IDS_KEY: &str = "documents-ids";
pub const FACETED_DOCUMENTS_IDS_PREFIX: &str = "faceted-documents-ids";
pub const FACETED_FIELDS_KEY: &str = "faceted-fields";
pub const FIELDS_IDS_MAP_KEY: &str = "fields-ids-map";
pub const PRIMARY_KEY_KEY: &str = "primary-key";
pub const SEARCHABLE_FIELDS_KEY: &str = "searchable-fields";
pub const HARD_EXTERNAL_DOCUMENTS_IDS_KEY: &str = "hard-external-documents-ids";
pub const SOFT_EXTERNAL_DOCUMENTS_IDS_KEY: &str = "soft-external-documents-ids";
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
    /// Maps the facet field id and the globally ordered value with the docids that corresponds to it.
    pub facet_field_id_value_docids: Database<ByteSlice, CboRoaringBitmapCodec>,
    /// Maps the document id, the facet field id and the globally ordered value.
    pub field_id_docid_facet_values: Database<ByteSlice, Unit>,
    /// Maps the document id to the document as an obkv store.
    pub documents: Database<OwnedType<BEU32>, ObkvCodec>,
}

impl Index {
    pub fn new<P: AsRef<Path>>(mut options: heed::EnvOpenOptions, path: P) -> anyhow::Result<Index> {
        options.max_dbs(7);

        let env = options.open(path)?;
        let main = env.create_poly_database(Some("main"))?;
        let word_docids = env.create_database(Some("word-docids"))?;
        let docid_word_positions = env.create_database(Some("docid-word-positions"))?;
        let word_pair_proximity_docids = env.create_database(Some("word-pair-proximity-docids"))?;
        let facet_field_id_value_docids = env.create_database(Some("facet-field-id-value-docids"))?;
        let field_id_docid_facet_values = env.create_database(Some("field-id-docid-facet-values"))?;
        let documents = env.create_database(Some("documents"))?;

        Ok(Index {
            env,
            main,
            word_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            facet_field_id_value_docids,
            field_id_docid_facet_values,
            documents,
        })
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
    pub fn put_primary_key(&self, wtxn: &mut RwTxn, primary_key: &str) -> heed::Result<()> {
        self.main.put::<_, Str, Str>(wtxn, PRIMARY_KEY_KEY, &primary_key)
    }

    /// Deletes the primary key of the documents, this can be done to reset indexes settings.
    pub fn delete_primary_key(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, PRIMARY_KEY_KEY)
    }

    /// Returns the documents primary key, `None` if it hasn't been defined.
    pub fn primary_key<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<&'t str>> {
        self.main.get::<_, Str, Str>(rtxn, PRIMARY_KEY_KEY)
    }

    /* external documents ids */

    /// Writes the external documents ids and internal ids (i.e. `u32`).
    pub fn put_external_documents_ids<'a>(
        &self,
        wtxn: &mut RwTxn,
        external_documents_ids: &ExternalDocumentsIds<'a>,
    ) -> heed::Result<()>
    {
        let ExternalDocumentsIds { hard, soft } = external_documents_ids;
        let hard = hard.as_fst().as_bytes();
        let soft = soft.as_fst().as_bytes();
        self.main.put::<_, Str, ByteSlice>(wtxn, HARD_EXTERNAL_DOCUMENTS_IDS_KEY, hard)?;
        self.main.put::<_, Str, ByteSlice>(wtxn, SOFT_EXTERNAL_DOCUMENTS_IDS_KEY, soft)?;
        Ok(())
    }

    /// Returns the external documents ids map which associate the external ids
    /// with the internal ids (i.e. `u32`).
    pub fn external_documents_ids<'t>(&self, rtxn: &'t RoTxn) -> anyhow::Result<ExternalDocumentsIds<'t>> {
        let hard = self.main.get::<_, Str, ByteSlice>(rtxn, HARD_EXTERNAL_DOCUMENTS_IDS_KEY)?;
        let soft = self.main.get::<_, Str, ByteSlice>(rtxn, SOFT_EXTERNAL_DOCUMENTS_IDS_KEY)?;
        let hard = match hard {
            Some(hard) => fst::Map::new(hard)?.map_data(Cow::Borrowed)?,
            None => fst::Map::default().map_data(Cow::Owned)?,
        };
        let soft = match soft {
            Some(soft) => fst::Map::new(soft)?.map_data(Cow::Borrowed)?,
            None => fst::Map::default().map_data(Cow::Owned)?,
        };
        Ok(ExternalDocumentsIds::new(hard, soft))
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

    /// Writes the fields that must be displayed in the defined order.
    /// There must be not be any duplicate field id.
    pub fn put_displayed_fields(&self, wtxn: &mut RwTxn, fields: &[&str]) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<&[&str]>>(wtxn, DISPLAYED_FIELDS_KEY, &fields)
    }

    /// Deletes the displayed fields ids, this will make the engine to display
    /// all the documents attributes in the order of the `FieldsIdsMap`.
    pub fn delete_displayed_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, DISPLAYED_FIELDS_KEY)
    }

    /// Returns the displayed fields in the order they were set by the user. If it returns
    /// `None` it means that all the attributes are set as displayed in the order of the `FieldsIdsMap`.
    pub fn displayed_fields<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<Vec<&'t str>>> {
        self.main.get::<_, Str, SerdeBincode<Vec<&'t str>>>(rtxn, DISPLAYED_FIELDS_KEY)
    }

    /* searchable fields */

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    pub fn put_searchable_fields(&self, wtxn: &mut RwTxn, fields: &[&str]) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<&[&str]>>(wtxn, SEARCHABLE_FIELDS_KEY, &fields)
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    pub fn delete_searchable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the searchable fields, those are the fields that are indexed,
    /// if the searchable fields aren't there it means that **all** the fields are indexed.
    pub fn searchable_fields<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<Vec<&'t str>>> {
        self.main.get::<_, Str, SerdeBincode<Vec<&'t str>>>(rtxn, SEARCHABLE_FIELDS_KEY)
    }

    /// Identical to `searchable_fields`, but returns the ids instead.
    pub fn searchable_fields_ids<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<Vec<u8>>> {
        match self.searchable_fields(rtxn)? {
            Some(names) => {
                let fields_map = self.fields_ids_map(rtxn)?;
                let mut ids = Vec::new();
                for name in names {
                    let id = fields_map
                        .id(name)
                        .ok_or_else(|| format!("field id map must contain {}", name))
                        .expect("corrupted data: ");
                    ids.push(id);
                }
                Ok(Some(ids))

            }
            None => Ok(None),
        }
    }

    /* faceted fields */

    /// Writes the facet fields associated with their facet type or `None` if
    /// the facet type is currently unknown.
    pub fn put_faceted_fields(&self, wtxn: &mut RwTxn, fields_types: &HashMap<String, FacetType>) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<_>>(wtxn, FACETED_FIELDS_KEY, fields_types)
    }

    /// Deletes the facet fields ids associated with their facet type.
    pub fn delete_faceted_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, FACETED_FIELDS_KEY)
    }

    /// Returns the facet fields names associated with their facet type.
    pub fn faceted_fields(&self, rtxn: &RoTxn) -> heed::Result<HashMap<String, FacetType>> {
        Ok(self.main.get::<_, Str, SerdeJson<_>>(rtxn, FACETED_FIELDS_KEY)?.unwrap_or_default())
    }

    /// Same as `faceted_fields`, but returns ids instead.
    pub fn faceted_fields_ids(&self, rtxn: &RoTxn) -> heed::Result<HashMap<FieldId, FacetType>> {
        let faceted_fields = self.faceted_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;
        let faceted_fields = faceted_fields
            .iter()
            .map(|(k, v)| {
                let kid = fields_ids_map
                    .id(k)
                    .ok_or_else(|| format!("{} should be present in the field id map", k))
                    .expect("corrupted data: ");
                (kid, *v)
            })
        .collect();
        Ok(faceted_fields)
    }

    /* faceted documents ids */

    /// Writes the documents ids that are faceted under this field id.
    pub fn put_faceted_documents_ids(&self, wtxn: &mut RwTxn, field_id: FieldId, docids: &RoaringBitmap) -> heed::Result<()> {
        let mut buffer = [0u8; FACETED_DOCUMENTS_IDS_PREFIX.len() + 1];
        buffer[..FACETED_DOCUMENTS_IDS_PREFIX.len()].clone_from_slice(FACETED_DOCUMENTS_IDS_PREFIX.as_bytes());
        *buffer.last_mut().unwrap() = field_id;
        self.main.put::<_, ByteSlice, RoaringBitmapCodec>(wtxn, &buffer, docids)
    }

    /// Retrieve all the documents ids that faceted under this field id.
    pub fn faceted_documents_ids(&self, rtxn: &RoTxn, field_id: FieldId) -> heed::Result<RoaringBitmap> {
        let mut buffer = [0u8; FACETED_DOCUMENTS_IDS_PREFIX.len() + 1];
        buffer[..FACETED_DOCUMENTS_IDS_PREFIX.len()].clone_from_slice(FACETED_DOCUMENTS_IDS_PREFIX.as_bytes());
        *buffer.last_mut().unwrap() = field_id;
        match self.main.get::<_, ByteSlice, RoaringBitmapCodec>(rtxn, &buffer)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /* criteria */

    pub fn put_criteria(&self, wtxn: &mut RwTxn, criteria: &[Criterion]) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<&[Criterion]>>(wtxn, CRITERIA_KEY, &criteria)
    }

    pub fn delete_criteria(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, CRITERIA_KEY)
    }

    pub fn criteria(&self, rtxn: &RoTxn) -> heed::Result<Vec<Criterion>> {
        match self.main.get::<_, Str, SerdeJson<Vec<Criterion>>>(rtxn, CRITERIA_KEY)? {
            Some(criteria) => Ok(criteria),
            None => Ok(default_criteria()),
        }
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

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents(&self, rtxn: &RoTxn) -> anyhow::Result<usize> {
        Ok(self.documents_ids(rtxn).map(|docids| docids.len() as usize)?)
    }

    pub fn search<'a>(&'a self, rtxn: &'a RoTxn) -> Search<'a> {
        Search::new(rtxn, self)
    }
}
