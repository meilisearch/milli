use bumpalo::Bump;
use byteorder::ReadBytesExt;
use fxhash::FxHashMap;
use grenad::ChunkCreator;
use heed::RoTxn;
use obkv::{KvReader, KvWriter};
use roaring::RoaringBitmap;
use serde::Serialize;
use smartstring::SmartString;
use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use super::enriched::EnrichedBumpDocument;
use super::helpers::{create_sorter, create_writer, keep_latest_obkv, merge_obkvs, MergeFn};
use super::{IndexDocumentsMethod, IndexerConfig};
use crate::documents::bumpalo_json::{self, serialize_bincode};
use crate::error::{Error, InternalError, UserError};
use crate::index::db_name;
use crate::update::index_documents::enriched::EnrichedDocumentsBatchReader;
use crate::update::{AvailableDocumentsIds, UpdateIndexingStep};
use crate::{
    ExternalDocumentsIds, FieldDistribution, FieldId, FieldIdMapMissingEntry, FieldsIdsMap, Index,
    Result, BEU32,
};

pub struct TransformOutput {
    pub primary_key: String,
    pub fields_ids_map: FieldsIdsMap,
    pub field_distribution: FieldDistribution,
    pub external_documents_ids: ExternalDocumentsIds<'static>,
    pub new_documents_ids: RoaringBitmap,
    pub replaced_documents_ids: RoaringBitmap,
    pub documents_count: usize,
    pub original_documents: File,
    pub flattened_documents: File,
}

/// Extract the external ids, deduplicate and compute the new internal documents ids
/// and fields ids, writing all the documents under their internal ids into a final file.
///
/// Outputs the new `FieldsIdsMap`, the new `UsersIdsDocumentsIds` map, the new documents ids,
/// the replaced documents ids, the number of documents in this update and the file
/// containing all those documents.
pub struct Transform<'a, 'i> {
    pub index: &'i Index,
    fields_ids_map: FieldsIdsMap,

    indexer_settings: &'a IndexerConfig,
    pub autogenerate_docids: bool,
    pub index_documents_method: IndexDocumentsMethod,
    available_documents_ids: AvailableDocumentsIds,

    original_sorter: grenad::Sorter<MergeFn>,
    flattened_sorter: grenad::Sorter<MergeFn>,
    replaced_documents_ids: RoaringBitmap,
    new_documents_ids: RoaringBitmap,
    // To increase the cache locality and the heap usage we use smartstring.
    new_external_documents_ids_builder: FxHashMap<SmartString<smartstring::Compact>, u64>,
    documents_count: usize,
}

/// A helper type to reuse allocations when calling [`write_document_to_sorters_as_obkv`]
struct WriteDocumentsToSorterBuffers {
    document_obkv: Vec<u8>,
    values_bytes: Vec<Vec<u8>>,
    sorted_documents: Vec<(usize, FieldId)>,
}
impl WriteDocumentsToSorterBuffers {
    fn default() -> Self {
        Self { document_obkv: vec![], values_bytes: vec![], sorted_documents: vec![] }
    }
    fn clear(&mut self, doc_len: usize) {
        self.document_obkv.clear();
        for doc in self.values_bytes.iter_mut().take(doc_len) {
            doc.clear();
        }
        self.sorted_documents.clear();
    }
    fn get_value_bytes_buffer(&mut self, index: usize) -> &mut Vec<u8> {
        if index < self.values_bytes.len() {
            &mut self.values_bytes[index]
        } else {
            assert_eq!(self.values_bytes.len(), index);
            self.values_bytes.push(vec![]);
            &mut self.values_bytes[index]
        }
    }
}

/// Inserts the given document as obkv into the sorter. The key associated with the document
/// in the sorter is the given docid.
fn write_document_to_sorters_as_obkv<'bump, const N: usize, MF, CC>(
    document: &bumpalo_json::Map<'bump>,
    docid: u32,
    fields_ids_map: &FieldsIdsMap,
    sorters: [&mut grenad::Sorter<MF, CC>; N],
    buffers: &mut WriteDocumentsToSorterBuffers,
) -> Result<()>
where
    CC: ChunkCreator,
    MF: for<'a> Fn(&[u8], &[Cow<'a, [u8]>]) -> std::result::Result<Cow<'a, [u8]>, Error>,
{
    // The steps to follow are:
    // 1. Map each key-value pair in the document as follows:
    //      * the key is mapped to its corresponding FieldId using `fields_ids_map`
    //      * the value is serialized to a byte vector using bincode::serialize
    // 2. Sort the result so that the keys (FieldIds) are in order, which is required for insertion into the obkv
    // 3. Make an obkv from the sorted result
    // 4. Insert the obkv into the sorter under the key `docid`
    buffers.clear(document.0.len());

    for (index, (key, value)) in document.0.iter().enumerate() {
        let value_buffer = buffers.get_value_bytes_buffer(index);
        serialize_bincode(value.as_ref(), value_buffer).unwrap();

        let field_id = fields_ids_map.id(key).unwrap();
        buffers.sorted_documents.push((index, field_id));
    }

    buffers.sorted_documents.sort_unstable_by(|x, y| x.1.cmp(&y.1));

    let WriteDocumentsToSorterBuffers { document_obkv, values_bytes, sorted_documents } = buffers;
    let mut document_writer_to_obkv = obkv::KvWriter::new(&mut *document_obkv);

    for (original_index, field_id) in sorted_documents.iter() {
        let value_bytes = &values_bytes[*original_index];
        document_writer_to_obkv.insert(*field_id, &value_bytes)?;
    }
    for sorter in sorters {
        sorter.insert(&docid.to_be_bytes(), &document_obkv)?;
    }
    Ok(())
}

impl<'a, 'i> Transform<'a, 'i> {
    pub fn new(
        wtxn: &mut heed::RwTxn,
        index: &'i Index,
        indexer_settings: &'a IndexerConfig,
        index_documents_method: IndexDocumentsMethod,
        autogenerate_docids: bool,
    ) -> Result<Self> {
        // We must choose the appropriate merge function for when two or more documents
        // with the same user id must be merged or fully replaced in the same batch.
        let merge_function = match index_documents_method {
            IndexDocumentsMethod::ReplaceDocuments => keep_latest_obkv,
            IndexDocumentsMethod::UpdateDocuments => merge_obkvs,
        };

        // We initialize the sorter with the user indexing settings.
        let original_sorter = create_sorter(
            merge_function,
            indexer_settings.chunk_compression_type,
            indexer_settings.chunk_compression_level,
            indexer_settings.max_nb_chunks,
            indexer_settings.max_memory.map(|mem| mem / 2),
        );

        // We initialize the sorter with the user indexing settings.
        let flattened_sorter = create_sorter(
            merge_function,
            indexer_settings.chunk_compression_type,
            indexer_settings.chunk_compression_level,
            indexer_settings.max_nb_chunks,
            indexer_settings.max_memory.map(|mem| mem / 2),
        );
        let documents_ids = index.documents_ids(wtxn)?;

        Ok(Transform {
            index,
            fields_ids_map: index.fields_ids_map(wtxn)?,
            indexer_settings,
            autogenerate_docids,
            available_documents_ids: AvailableDocumentsIds::from_documents_ids(&documents_ids),
            original_sorter,
            flattened_sorter,
            index_documents_method,
            replaced_documents_ids: RoaringBitmap::new(),
            new_documents_ids: RoaringBitmap::new(),
            new_external_documents_ids_builder: FxHashMap::default(),
            documents_count: 0,
        })
    }

    /**
    Pre-process the documents from the given `reader`. Return the number of pre-processed documents.

    The pre-processing steps are:
    1. If a documents does not contain a primary key and the “autogenerate ids” feature is activated,
    an additional “id” key with the autogenerated docid will be added to both the original and the flattened document.

    2. We get or create an internal ID for each document. If a document already exists in the database,
    we add its internal docid to the `self.replaced_documents` bitmap. We also make use of the “merging”
    functionality in `grenad::Sorter` to resolve the conflict. First, we add the previously existing document
    in the database (original and flattened) to the sorters, and then add again the new documents to those
    same sorters. If the merging policy is "replace the documents", then only the new documents will be kept.
    Otherwise, if the merging policy is “merge documents”, then the content of the old and new documents
    will be merged together.

    3. Update the `fields_ids_map` in the database so that it contains the name of each
    new field appearing in the added documents.

    4. Write the documents to a file via a `grenad::Sorter`s where the key is the *internal*
    document ID and the value is the document encoded as an OBKV. Note that this is done twice:
        1. Once for the original document. In this case the keys in the OBKV are the "top-level" keys of
        documents and the values are the Json values corresponding to those keys.
        2. Once for the flattened document created by `bumpalo_json::flatten`

    5. Create the `primary_key` in the database if it didn't exist.
    */
    pub fn read_documents<R, F>(
        &mut self,
        reader: EnrichedDocumentsBatchReader<R>,
        wtxn: &mut heed::RwTxn,
        progress_callback: F,
    ) -> Result<usize>
    where
        R: Read + Seek,
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let mut cursor = reader.into_cursor();
        // Maps the external documents ids to the internal ids
        let internal_docid_from_external_id = self.index.external_documents_ids(wtxn)?;
        let primary_key = cursor.primary_key().to_string();
        let mut documents_count = 0;

        let mut write_docs_buffers = WriteDocumentsToSorterBuffers::default();
        let mut bump = Bump::new();
        loop {
            bump.reset();
            let enriched_document =
                if let Some(enriched_document) = cursor.next_enriched_bump_document(&bump)? {
                    enriched_document
                } else {
                    break;
                };
            let EnrichedBumpDocument { document: original_document, document_id: external_id } =
                enriched_document;

            if self.indexer_settings.log_every_n.map_or(false, |len| documents_count % len == 0) {
                progress_callback(UpdateIndexingStep::RemapDocumentAddition {
                    documents_seen: documents_count,
                });
            }

            let external_id_string = external_id.value();
            // Step 1.
            if external_id.is_generated() {
                // TODO: do this only at the last step, when writing into the obkv
                original_document.0.push((
                    bump.alloc_str(primary_key.as_str()),
                    bumpalo_json::MaybeMut::Ref(
                        bump.alloc(bumpalo_json::Value::String(bump.alloc(external_id_string))),
                    ),
                ));
            }

            // Step 2, part 1.

            // Get the internal ID of the document, creating one if necessary.
            let (internal_id, document_exists_in_db, document_already_seen_in_batch) =
                match internal_docid_from_external_id.get(external_id_string) {
                    // The document is already in the db
                    Some(docid) => (docid, true, self.replaced_documents_ids.contains(docid)),
                    // The document is not in the db
                    None => {
                        // We get or create its internal ID using the builder
                        match self
                            .new_external_documents_ids_builder
                            .entry(external_id_string.into())
                        {
                            // It already has an internal ID -> it was already seen in this batch
                            Entry::Occupied(entry) => (*entry.get() as u32, false, true),
                            // It doesn't already have an internal ID -> we create it and add it to the builder
                            Entry::Vacant(entry) => {
                                let new_docid = self
                                    .available_documents_ids
                                    .next()
                                    .ok_or(UserError::DocumentLimitReached)?;
                                entry.insert(new_docid as u64);
                                (new_docid, false, false)
                            }
                        }
                    }
                };

            // Step 2, part 2.
            // We need to add the document from the DB to the sorters, but we make sure
            // to only do this once in case many documents in this batch replace the same document
            if document_exists_in_db && !document_already_seen_in_batch {
                self.replaced_documents_ids.insert(internal_id);

                let key = BEU32::new(internal_id);
                let base_obkv = self
                    .index
                    .documents
                    .remap_data_type::<heed::types::ByteSlice>()
                    .get(wtxn, &key)?
                    .ok_or(InternalError::DatabaseMissingEntry {
                        db_name: db_name::DOCUMENTS,
                        key: None,
                    })?;

                self.original_sorter.insert(&internal_id.to_be_bytes(), base_obkv)?;

                // now we also append the flattened document into the sorter, which is done by:
                // 1. converting the base_obkv to json
                // 2. flattening the json
                // 3. writing it into the flattened sorter
                let reader = KvReader::new(base_obkv);
                let mut document = bumpalo::collections::vec::Vec::with_capacity_in(16, &bump);
                for (field_id, value) in reader.iter() {
                    let key: &str = bump.alloc_str(self.fields_ids_map.name(field_id).unwrap()); // TODO: error handling
                    let value = bumpalo_json::deserialize_bincode_slice(value, &bump).unwrap(); // TODO: error handling
                    document.push((key, bumpalo_json::MaybeMut::Mut(bump.alloc(value))));
                }
                let document: &_ = bump.alloc(bumpalo_json::Map(document));
                let (was_flattened, flattened_document) = bumpalo_json::flatten(document, &bump);
                match was_flattened {
                    true => {
                        write_document_to_sorters_as_obkv(
                            flattened_document,
                            internal_id,
                            &self.fields_ids_map,
                            [&mut self.flattened_sorter],
                            &mut write_docs_buffers,
                        )?;
                    }
                    false => {
                        self.flattened_sorter.insert(&internal_id.to_be_bytes(), base_obkv)?;
                    }
                }
            } else {
                self.new_documents_ids.insert(internal_id);
            }

            let (was_flattened, flattened_document) =
                bumpalo_json::flatten(&original_document, &bump);

            // Step 3.
            if was_flattened {
                for (key, _) in flattened_document.0.iter() {
                    self.fields_ids_map.insert(key).ok_or(UserError::AttributeLimitReached)?;
                }
            }
            // TODO: remove when the flattened document is guaranteed to contain all the keys of the original document
            for (key, _) in original_document.0.iter() {
                self.fields_ids_map.insert(key).ok_or(UserError::AttributeLimitReached)?;
            }

            // // Step 4.
            match was_flattened {
                false => {
                    write_document_to_sorters_as_obkv(
                        original_document,
                        internal_id,
                        &self.fields_ids_map,
                        [&mut self.original_sorter, &mut self.flattened_sorter],
                        &mut write_docs_buffers,
                    )?;
                }
                true => {
                    write_document_to_sorters_as_obkv(
                        flattened_document,
                        internal_id,
                        &self.fields_ids_map,
                        [&mut self.flattened_sorter],
                        &mut write_docs_buffers,
                    )?;
                    write_document_to_sorters_as_obkv(
                        &original_document,
                        internal_id,
                        &self.fields_ids_map,
                        [&mut self.original_sorter],
                        &mut write_docs_buffers,
                    )?;
                }
            }

            documents_count += 1;

            progress_callback(UpdateIndexingStep::RemapDocumentAddition {
                documents_seen: documents_count,
            });
        }

        progress_callback(UpdateIndexingStep::RemapDocumentAddition {
            documents_seen: documents_count,
        });

        // Step 4 & 5
        self.index.put_fields_ids_map(wtxn, &self.fields_ids_map)?;
        self.index.put_primary_key(wtxn, &primary_key)?;
        self.documents_count += documents_count;

        Ok(documents_count)
    }

    /// Generate the `TransformOutput` based on the given sorter that can be generated from any
    /// format like CSV, JSON or JSON stream. This sorter must contain a key that is the document
    /// id for the user side and the value must be an obkv where keys are valid fields ids.
    pub(crate) fn output_from_sorter<F>(
        self,
        wtxn: &mut heed::RwTxn,
        progress_callback: F,
    ) -> Result<TransformOutput>
    where
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let primary_key = self
            .index
            .primary_key(&wtxn)?
            .ok_or(Error::UserError(UserError::MissingPrimaryKey))?
            .to_string();

        let mut external_documents_ids = self.index.external_documents_ids(wtxn)?;

        // We create a final writer to write the new documents in order from the sorter.
        let mut writer = create_writer(
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            tempfile::tempfile()?,
        );

        // Once we have all the documents in the sorter, we write the documents
        // in the writer. We also generate the field distribution.
        let mut field_distribution = self.index.field_distribution(wtxn)?;
        let mut iter = self.original_sorter.into_stream_merger_iter()?;
        // used only for the callback
        let mut documents_count = 0;

        while let Some((key, val)) = iter.next()? {
            // send a callback to show at which step we are
            documents_count += 1;
            progress_callback(UpdateIndexingStep::ComputeIdsAndMergeDocuments {
                documents_seen: documents_count,
                total_documents: self.documents_count,
            });

            let u32_key = key.clone().read_u32::<byteorder::BigEndian>()?;
            // if the document was already in the db we remove all of its field
            // from the field distribution.
            if self.replaced_documents_ids.contains(u32_key) {
                let obkv = self.index.documents.get(wtxn, &BEU32::new(u32_key))?.ok_or(
                    InternalError::DatabaseMissingEntry { db_name: db_name::DOCUMENTS, key: None },
                )?;

                for (key, _) in obkv.iter() {
                    let name =
                        self.fields_ids_map.name(key).ok_or(FieldIdMapMissingEntry::FieldId {
                            field_id: key,
                            process: "Computing field distribution in transform.",
                        })?;
                    // We checked that the document was in the db earlier. If we can't find it it means
                    // there is an inconsistency between the field distribution and the field id map.
                    let field = field_distribution.get_mut(name).ok_or(
                        FieldIdMapMissingEntry::FieldId {
                            field_id: key,
                            process: "Accessing field distribution in transform.",
                        },
                    )?;
                    *field -= 1;
                    if *field == 0 {
                        // since we were able to get the field right before it's safe to unwrap here
                        field_distribution.remove(name).unwrap();
                    }
                }
            }

            // We increment all the field of the current document in the field distribution.
            let obkv = KvReader::new(val);

            for (key, _) in obkv.iter() {
                let name =
                    self.fields_ids_map.name(key).ok_or(FieldIdMapMissingEntry::FieldId {
                        field_id: key,
                        process: "Computing field distribution in transform.",
                    })?;
                *field_distribution.entry(name.to_string()).or_insert(0) += 1;
            }
            writer.insert(key, val)?;
        }

        let mut original_documents = writer.into_inner()?;
        // We then extract the file and reset the seek to be able to read it again.
        original_documents.seek(SeekFrom::Start(0))?;

        // We create a final writer to write the new documents in order from the sorter.
        let mut writer = create_writer(
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            tempfile::tempfile()?,
        );
        // Once we have written all the documents into the final sorter, we write the documents
        // into this writer, extract the file and reset the seek to be able to read it again.
        self.flattened_sorter.write_into_stream_writer(&mut writer)?;
        let mut flattened_documents = writer.into_inner()?;
        flattened_documents.seek(SeekFrom::Start(0))?;

        let mut new_external_documents_ids_builder: Vec<_> =
            self.new_external_documents_ids_builder.into_iter().collect();

        new_external_documents_ids_builder
            .sort_unstable_by(|(left, _), (right, _)| left.cmp(&right));
        let mut fst_new_external_documents_ids_builder = fst::MapBuilder::memory();
        new_external_documents_ids_builder.into_iter().try_for_each(|(key, value)| {
            fst_new_external_documents_ids_builder.insert(key, value)
        })?;
        let new_external_documents_ids = fst_new_external_documents_ids_builder.into_map();
        external_documents_ids.insert_ids(&new_external_documents_ids)?;

        Ok(TransformOutput {
            primary_key,
            fields_ids_map: self.fields_ids_map,
            field_distribution,
            external_documents_ids: external_documents_ids.into_static(),
            new_documents_ids: self.new_documents_ids,
            replaced_documents_ids: self.replaced_documents_ids,
            documents_count: self.documents_count,
            original_documents,
            flattened_documents,
        })
    }

    /// Returns a `TransformOutput` with a file that contains the documents of the index
    /// with the attributes reordered accordingly to the `FieldsIdsMap` given as argument.
    // TODO this can be done in parallel by using the rayon `ThreadPool`.
    pub fn remap_index_documents(
        self,
        wtxn: &mut heed::RwTxn,
        old_fields_ids_map: FieldsIdsMap,
        mut new_fields_ids_map: FieldsIdsMap,
    ) -> Result<TransformOutput> {
        // There already has been a document addition, the primary key should be set by now.
        let primary_key =
            self.index.primary_key(wtxn)?.ok_or(UserError::MissingPrimaryKey)?.to_string();
        let field_distribution = self.index.field_distribution(wtxn)?;
        let external_documents_ids = self.index.external_documents_ids(wtxn)?;
        let documents_ids = self.index.documents_ids(wtxn)?;
        let documents_count = documents_ids.len() as usize;

        // We create a final writer to write the new documents in order from the sorter.
        let mut original_writer = create_writer(
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            tempfile::tempfile()?,
        );

        // We create a final writer to write the new documents in order from the sorter.
        let mut flattened_writer = create_writer(
            self.indexer_settings.chunk_compression_type,
            self.indexer_settings.chunk_compression_level,
            tempfile::tempfile()?,
        );

        let mut obkv_buffer = Vec::new();
        let mut value_bytes = Vec::<u8>::with_capacity(1024);

        let mut bump = bumpalo::Bump::new();
        for result in self.index.documents.iter(wtxn)? {
            bump.reset();
            let (docid, obkv) = result?;
            let docid = docid.get();

            obkv_buffer.clear();
            let mut obkv_writer = obkv::KvWriter::<_, FieldId>::new(&mut obkv_buffer);

            // We iterate over the new `FieldsIdsMap` ids in order and construct the new obkv.
            for (id, name) in new_fields_ids_map.iter() {
                if let Some(val) = old_fields_ids_map.id(name).and_then(|id| obkv.get(id)) {
                    obkv_writer.insert(id, val)?;
                }
            }

            let buffer = obkv_writer.into_inner()?;
            original_writer.insert(docid.to_be_bytes(), &buffer)?;

            // Once we have the document. We're going to flatten it
            // and insert it in the flattened sorter.
            let mut doc =
                bumpalo_json::Map(bumpalo::collections::vec::Vec::with_capacity_in(16, &bump));

            let reader = obkv::KvReader::new(buffer);
            for (k, v) in reader.iter() {
                let key = new_fields_ids_map.name(k).ok_or(FieldIdMapMissingEntry::FieldId {
                    field_id: k,
                    process: "Accessing field distribution in transform.",
                })?;
                let value = bumpalo_json::deserialize_bincode_slice(v, &bump)
                    .map_err(InternalError::Bincode)?;
                doc.0.push((bump.alloc_str(key), bumpalo_json::MaybeMut::Ref(bump.alloc(value))));
            }
            let doc = bump.alloc(doc);
            let (_, flattened) = bumpalo_json::flatten(doc, &bump);

            // Once we have the flattened version we can convert it back to obkv and
            // insert all the new generated fields_ids (if any) in the fields ids map.
            let mut buffer: Vec<u8> = Vec::new();
            let mut writer = KvWriter::new(&mut buffer);
            let mut flattened: Vec<_> = flattened.0.iter().collect();
            // we reorder the field to get all the known field first
            flattened.sort_unstable_by_key(|(key, _)| {
                new_fields_ids_map.id(&key).unwrap_or(FieldId::MAX)
            });

            for (key, value) in flattened {
                let fid =
                    new_fields_ids_map.insert(&key).ok_or(UserError::AttributeLimitReached)?;
                let mut serializer =
                    bincode::Serializer::new(&mut value_bytes, bincode::DefaultOptions::default());
                value.as_ref().serialize(&mut serializer).map_err(InternalError::Bincode)?;
                writer.insert(fid, &value_bytes)?;
                value_bytes.clear();
            }
            flattened_writer.insert(docid.to_be_bytes(), &buffer)?;
        }

        // Once we have written all the documents, we extract
        // the file and reset the seek to be able to read it again.
        let mut original_documents = original_writer.into_inner()?;
        original_documents.seek(SeekFrom::Start(0))?;

        let mut flattened_documents = flattened_writer.into_inner()?;
        flattened_documents.seek(SeekFrom::Start(0))?;

        Ok(TransformOutput {
            primary_key,
            fields_ids_map: new_fields_ids_map,
            field_distribution,
            external_documents_ids: external_documents_ids.into_static(),
            new_documents_ids: documents_ids,
            replaced_documents_ids: RoaringBitmap::default(),
            documents_count,
            original_documents,
            flattened_documents,
        })
    }
}

impl TransformOutput {
    // find and insert the new field ids
    pub fn compute_real_facets(&self, rtxn: &RoTxn, index: &Index) -> Result<HashSet<String>> {
        let user_defined_facets = index.user_defined_faceted_fields(rtxn)?;

        Ok(self
            .fields_ids_map
            .names()
            .filter(|&field| crate::is_faceted(field, &user_defined_facets))
            .map(|field| field.to_string())
            .collect())
    }
}
