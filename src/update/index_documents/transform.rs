use std::borrow::Cow;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::iter::Peekable;
use std::time::Instant;

use anyhow::{anyhow, Context};
use grenad::CompressionType;
use log::info;
use roaring::RoaringBitmap;
use serde_json::{Map, Value};

use crate::{BEU32, MergeFn, Index, FieldsIdsMap, ExternalDocumentsIds};
use crate::update::{AvailableDocumentsIds, UpdateIndexingStep};
use super::merge_function::merge_two_obkvs;
use super::{create_writer, create_sorter, IndexDocumentsMethod};

pub struct TransformOutput {
    pub primary_key: String,
    pub fields_ids_map: FieldsIdsMap,
    pub external_documents_ids: ExternalDocumentsIds<'static>,
    pub new_documents_ids: RoaringBitmap,
    pub replaced_documents_ids: RoaringBitmap,
    pub documents_count: usize,
    pub documents_file: File,
}

/// Extract the external ids, deduplicate and compute the new internal documents ids
/// and fields ids, writing all the documents under their internal ids into a final file.
///
/// Outputs the new `FieldsIdsMap`, the new `UsersIdsDocumentsIds` map, the new documents ids,
/// the replaced documents ids, the number of documents in this update and the file
/// containing all those documents.
pub struct Transform<'t, 'i> {
    pub rtxn: &'t heed::RoTxn<'i>,
    pub index: &'i Index,
    pub log_every_n: Option<usize>,
    pub chunk_compression_type: CompressionType,
    pub chunk_compression_level: Option<u32>,
    pub chunk_fusing_shrink_size: Option<u64>,
    pub max_nb_chunks: Option<usize>,
    pub max_memory: Option<usize>,
    pub index_documents_method: IndexDocumentsMethod,
    pub autogenerate_docids: bool,
}

impl Transform<'_, '_> {
    pub fn output_from_json<R, F>(self, reader: R, progress_callback: F) -> anyhow::Result<TransformOutput>
    where
        R: Read,
        F: Fn(UpdateIndexingStep) + Sync,
    {
        self.output_from_generic_json(reader, false, progress_callback)
    }

    pub fn output_from_json_stream<R, F>(self, reader: R, progress_callback: F) -> anyhow::Result<TransformOutput>
    where
        R: Read,
        F: Fn(UpdateIndexingStep) + Sync,
    {
        self.output_from_generic_json(reader, true, progress_callback)
    }

    fn output_from_generic_json<R, F>(
        self,
        reader: R,
        is_stream: bool,
        progress_callback: F,
    ) -> anyhow::Result<TransformOutput>
    where
        R: Read,
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let mut fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let external_documents_ids = self.index.external_documents_ids(self.rtxn).unwrap();

        // Deserialize the whole batch of documents in memory.
        let mut documents: Peekable<Box<dyn Iterator<Item=serde_json::Result<Map<String, Value>>>>> = if is_stream {
            let iter = serde_json::Deserializer::from_reader(reader).into_iter();
            let iter = Box::new(iter) as Box<dyn Iterator<Item=_>>;
            iter.peekable()
        } else {
            let vec: Vec<_> = serde_json::from_reader(reader)?;
            let iter = vec.into_iter().map(Ok);
            let iter = Box::new(iter) as Box<dyn Iterator<Item=_>>;
            iter.peekable()
        };

        // We extract the primary key from the first document in
        // the batch if it hasn't already been defined in the index
        let (primary_key_id, primary_key) = match self.index.primary_key(self.rtxn)? {
            Some(primary_key) => {
                let id = fields_ids_map.id(primary_key).expect("primary key must be present in the fields id map");
                (id, primary_key.to_string())
            }
            None => {
                // We ignore a potential error here as we can't early return it now,
                // the peek method gives us only a reference on the next item,
                // we will eventually return it in the iteration just after.
                let first = documents.peek().and_then(|r| r.as_ref().ok());
                let name = match first.and_then(|doc| doc.keys().find(|k| k.contains("id"))) {
                    Some(key) => key.to_string(),
                    None => {
                        if !self.autogenerate_docids {
                            // If there is no primary key in the current document batch, we must
                            // return an error and not automatically generate any document id.
                            return Err(anyhow!("missing primary key"))
                        }
                        "id".to_string()
                    },
                };
                let id = fields_ids_map.insert("id").context("field id limit reached")?;
                (id, name)
            },
        };

        if documents.peek().is_none() {
            return Ok(TransformOutput {
                primary_key,
                fields_ids_map,
                external_documents_ids: ExternalDocumentsIds::default(),
                new_documents_ids: RoaringBitmap::new(),
                replaced_documents_ids: RoaringBitmap::new(),
                documents_count: 0,
                documents_file: tempfile::tempfile()?,
            });
        }

        // We must choose the appropriate merge function for when two or more documents
        // with the same user id must be merged or fully replaced in the same batch.
        let merge_function = match self.index_documents_method {
            IndexDocumentsMethod::ReplaceDocuments => keep_latest_obkv,
            IndexDocumentsMethod::UpdateDocuments => merge_obkvs,
        };

        // We initialize the sorter with the user indexing settings.
        let mut sorter = create_sorter(
            merge_function,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.chunk_fusing_shrink_size,
            self.max_nb_chunks,
            self.max_memory,
        );

        let mut json_buffer = Vec::new();
        let mut obkv_buffer = Vec::new();
        let mut uuid_buffer = [0; uuid::adapter::Hyphenated::LENGTH];
        let mut documents_count = 0;

        for result in documents {
            let document = result?;

            if self.log_every_n.map_or(false, |len| documents_count % len == 0) {
                progress_callback(UpdateIndexingStep::TransformFromUserIntoGenericFormat {
                    documents_seen: documents_count,
                });
            }

            obkv_buffer.clear();
            let mut writer = obkv::KvWriter::new(&mut obkv_buffer);

            // We prepare the fields ids map with the documents keys.
            for (key, _value) in &document {
                fields_ids_map.insert(&key).context("field id limit reached")?;
            }

            // We retrieve the user id from the document based on the primary key name,
            // if the document id isn't present we generate a uuid.
            let external_id = match document.get(&primary_key) {
                Some(value) => match value {
                    Value::String(string) => Cow::Borrowed(string.as_str()),
                    Value::Number(number) => Cow::Owned(number.to_string()),
                    _ => return Err(anyhow!("documents ids must be either strings or numbers")),
                },
                None => {
                    if !self.autogenerate_docids {
                        return Err(anyhow!("missing primary key"));
                    }
                    let uuid = uuid::Uuid::new_v4().to_hyphenated().encode_lower(&mut uuid_buffer);
                    Cow::Borrowed(uuid)
                },
            };

            // We iterate in the fields ids ordered.
            for (field_id, name) in fields_ids_map.iter() {
                json_buffer.clear();

                // We try to extract the value from the document and if we don't find anything
                // and this should be the document id we return the one we generated.
                if let Some(value) = document.get(name) {
                    // We serialize the attribute values.
                    serde_json::to_writer(&mut json_buffer, value)?;
                    writer.insert(field_id, &json_buffer)?;
                }
                else if field_id == primary_key_id {
                    // We validate the document id [a-zA-Z0-9\-_].
                    let external_id = match validate_document_id(&external_id) {
                        Some(valid) => valid,
                        None => return Err(anyhow!("invalid document id: {:?}", external_id)),
                    };

                    // We serialize the document id.
                    serde_json::to_writer(&mut json_buffer, &external_id)?;
                    writer.insert(field_id, &json_buffer)?;
                }
            }

            // We use the extracted/generated user id as the key for this document.
            sorter.insert(external_id.as_bytes(), &obkv_buffer)?;
            documents_count += 1;
        }

        progress_callback(UpdateIndexingStep::TransformFromUserIntoGenericFormat {
            documents_seen: documents_count,
        });

        // Now that we have a valid sorter that contains the user id and the obkv we
        // give it to the last transforming function which returns the TransformOutput.
        self.output_from_sorter(
            sorter,
            primary_key,
            fields_ids_map,
            documents_count,
            external_documents_ids,
            progress_callback,
        )
    }

    pub fn output_from_csv<R, F>(self, reader: R, progress_callback: F) -> anyhow::Result<TransformOutput>
    where
        R: Read,
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let mut fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let external_documents_ids = self.index.external_documents_ids(self.rtxn).unwrap();

        let mut csv = csv::Reader::from_reader(reader);
        let headers = csv.headers()?;

        // Generate the new fields ids based on the current fields ids and this CSV headers.
        let mut fields_ids = Vec::new();
        for (i, header) in headers.iter().enumerate() {
            let id = fields_ids_map.insert(header).context("field id limit reached)")?;
            fields_ids.push((id, i));
        }

        // Extract the position of the primary key in the current headers, None if not found.
        let external_id_pos = match self.index.primary_key(self.rtxn)? {
            Some(primary_key) => {
                // Te primary key have is known so we must find the position in the CSV headers.
                headers.iter().position(|h| h == primary_key)
            },
            None => headers.iter().position(|h| h.contains("id")),
        };

        // Returns the field id in the fileds ids map, create an "id" field
        // in case it is not in the current headers.
        let primary_key_field_id = match external_id_pos {
            Some(pos) => fields_ids_map.id(&headers[pos]).expect("found the primary key"),
            None => {
                if !self.autogenerate_docids {
                    // If there is no primary key in the current document batch, we must
                    // return an error and not automatically generate any document id.
                    return Err(anyhow!("missing primary key"))
                }
                let field_id = fields_ids_map.insert("id").context("field id limit reached")?;
                // We make sure to add the primary key field id to the fields ids,
                // this way it is added to the obks.
                fields_ids.push((field_id, usize::max_value()));
                field_id
            },
        };

        // We sort the fields ids by the fields ids map id, this way we are sure to iterate over
        // the records fields in the fields ids map order and correctly generate the obkv.
        fields_ids.sort_unstable_by_key(|(field_id, _)| *field_id);

        // We initialize the sorter with the user indexing settings.
        let mut sorter = create_sorter(
            keep_latest_obkv,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.chunk_fusing_shrink_size,
            self.max_nb_chunks,
            self.max_memory,
        );

        // We write into the sorter to merge and deduplicate the documents
        // based on the external ids.
        let mut json_buffer = Vec::new();
        let mut obkv_buffer = Vec::new();
        let mut uuid_buffer = [0; uuid::adapter::Hyphenated::LENGTH];
        let mut documents_count = 0;

        let mut record = csv::StringRecord::new();
        while csv.read_record(&mut record)? {
            obkv_buffer.clear();
            let mut writer = obkv::KvWriter::new(&mut obkv_buffer);

            if self.log_every_n.map_or(false, |len| documents_count % len == 0) {
                progress_callback(UpdateIndexingStep::TransformFromUserIntoGenericFormat {
                    documents_seen: documents_count,
                });
            }

            // We extract the user id if we know where it is or generate an UUID V4 otherwise.
            let external_id = match external_id_pos {
                Some(pos) => {
                    let external_id = &record[pos];
                    // We validate the document id [a-zA-Z0-9\-_].
                    match validate_document_id(&external_id) {
                        Some(valid) => valid,
                        None => return Err(anyhow!("invalid document id: {:?}", external_id)),
                    }
                },
                None => uuid::Uuid::new_v4().to_hyphenated().encode_lower(&mut uuid_buffer),
            };

            // When the primary_key_field_id is found in the fields ids list
            // we return the generated document id instead of the record field.
            let iter = fields_ids.iter()
                .map(|(fi, i)| {
                    let field = if *fi == primary_key_field_id { external_id } else { &record[*i] };
                    (fi, field)
                });

            // We retrieve the field id based on the fields ids map fields ids order.
            for (field_id, field) in iter {
                // We serialize the attribute values as JSON strings.
                json_buffer.clear();
                serde_json::to_writer(&mut json_buffer, &field)?;
                writer.insert(*field_id, &json_buffer)?;
            }

            // We use the extracted/generated user id as the key for this document.
            sorter.insert(external_id, &obkv_buffer)?;
            documents_count += 1;
        }

        progress_callback(UpdateIndexingStep::TransformFromUserIntoGenericFormat {
            documents_seen: documents_count,
        });

        // Now that we have a valid sorter that contains the user id and the obkv we
        // give it to the last transforming function which returns the TransformOutput.
        let primary_key_name = fields_ids_map
            .name(primary_key_field_id)
            .map(String::from)
            .expect("Primary key must be present in field_id map");
        self.output_from_sorter(
            sorter,
            primary_key_name,
            fields_ids_map,
            documents_count,
            external_documents_ids,
            progress_callback,
        )
    }

    /// Generate the `TransformOutput` based on the given sorter that can be generated from any
    /// format like CSV, JSON or JSON stream. This sorter must contain a key that is the document
    /// id for the user side and the value must be an obkv where keys are valid fields ids.
    fn output_from_sorter<F>(
        self,
        sorter: grenad::Sorter<MergeFn>,
        primary_key: String,
        fields_ids_map: FieldsIdsMap,
        approximate_number_of_documents: usize,
        mut external_documents_ids: ExternalDocumentsIds<'_>,
        progress_callback: F,
    ) -> anyhow::Result<TransformOutput>
    where
        F: Fn(UpdateIndexingStep) + Sync,
    {
        let documents_ids = self.index.documents_ids(self.rtxn)?;
        let mut available_documents_ids = AvailableDocumentsIds::from_documents_ids(&documents_ids);

        // Once we have sort and deduplicated the documents we write them into a final file.
        let mut final_sorter = create_sorter(
            |_docid, _obkvs| Err(anyhow!("cannot merge two documents")),
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.chunk_fusing_shrink_size,
            self.max_nb_chunks,
            self.max_memory,
        );
        let mut new_external_documents_ids_builder = fst::MapBuilder::memory();
        let mut replaced_documents_ids = RoaringBitmap::new();
        let mut new_documents_ids = RoaringBitmap::new();
        let mut obkv_buffer = Vec::new();

        // While we write into final file we get or generate the internal documents ids.
        let mut documents_count = 0;
        let mut iter = sorter.into_iter()?;
        while let Some((external_id, update_obkv)) = iter.next()? {

            if self.log_every_n.map_or(false, |len| documents_count % len == 0) {
                progress_callback(UpdateIndexingStep::ComputeIdsAndMergeDocuments {
                    documents_seen: documents_count,
                    total_documents: approximate_number_of_documents,
                });
            }

            let (docid, obkv) = match external_documents_ids.get(external_id) {
                Some(docid) => {
                    // If we find the user id in the current external documents ids map
                    // we use it and insert it in the list of replaced documents.
                    replaced_documents_ids.insert(docid);

                    // Depending on the update indexing method we will merge
                    // the document update with the current document or not.
                    match self.index_documents_method {
                        IndexDocumentsMethod::ReplaceDocuments => (docid, update_obkv),
                        IndexDocumentsMethod::UpdateDocuments => {
                            let key = BEU32::new(docid);
                            let base_obkv = self.index.documents.get(&self.rtxn, &key)?
                                .context("document not found")?;
                            let update_obkv = obkv::KvReader::new(update_obkv);
                            merge_two_obkvs(base_obkv, update_obkv, &mut obkv_buffer);
                            (docid, obkv_buffer.as_slice())
                        }
                    }
                },
                None => {
                    // If this user id is new we add it to the external documents ids map
                    // for new ids and into the list of new documents.
                    let new_docid = available_documents_ids.next()
                        .context("no more available documents ids")?;
                    new_external_documents_ids_builder.insert(external_id, new_docid as u64)?;
                    new_documents_ids.insert(new_docid);
                    (new_docid, update_obkv)
                },
            };

            // We insert the document under the documents ids map into the final file.
            final_sorter.insert(docid.to_be_bytes(), obkv)?;
            documents_count += 1;
        }

        progress_callback(UpdateIndexingStep::ComputeIdsAndMergeDocuments {
            documents_seen: documents_count,
            total_documents: documents_count,
        });

        // We create a final writer to write the new documents in order from the sorter.
        let file = tempfile::tempfile()?;
        let mut writer = create_writer(self.chunk_compression_type, self.chunk_compression_level, file)?;

        // Once we have written all the documents into the final sorter, we write the documents
        // into this writer, extract the file and reset the seek to be able to read it again.
        final_sorter.write_into(&mut writer)?;
        let mut documents_file = writer.into_inner()?;
        documents_file.seek(SeekFrom::Start(0))?;

        let before_docids_merging = Instant::now();
        // We merge the new external ids with existing external documents ids.
        let new_external_documents_ids = new_external_documents_ids_builder.into_map();
        external_documents_ids.insert_ids(&new_external_documents_ids)?;

        info!("Documents external merging took {:.02?}", before_docids_merging.elapsed());

        Ok(TransformOutput {
            primary_key,
            fields_ids_map,
            external_documents_ids: external_documents_ids.into_static(),
            new_documents_ids,
            replaced_documents_ids,
            documents_count,
            documents_file,
        })
    }

    /// Returns a `TransformOutput` with a file that contains the documents of the index
    /// with the attributes reordered accordingly to the `FieldsIdsMap` given as argument.
    // TODO this can be done in parallel by using the rayon `ThreadPool`.
    pub fn remap_index_documents(
        self,
        primary_key: String,
        fields_ids_map: FieldsIdsMap,
    ) -> anyhow::Result<TransformOutput>
    {
        let current_fields_ids_map = self.index.fields_ids_map(self.rtxn)?;
        let external_documents_ids = self.index.external_documents_ids(self.rtxn)?;
        let documents_ids = self.index.documents_ids(self.rtxn)?;
        let documents_count = documents_ids.len() as usize;

        // We create a final writer to write the new documents in order from the sorter.
        let file = tempfile::tempfile()?;
        let mut writer = create_writer(self.chunk_compression_type, self.chunk_compression_level, file)?;

        let mut obkv_buffer = Vec::new();
        for result in self.index.documents.iter(self.rtxn)? {
            let (docid, obkv) = result?;
            let docid = docid.get();

            obkv_buffer.clear();
            let mut obkv_writer = obkv::KvWriter::new(&mut obkv_buffer);

            // We iterate over the new `FieldsIdsMap` ids in order and construct the new obkv.
            for (id, name) in fields_ids_map.iter() {
                if let Some(val) = current_fields_ids_map.id(name).and_then(|id| obkv.get(id)) {
                    obkv_writer.insert(id, val)?;
                }
            }

            let buffer = obkv_writer.into_inner()?;
            writer.insert(docid.to_be_bytes(), buffer)?;
        }

        // Once we have written all the documents, we extract
        // the file and reset the seek to be able to read it again.
        let mut documents_file = writer.into_inner()?;
        documents_file.seek(SeekFrom::Start(0))?;

        Ok(TransformOutput {
            primary_key,
            fields_ids_map,
            external_documents_ids: external_documents_ids.into_static(),
            new_documents_ids: documents_ids,
            replaced_documents_ids: RoaringBitmap::default(),
            documents_count,
            documents_file,
        })
    }
}

/// Only the last value associated with an id is kept.
fn keep_latest_obkv(_key: &[u8], obkvs: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    obkvs.last().context("no last value").map(|last| last.clone().into_owned())
}

/// Merge all the obks in the order we see them.
fn merge_obkvs(_key: &[u8], obkvs: &[Cow<[u8]>]) -> anyhow::Result<Vec<u8>> {
    let mut iter = obkvs.iter();
    let first = iter.next().map(|b| b.clone().into_owned()).context("no first value")?;
    Ok(iter.fold(first, |acc, current| {
        let first = obkv::KvReader::new(&acc);
        let second = obkv::KvReader::new(current);
        let mut buffer = Vec::new();
        merge_two_obkvs(first, second, &mut buffer);
        buffer
    }))
}

fn validate_document_id(document_id: &str) -> Option<&str> {
    let document_id = document_id.trim();
    Some(document_id).filter(|id| {
        !id.is_empty() && id.chars().all(|c| {
            matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_')
        })
    })
}
