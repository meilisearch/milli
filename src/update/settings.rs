use std::collections::HashMap;
use std::str::FromStr;

use anyhow::Context;
use grenad::CompressionType;
use rayon::ThreadPool;

use crate::update::index_documents::{Transform, IndexDocumentsMethod};
use crate::update::{ClearDocuments, IndexDocuments, UpdateIndexingStep};
use crate::{Index, FieldsIdsMap, FieldId};
use crate::facet::FacetType;
use crate::criterion::Criterion;

pub struct Settings<'a, 't, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) log_every_n: Option<usize>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
    pub(crate) linked_hash_map_size: Option<usize>,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) chunk_fusing_shrink_size: Option<u64>,
    pub(crate) thread_pool: Option<&'a ThreadPool>,

    // If a struct field is set to `None` it means that it hasn't been set by the user,
    // however if it is `Some(None)` it means that the user forced a reset of the setting.
    searchable_fields: Option<Option<Vec<String>>>,
    displayed_fields: Option<Option<Vec<String>>>,
    faceted_fields: Option<Option<HashMap<String, String>>>,
    criteria: Option<Option<Vec<String>>>,
}

impl<'a, 't, 'u, 'i> Settings<'a, 't, 'u, 'i> {
    pub fn new(wtxn: &'t mut heed::RwTxn<'i, 'u>, index: &'i Index) -> Settings<'a, 't, 'u, 'i> {
        Settings {
            wtxn,
            index,
            log_every_n: None,
            max_nb_chunks: None,
            max_memory: None,
            linked_hash_map_size: None,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            chunk_fusing_shrink_size: None,
            thread_pool: None,
            searchable_fields: None,
            displayed_fields: None,
            faceted_fields: None,
            criteria: None,
        }
    }

    pub fn reset_searchable_fields(&mut self) {
        self.searchable_fields = Some(None);
    }

    pub fn set_searchable_fields(&mut self, names: Vec<String>) {
        self.searchable_fields = Some(Some(names));
    }

    pub fn reset_displayed_fields(&mut self) {
        self.displayed_fields = Some(None);
    }

    pub fn set_displayed_fields(&mut self, names: Vec<String>) {
        self.displayed_fields = Some(Some(names));
    }

    pub fn set_faceted_fields(&mut self, names_facet_types: HashMap<String, String>) {
        self.faceted_fields = Some(Some(names_facet_types));
    }

    pub fn reset_faceted_fields(&mut self) {
        self.faceted_fields = Some(None);
    }

    pub fn reset_criteria(&mut self) {
        self.criteria = Some(None);
    }

    pub fn set_criteria(&mut self, criteria: Vec<String>) {
        self.criteria = Some(Some(criteria));
    }

    fn reindex<F>(&mut self, cb: &F, primary_key: FieldId) -> anyhow::Result<()>
    where
        F: Fn(UpdateIndexingStep) + Sync

    {
        let fields_ids_map = self.index.fields_ids_map(self.wtxn)?;

            let transform = Transform {
                rtxn: &self.wtxn,
                index: self.index,
                log_every_n: self.log_every_n,
                chunk_compression_type: self.chunk_compression_type,
                chunk_compression_level: self.chunk_compression_level,
                chunk_fusing_shrink_size: self.chunk_fusing_shrink_size,
                max_nb_chunks: self.max_nb_chunks,
                max_memory: self.max_memory,
                index_documents_method: IndexDocumentsMethod::ReplaceDocuments,
                autogenerate_docids: false,
            };

            // We remap the documents fields based on the new `FieldsIdsMap`.
            let output = transform.remap_index_documents(primary_key, fields_ids_map.clone())?;

            // We clear the full database (words-fst, documents ids and documents content).
            ClearDocuments::new(self.wtxn, self.index).execute()?;

            // We index the generated `TransformOutput` which must contain
            // all the documents with fields in the newly defined searchable order.
            let mut indexing_builder = IndexDocuments::new(self.wtxn, self.index);
            indexing_builder.log_every_n = self.log_every_n;
            indexing_builder.max_nb_chunks = self.max_nb_chunks;
            indexing_builder.max_memory = self.max_memory;
            indexing_builder.linked_hash_map_size = self.linked_hash_map_size;
            indexing_builder.chunk_compression_type = self.chunk_compression_type;
            indexing_builder.chunk_compression_level = self.chunk_compression_level;
            indexing_builder.chunk_fusing_shrink_size = self.chunk_fusing_shrink_size;
            indexing_builder.thread_pool = self.thread_pool;
            indexing_builder.execute_raw(output, &cb)?;
            Ok(())
    }

    fn update_displayed(&mut self) -> anyhow::Result<bool> {
        match self.displayed_fields {
            Some(ref displayed_fields) => {
                match displayed_fields {
                    Some(fields) => {
                        let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                        let mut new_displayed = Vec::with_capacity(fields.len());
                        for field in fields {
                            let id = fields_ids_map.insert(&field).context("field id limit exceeded")?;
                            new_displayed.push(id);
                        }
                        self.index.put_displayed_fields(self.wtxn, &new_displayed)?;
                        self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;
                    }
                    None => { self.index.delete_displayed_fields(self.wtxn)?; },
                }
                Ok(true)
            }
            None => Ok(false),
        } 
    }

    /// Udpates the index's searchable attributes. This causes the field map to be recomputed to
    /// reflect the order of the searchable attributes. This causes all the other settings to be
    /// reset with the new id values.
    fn update_searchable(&mut self) -> anyhow::Result<bool> {
        match self.searchable_fields {
            Some(ref searchable_fields) => {
                match searchable_fields {
                    Some(fields) => {
                        // every time the searchable attributes are updated, we need to update the
                        // ids for any settings that uses the facets. (displayed_fields,
                        // faceted_fields)
                        let old_fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                        let old_primary_key = self.index
                            .primary_key(self.wtxn)?
                            .map(|id| old_fields_ids_map.name(id).expect("unexpected error when retrieving attribute name; corrupted data"));
                        let old_displayed_names = self.index
                            .displayed_fields(self.wtxn)?
                            .map(|fields| fields
                                .iter()
                                .map(|f| old_fields_ids_map.name(*f)
                                //At this point we need to crash, the data integrity is compromised
                                .expect("unexpected error when retrieving attribute name; corrupted data"))
                                .collect::<Vec<_>>());
                        let old_facets: HashMap<_, _> = self.index
                            .faceted_fields(self.wtxn)?
                            .iter()
                            .map(|(id, ty)| (old_fields_ids_map.name(*id).expect("unexpected error when retrieving attribute name; corrupted data"), *ty))
                            .collect();

                        let mut new_searchable = Vec::with_capacity(fields.len());
                        let mut new_fields_ids_map = FieldsIdsMap::new();

                        // Add all the searchable attributes to the field map, and then add the
                        // remaining fields from the old field map to the new one
                        for name in fields {
                            let id = new_fields_ids_map
                                .insert(&name)
                                .context("field id limit exceeded")?;
                            if !new_searchable.contains(&id) {
                                new_searchable.push(id);
                            }
                        }

                        for (_, name) in old_fields_ids_map.iter() {
                            new_fields_ids_map
                                .insert(&name)
                                .context("field id limit exceeded")?;
                        }

                        self.index.put_searchable_fields(self.wtxn, &new_searchable)?;

                        // If there were displayed or faceted fields, restore them with the new ids.
                        if let Some(old_displayed_names) = old_displayed_names {
                            let new_displayed: Vec<_> = old_displayed_names
                                .iter()
                                .map(|name| new_fields_ids_map.id(name).unwrap())
                                .collect();
                            self.index.put_displayed_fields(self.wtxn, &new_displayed)?;
                        }

                        if let Some(old_primary_key) = old_primary_key {
                            let new_primary_key = new_fields_ids_map
                                .insert(old_primary_key)
                                .context("field id limit exceeded")?;
                            self.index.put_primary_key(self.wtxn, new_primary_key)?;
                        }

                        let new_facets = old_facets
                            .iter()
                            .map(|(name, ty)| (new_fields_ids_map.id(name).unwrap(), *ty))
                            .collect();
                        self.index.put_faceted_fields(self.wtxn, &new_facets)?;
                        self.index.put_fields_ids_map(self.wtxn, &new_fields_ids_map)?;
                    }
                    None => { self.index.delete_searchable_fields(self.wtxn)?; },
                }
                Ok(true)
            }
            None => Ok(false),
        } 
    }

    fn update_facets(&mut self) -> anyhow::Result<bool> {
        match self.faceted_fields {
            Some(ref facet_fields) => {
                match facet_fields {
                    Some(fields) => {
                        let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                        let mut new_facets = HashMap::new();
                        for (name, ty) in fields {
                            let id = fields_ids_map.insert(name).context("field id limit exceeded")?;
                            let ty = FacetType::from_str(&ty)?;
                            new_facets.insert(id, ty);
                        }
                        self.index.put_faceted_fields(self.wtxn, &new_facets)?;
                        self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;
                    }
                    None => { self.index.delete_faceted_fields(self.wtxn)?; },
                }
                Ok(true)
            }
            None => Ok(false)
        }
    }

    fn update_primary_key(&mut self) -> anyhow::Result<FieldId> {
        let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
        let primary_key_id = match self.index.primary_key(&self.wtxn)? {
            Some(id) => id,
            None => fields_ids_map.insert("id").context("field id limit reached")?,
        };
        self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;

        Ok(primary_key_id)
    }

    fn update_criteria(&mut self) -> anyhow::Result<()> {
        if let Some(ref criteria) = self.criteria {
            match criteria {
                Some(fields) => {
                    let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                    let mut new_criteria = Vec::new();
                    for name in fields {
                        let criterion = Criterion::from_str(&mut fields_ids_map, &name)?;
                        new_criteria.push(criterion);
                    }
                    self.index.put_criteria(self.wtxn, &new_criteria)?;
                    self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;
                }
                None => { self.index.delete_criteria(self.wtxn)?; }
            }
        }
        Ok(())
    }

    pub fn execute<F>(mut self, progress_callback: F) -> anyhow::Result<()>
    where
        F: Fn(UpdateIndexingStep) + Sync
    {
        self.update_displayed()?;
        self.update_criteria()?;
        let primary_key = self.update_primary_key()?;

        // Use of eager operator | so both operand are evaluated unconditionally
        if self.update_facets()? | self.update_searchable()? {
            self.reindex(&progress_callback, primary_key)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use heed::EnvOpenOptions;
    use maplit::hashmap;

    use crate::facet::FacetType;
    use crate::update::{IndexDocuments, UpdateFormat};

    #[test]
    fn set_and_reset_searchable_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        // We change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_searchable_fields(vec!["name".into()]);
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the searchable field is correctly set to "name" only.
        let rtxn = index.read_txn().unwrap();
        // When we search for something that is not in
        // the searchable fields it must not return any document.
        let result = index.search(&rtxn).query("23").execute().unwrap();
        assert!(result.documents_ids.is_empty());

        // When we search for something that is in the searchable fields
        // we must find the appropriate document.
        let result = index.search(&rtxn).query(r#""kevin""#).execute().unwrap();
        let documents = index.documents(&rtxn, result.documents_ids).unwrap();
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].1.get(0), Some(&br#""kevin""#[..]));
        drop(rtxn);

        // We change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.reset_searchable_fields();
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the searchable field have been reset and documents are found now.
        let rtxn = index.read_txn().unwrap();
        let searchable_fields = index.searchable_fields(&rtxn).unwrap();
        assert_eq!(searchable_fields, None);
        let result = index.search(&rtxn).query("23").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        let documents = index.documents(&rtxn, result.documents_ids).unwrap();
        assert_eq!(documents[0].1.get(0), Some(&br#""kevin""#[..]));
        drop(rtxn);
    }

    #[test]
    fn mixup_searchable_with_displayed_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        // In the same transaction we change the displayed fields to be only the "age".
        // We also change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_displayed_fields(vec!["age".into()]);
        builder.set_searchable_fields(vec!["name".into()]);
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        let age_id = fields_ids_map.id("age").unwrap();
        assert_eq!(fields_ids, Some(&[age_id][..]));
        drop(rtxn);

        // We change the searchable fields to be the "name" field only.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.reset_searchable_fields();
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields always contains only the "age" field.
        let rtxn = index.read_txn().unwrap();
        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        let age_id = fields_ids_map.id("age").unwrap();
        assert_eq!(fields_ids, Some(&[age_id][..]));
        drop(rtxn);
    }

    #[test]
    fn default_displayed_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, None);
        drop(rtxn);
    }

    #[test]
    fn set_and_reset_displayed_field() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // First we send 3 documents with ids from 1 to 3.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();

        // In the same transaction we change the displayed fields to be only the age.
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_displayed_fields(vec!["age".into()]);
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to only the "age" field.
        let rtxn = index.read_txn().unwrap();
        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let age_field_id = fields_ids_map.id("age").unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids.unwrap(), &[age_field_id][..]);
        drop(rtxn);

        // We reset the fields ids to become `None`, the default value.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.reset_displayed_fields();
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set to `None` (default value).
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.displayed_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, None);
        drop(rtxn);
    }

    #[test]
    fn set_faceted_fields() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set the faceted fields to be the age.
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_faceted_fields(hashmap!{ "age".into() => "integer".into() });
        builder.execute(|_| ()).unwrap();

        // Then index some documents.
        let content = &b"name,age\nkevin,23\nkevina,21\nbenoit,34\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        // Check that the displayed fields are correctly set.
        let rtxn = index.read_txn().unwrap();
        let fields_ids = index.faceted_fields(&rtxn).unwrap();
        assert_eq!(fields_ids, hashmap!{ 1 => FacetType::Integer });
        // Only count the field_id 0 and level 0 facet values.
        let count = index.facet_field_id_value_docids.prefix_iter(&rtxn, &[1, 0]).unwrap().count();
        assert_eq!(count, 3);
        drop(rtxn);

        // Index a little more documents with new and current facets values.
        let mut wtxn = index.write_txn().unwrap();
        let content = &b"name,age\nkevin2,23\nkevina2,21\nbenoit2,35\n"[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index);
        builder.update_format(UpdateFormat::Csv);
        builder.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        // Only count the field_id 0 and level 0 facet values.
        let count = index.facet_field_id_value_docids.prefix_iter(&rtxn, &[1, 0]).unwrap().count();
        assert_eq!(count, 4);
        drop(rtxn);
    }

    #[test]
    fn setting_searchable_recomputes_other_settings() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        // Set all the settings except searchable
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_displayed_fields(vec!["hello".to_string()]);
        builder.set_faceted_fields(hashmap!{ "age".into() => "integer".into() });
        builder.set_criteria(vec!["asc(toto)".to_string()]);
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        // check the output
        let rtxn = index.read_txn().unwrap();
        assert_eq!([0], index.displayed_fields(&rtxn).unwrap().unwrap());
        assert_eq!(2, index.primary_key(&rtxn).unwrap().unwrap());
        assert_eq!("[Asc(1)]", format!("{:?}", index.criteria(&rtxn).unwrap()));
        assert_eq!(
            r##"FieldsIdsMap { names_ids: {"age": 3, "hello": 0, "id": 2, "toto": 1}, ids_names: {0: "hello", 1: "toto", 2: "id", 3: "age"}, next_id: Some(4) }"##,
            format!("{:?}", index.fields_ids_map(&rtxn).unwrap())
        );
        drop(rtxn);

        // We set toto and age as searchable to force reordering of the fields
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = Settings::new(&mut wtxn, &index);
        builder.set_searchable_fields(vec!["toto".to_string(), "age".to_string()]);
        builder.execute(|_| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();
        assert_eq!([2], index.displayed_fields(&rtxn).unwrap().unwrap());
        assert_eq!(2, index.primary_key(&rtxn).unwrap().unwrap());
        assert_eq!("[Asc(1)]", format!("{:?}", index.criteria(&rtxn).unwrap()));
        assert_eq!(
            r##"FieldsIdsMap { names_ids: {"age": 1, "hello": 2, "id": 3, "toto": 0}, ids_names: {0: "toto", 1: "age", 2: "hello", 3: "id"}, next_id: Some(4) }"##,
            format!("{:?}", index.fields_ids_map(&rtxn).unwrap())
        );
        drop(rtxn);
    }
}
