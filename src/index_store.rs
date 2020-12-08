use std::borrow::Cow;
use std::fs;
use std::path::Path;

use anyhow::bail;
use heed::EnvOpenOptions;
use heed::types::{Str, SerdeJson};
use heed::{BytesEncode, BytesDecode};
use uuid::{Uuid, adapter::Hyphenated};

use crate::Index;

pub struct IndexStore {
    env: heed::Env,
    indexes_name_path: heed::Database<Str, Str>,
    indexes_name_options: heed::Database<Str, EnvOpenOptionsCodec>,
}

impl IndexStore {
    pub fn new<P: AsRef<Path>>(mut options: EnvOpenOptions, path: P) -> anyhow::Result<IndexStore> {
        options.max_dbs(2);
        let env = options.open(path)?;
        let indexes_name_path = env.create_database(Some("indexes-name-path"))?;
        let indexes_name_options = env.create_database(Some("indexes-name-options"))?;
        Ok(IndexStore { env, indexes_name_path, indexes_name_options })
    }

    pub fn create_index(&self, name: &str, options: EnvOpenOptions) -> anyhow::Result<Index> {
        let mut wtxn = self.env.write_txn()?;

        if let Some(path) = self.indexes_name_path.get(&wtxn, name)?.map(ToOwned::to_owned) {
            // We update the options even if the index already exists.
            self.indexes_name_options.put(&mut wtxn, name, &options)?;
            wtxn.commit()?;
            return Index::new(options, path);
        }

        // Generate a unique UUID V4 and a path based on it for this index.
        let mut buffer = [0u8; Hyphenated::LENGTH];
        let unique_id = Uuid::new_v4().to_hyphenated().encode_lower(&mut buffer);
        let unique_path = {
            let mut path = self.env.path().to_path_buf();
            path.push("indexes");
            path.push(unique_id);
            path
        };

        match unique_path.to_str() {
            Some(path) => {
                fs::create_dir_all(path)?;
                self.indexes_name_path.put(&mut wtxn, name, path)?;
                self.indexes_name_options.put(&mut wtxn, name, &options)?;
                wtxn.commit()?;
                self.create_index(name, options)
            },
            None => bail!("path contains invalid UTF-8 characters"),
        }
    }

    pub fn index(&self, name: &str) -> anyhow::Result<Option<Index>> {
        let rtxn = self.env.read_txn()?;
        match self.indexes_name_path.get(&rtxn, name)? {
            Some(path) => {
                let options = self.indexes_name_options.get(&rtxn, name)?.unwrap_or_default();
                Index::new(options, path).map(Some)
            },
            None => Ok(None),
        }
    }

    pub fn delete_index(&self, name: &str) -> anyhow::Result<bool> {
        let mut wtxn = self.env.write_txn()?;
        match self.indexes_name_path.get(&wtxn, name)? {
            Some(path) => {
                let path = path.to_owned();
                self.indexes_name_path.delete(&mut wtxn, name)?;
                self.indexes_name_options.delete(&mut wtxn, name)?;
                let index = Index::new(EnvOpenOptions::new(), &path)?;
                wtxn.commit()?;
                index.prepare_for_closing().wait();
                fs::remove_dir_all(path)?;
                Ok(true)
            },
            None => Ok(false),
        }
    }

    pub fn swap_indexes(&self, first: &str, second: &str) -> anyhow::Result<bool> {
        let fetch_index_data = |rtxn, name| -> anyhow::Result<Option<(String, EnvOpenOptions)>> {
            let path = self.indexes_name_path.get(rtxn, name)?.map(ToOwned::to_owned);
            let options = self.indexes_name_options.get(rtxn, name)?;
            Ok(path.zip(options))
        };

        let mut wtxn = self.env.write_txn()?;
        let first_data = fetch_index_data(&wtxn, first)?;
        let second_data = fetch_index_data(&wtxn, second)?;

        match first_data.zip(second_data) {
            Some(((first_path, first_options), (second_path, second_options))) => {
                self.indexes_name_path.put(&mut wtxn, first, &second_path)?;
                self.indexes_name_options.put(&mut wtxn, first, &second_options)?;

                self.indexes_name_path.put(&mut wtxn, second, &first_path)?;
                self.indexes_name_options.put(&mut wtxn, second, &first_options)?;

                wtxn.commit()?;
                Ok(true)
            },
            None => Ok(false),
        }
    }
}

struct EnvOpenOptionsCodec;

impl BytesDecode<'_> for EnvOpenOptionsCodec {
    type DItem = EnvOpenOptions;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        let (map_size, max_readers, max_dbs, flags) = SerdeJson::bytes_decode(bytes)?;
        Some(EnvOpenOptions { map_size, max_readers, max_dbs, flags })
    }
}

impl BytesEncode<'_> for EnvOpenOptionsCodec {
    type EItem = EnvOpenOptions;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        let EnvOpenOptions { map_size, max_readers, max_dbs, flags } = item;
        SerdeJson::bytes_encode(&(map_size, max_readers, max_dbs, flags))
            .map(Cow::into_owned)
            .map(Cow::Owned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use crate::update::IndexDocuments;

    #[test]
    fn simple_delete() {
        let options = EnvOpenOptions::new();
        let dir = tempfile::tempdir().unwrap();
        let store = IndexStore::new(options, &dir).unwrap();

        let options = EnvOpenOptions::new();
        let hello = store.create_index("hello", options).unwrap();

        // We make sure that we drop all of the references we have on the index.
        drop(hello);
        assert!(store.delete_index("hello").unwrap());

        // We try to get the index back.
        let index = store.index("hello").unwrap();
        assert!(index.is_none());

        let mut iter = fs::read_dir(dir.path().join("indexes")).unwrap();
        assert!(iter.next().is_none());
    }

    #[test]
    fn simple_swap() {
        let options = EnvOpenOptions::new();
        let dir = tempfile::tempdir().unwrap();
        let store = IndexStore::new(options, &dir).unwrap();

        let options = EnvOpenOptions::new();
        let hello = store.create_index("hello", options).unwrap();
        let options = EnvOpenOptions::new();
        let world = store.create_index("world", options).unwrap();

        // We update the first index.
        let mut wtxn = hello.write_txn().unwrap();
        let update = IndexDocuments::new(&mut wtxn, &hello);
        let content = &br#"[
            { "name": "Mel Gibson" },
            { "name": "Kevin Costner" }
        ]"#[..];
        update.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        // We update the second index.
        let mut wtxn = world.write_txn().unwrap();
        let update = IndexDocuments::new(&mut wtxn, &world);
        let content = &br#"[
            { "name": "Harrison Ford" },
            { "name": "Richard Gere" }
        ]"#[..];
        update.execute(content, |_| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = hello.read_txn().unwrap();
        let result = hello.search(&rtxn).query("gibson").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        drop(rtxn);

        let rtxn = world.read_txn().unwrap();
        let result = world.search(&rtxn).query("richard").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        drop(rtxn);

        // We drop the previous references to the indexes before swapping them.
        drop(hello);
        drop(world);

        store.swap_indexes("hello", "world").unwrap();

        // We retrieve again the indexes after we swapped them.
        // "hello" is the old "world" and "world" is the old "hello".
        let hello = store.index("hello").unwrap().unwrap();
        let world = store.index("world").unwrap().unwrap();

        // We try to search what's in "hello" in "world".
        let rtxn = world.read_txn().unwrap();
        let result = world.search(&rtxn).query("kevin costner").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        drop(rtxn);

        // We try to search what's in "world" in "hello".
        let rtxn = hello.read_txn().unwrap();
        let result = hello.search(&rtxn).query("harrison ford").execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
        drop(rtxn);
    }
}
