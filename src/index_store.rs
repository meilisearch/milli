use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::bail;
use heed::EnvOpenOptions;
use heed::types::{Str, SerdeJson};
use heed::{BytesEncode, BytesDecode};
use parking_lot::{RwLock, MappedRwLockReadGuard};
use parking_lot::lock_api::{RwLockWriteGuard, RwLockReadGuard};
use serde::{Serialize, Deserialize};
use uuid::{Uuid, adapter::Hyphenated};

use crate::{Index, UpdateStore};

pub struct IndexStore<M, N, F> {
    env: heed::Env,
    indexes_name_path: heed::Database<Str, Str>,
    indexes_name_options: heed::Database<Str, EnvOpenOptionsCodec>,
    indexes_name_update_options: heed::Database<Str, EnvOpenOptionsCodec>,
    indexes: RwLock<HashMap<String, (Index, UpdateStore<M, N>)>>,
    update_function: F,
}

impl<M, N, F> IndexStore<M, N, F>
where
    F: FnMut(u64, M, &[u8]) -> heed::Result<N> + Send + Clone + 'static,
    M: for<'a> Deserialize<'a> + 'static,
    N: Serialize + 'static,
{
    pub fn new<P: AsRef<Path>>(
        mut options: EnvOpenOptions,
        path: P,
        update_function: F,
    ) -> anyhow::Result<IndexStore<M, N, F>>
    {
        options.max_dbs(3);
        let env = options.open(path)?;
        let indexes_name_path = env.create_database(Some("indexes-name-path"))?;
        let indexes_name_options = env.create_database(Some("indexes-name-options"))?;
        let indexes_name_update_options = env.create_database(Some("indexes-name-update-options"))?;
        Ok(IndexStore {
            env,
            indexes_name_path,
            indexes_name_options,
            indexes_name_update_options,
            indexes: RwLock::new(HashMap::new()),
            update_function,
        })
    }

    pub fn create_index(
        &self,
        name: &str,
        options: EnvOpenOptions,
        update_options: EnvOpenOptions,
    ) -> anyhow::Result<MappedRwLockReadGuard<(Index, UpdateStore<M, N>)>>
    {
        let mut wtxn = self.env.write_txn()?;

        if let Some(path) = self.indexes_name_path.get(&wtxn, name)?.map(ToOwned::to_owned) {
            let updates_path = Path::new(&path).join("updates");

            // We update the options even if the index already exists.
            self.indexes_name_options.put(&mut wtxn, name, &options)?;
            self.indexes_name_update_options.put(&mut wtxn, name, &update_options)?;
            wtxn.commit()?;

            let index = Index::new(options, path)?;
            let update_function = self.update_function.clone();
            let update_store = UpdateStore::open(update_options, updates_path, update_function)?;

            let mut indexes = self.indexes.write();
            indexes.insert(name.to_owned(), (index, update_store));
            let indexes = RwLockWriteGuard::downgrade(indexes);
            return Ok(RwLockReadGuard::map(indexes, |map| map.get(name).unwrap()));
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
                fs::create_dir_all(Path::new(path).join("updates"))?;
                self.indexes_name_path.put(&mut wtxn, name, path)?;
                self.indexes_name_options.put(&mut wtxn, name, &options)?;
                self.indexes_name_update_options.put(&mut wtxn, name, &update_options)?;
                wtxn.commit()?;
                self.create_index(name, options, update_options)
            },
            None => bail!("path contains invalid UTF-8 characters"),
        }
    }

    pub fn index(&self, name: &str) -> Option<MappedRwLockReadGuard<(Index, UpdateStore<M, N>)>> {
        let indexes = self.indexes.read();
        if indexes.contains_key(name) {
            Some(RwLockReadGuard::map(indexes, |map| map.get(name).unwrap()))
        } else {
            None
        }
    }

    pub fn delete_index(&self, name: &str) -> anyhow::Result<bool> {
        let mut indexes = self.indexes.write();
        match indexes.remove(name) {
            Some((index, update_store)) => {
                let path = index.path().to_owned();

                let mut wtxn = self.env.write_txn()?;
                self.indexes_name_path.delete(&mut wtxn, name)?;
                self.indexes_name_options.delete(&mut wtxn, name)?;
                self.indexes_name_update_options.delete(&mut wtxn, name)?;
                wtxn.commit()?;

                update_store.prepare_for_closing().wait();
                index.prepare_for_closing().wait();
                fs::remove_dir_all(path)?;

                Ok(true)
            },
            None => Ok(false),
        }
    }

    pub fn swap_indexes(&self, first: &str, second: &str) -> anyhow::Result<bool> {
        let fetch_index_data = |rtxn, name| -> anyhow::Result<Option<(String, EnvOpenOptions, EnvOpenOptions)>> {
            let path = self.indexes_name_path.get(rtxn, name)?.map(ToOwned::to_owned);
            let options = self.indexes_name_options.get(rtxn, name)?;
            let uoptions = self.indexes_name_update_options.get(rtxn, name)?;
            match (path, options, uoptions) {
                (Some(path), Some(options), Some(uoptions)) => Ok(Some((path, options, uoptions))),
                (_, _, _) => Ok(None),
            }
        };

        let mut indexes = self.indexes.write();
        match indexes.remove_entry(first).zip(indexes.remove_entry(second)) {
            Some((first_entry, second_entry)) => {
                let (first, (first_index, first_update_store)) = first_entry;
                let (second, (second_index, second_update_store)) = second_entry;
                indexes.insert(first, (second_index, second_update_store));
                indexes.insert(second, (first_index, first_update_store));
            },
            None => return Ok(false),
        }

        let mut wtxn = self.env.write_txn()?;
        let first_data = fetch_index_data(&wtxn, first)?;
        let second_data = fetch_index_data(&wtxn, second)?;

        match first_data.zip(second_data) {
            Some(((fpath, foptions, fupdoptions), (spath, soptions, supdoptions))) => {
                self.indexes_name_path.put(&mut wtxn, first, &spath)?;
                self.indexes_name_options.put(&mut wtxn, first, &soptions)?;
                self.indexes_name_update_options.put(&mut wtxn, first, &supdoptions)?;

                self.indexes_name_path.put(&mut wtxn, second, &fpath)?;
                self.indexes_name_options.put(&mut wtxn, second, &foptions)?;
                self.indexes_name_update_options.put(&mut wtxn, second, &fupdoptions)?;

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
        let uoptions = EnvOpenOptions::new();
        let dir = tempfile::tempdir().unwrap();
        let store = IndexStore::<(), (), _>::new(options, &dir, |_, _, _| panic!()).unwrap();

        let options = EnvOpenOptions::new();
        let hello = store.create_index("hello", options, uoptions).unwrap();

        // We make sure that we drop all of the references we have on the index.
        drop(hello);

        assert!(store.delete_index("hello").unwrap());

        // We try to get the index back.
        assert!(store.index("hello").is_none());

        let mut iter = fs::read_dir(dir.path().join("indexes")).unwrap();
        assert!(iter.next().is_none());
    }

    #[test]
    fn simple_swap() {
        let options = EnvOpenOptions::new();
        let dir = tempfile::tempdir().unwrap();
        let store = IndexStore::<(), (), _>::new(options, &dir, |_, _, _| panic!()).unwrap();

        {
            let options = EnvOpenOptions::new();
            let uoptions = EnvOpenOptions::new();
            let _ = store.create_index("hello", options, uoptions).unwrap();
            let options = EnvOpenOptions::new();
            let uoptions = EnvOpenOptions::new();
            let _ = store.create_index("world", options, uoptions).unwrap();
        }

        {
            let (hello, _) = &*store.index("hello").unwrap();
            let (world, _) = &*store.index("world").unwrap();

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
        }

        store.swap_indexes("hello", "world").unwrap();

        // We retrieve again the indexes after we swapped them.
        // "hello" is the old "world" and "world" is the old "hello".
        let (hello, _) = &*store.index("hello").unwrap();
        let (world, _) = &*store.index("world").unwrap();

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
