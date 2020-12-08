use std::fs;
use std::path::Path;

use anyhow::bail;
use heed::EnvOpenOptions;
use heed::types::Str;
use uuid::{Uuid, adapter::Hyphenated};

use crate::Index;

pub struct IndexStore {
    env: heed::Env,
    indexes_name_path: heed::Database<Str, Str>,
}

impl IndexStore {
    pub fn new<P: AsRef<Path>>(mut options: EnvOpenOptions, path: P) -> anyhow::Result<IndexStore> {
        options.max_dbs(1);
        let env = options.open(path)?;
        let indexes_name_path = env.create_database(Some("indexes-name-path"))?;
        Ok(IndexStore { env, indexes_name_path })
    }

    pub fn create_index(&self, name: &str, options: EnvOpenOptions) -> anyhow::Result<Index> {
        let mut wtxn = self.env.write_txn()?;

        if let Some(path) = self.indexes_name_path.get(&wtxn, name)? {
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
                wtxn.commit()?;
                self.create_index(name, options)
            },
            None => bail!("path contains invalid UTF-8 characters"),
        }
    }

    pub fn index(&self, name: &str) -> anyhow::Result<Option<Index>> {
        let rtxn = self.env.read_txn()?;
        match self.indexes_name_path.get(&rtxn, name)? {
            // FIXME the EnvOpenOptions params must be serialized in LMDB
            //       to be able to reopen an Index with the same settings.
            Some(path) => Index::new(EnvOpenOptions::new(), path).map(Some),
            None => Ok(None),
        }
    }

    pub fn delete_index(&self, name: &str) -> anyhow::Result<bool> {
        let mut wtxn = self.env.write_txn()?;
        match self.indexes_name_path.get(&wtxn, name)? {
            Some(path) => {
                let path = path.to_owned();
                self.indexes_name_path.delete(&mut wtxn, name)?;
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
        let mut wtxn = self.env.write_txn()?;

        let first_path = match self.indexes_name_path.get(&wtxn, first)? {
            Some(path) => path.to_owned(),
            None => return Ok(false),
        };

        let second_path = match self.indexes_name_path.get(&wtxn, second)? {
            Some(path) => path.to_owned(),
            None => return Ok(false),
        };

        self.indexes_name_path.put(&mut wtxn, first, &second_path)?;
        self.indexes_name_path.put(&mut wtxn, second, &first_path)?;
        wtxn.commit()?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::update::IndexDocuments;

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
