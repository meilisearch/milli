use std::fs;
use std::path::Path;

use heed::{EnvOpenOptions, EnvClosingEvent};
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

        let mut buffer = [0u8; Hyphenated::LENGTH];
        let unique_id = Uuid::new_v4().to_hyphenated().encode_lower(&mut buffer);

        // FIXME this is very bad to format a path like this
        let unique_path = format!("{}/indexes/{}", self.env.path().display(), unique_id);
        self.indexes_name_path.put(&mut wtxn, name, &unique_path)?;

        wtxn.commit()?;

        self.create_index(name, options)
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
