use std::cmp::Reverse;
use std::collections::HashSet;
use std::io::Cursor;

use big_s::S;
use either::{Either, Left, Right};
use heed::EnvOpenOptions;
use maplit::{hashmap, hashset};
use milli::documents::{DocumentBatchBuilder, DocumentBatchReader};
use milli::update::{Settings, UpdateBuilder};
use milli::{AscDesc, Criterion, DocumentId, Index, Member};
use serde::Deserialize;
use slice_group_by::GroupBy;

mod distinct;
mod filters;
mod query_criteria;
mod sort;

pub const TEST_QUERY: &'static str = "hello world america";

pub const EXTERNAL_DOCUMENTS_IDS: &[&str; 17] =
    &["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q"];

pub const CONTENT: &str = include_str!("../assets/test_set.ndjson");

pub fn setup_search_index_with_criteria(criteria: &[Criterion]) -> Index {
    let path = tempfile::tempdir().unwrap();
    let mut options = EnvOpenOptions::new();
    options.map_size(10 * 1024 * 1024); // 10 MB
    let index = Index::new(options, &path).unwrap();

    let mut wtxn = index.write_txn().unwrap();

    let mut builder = Settings::new(&mut wtxn, &index);

    let criteria = criteria.iter().map(|c| c.to_string()).collect();
    builder.set_criteria(criteria);
    builder.set_filterable_fields(hashset! {
        S("tag"),
        S("asc_desc_rank"),
        S("_geo"),
    });
    builder.set_sortable_fields(hashset! {
        S("tag"),
        S("asc_desc_rank"),
    });
    builder.set_synonyms(hashmap! {
        S("hello") => vec![S("good morning")],
        S("world") => vec![S("earth")],
        S("america") => vec![S("the united states")],
    });
    builder.set_searchable_fields(vec![S("title"), S("description")]);
    builder.execute(|_| ()).unwrap();

    // index documents
    let mut builder = UpdateBuilder::new();
    builder.max_memory(10 * 1024 * 1024); // 10MiB
    let mut builder = builder.index_documents(&mut wtxn, &index);
    builder.enable_autogenerate_docids();
    let mut cursor = Cursor::new(Vec::new());
    let mut documents_builder = DocumentBatchBuilder::new(&mut cursor).unwrap();
    let reader = Cursor::new(CONTENT.as_bytes());

    for doc in serde_json::Deserializer::from_reader(reader).into_iter::<serde_json::Value>() {
        let doc = Cursor::new(serde_json::to_vec(&doc.unwrap()).unwrap());
        documents_builder.extend_from_json(doc).unwrap();
    }

    documents_builder.finish().unwrap();

    cursor.set_position(0);

    // index documents
    let content = DocumentBatchReader::from_reader(cursor).unwrap();
    builder.execute(content, |_| ()).unwrap();

    wtxn.commit().unwrap();

    index
}

pub fn internal_to_external_ids(index: &Index, internal_ids: &[DocumentId]) -> Vec<String> {
    let mut rtxn = index.read_txn().unwrap();
    let docid_map = index.external_documents_ids(&mut rtxn).unwrap();
    let docid_map: std::collections::HashMap<_, _> =
        EXTERNAL_DOCUMENTS_IDS.iter().map(|id| (docid_map.get(id).unwrap(), id)).collect();
    internal_ids.iter().map(|id| docid_map.get(id).unwrap().to_string()).collect()
}

pub fn expected_order(
    criteria: &[Criterion],
    authorize_typo: bool,
    optional_words: bool,
    sort_by: &[AscDesc],
) -> Vec<TestDocument> {
    let dataset =
        serde_json::Deserializer::from_str(CONTENT).into_iter().map(|r| r.unwrap()).collect();
    let mut groups: Vec<Vec<TestDocument>> = vec![dataset];

    for criterion in criteria {
        let mut new_groups = Vec::new();
        for group in groups.iter_mut() {
            match criterion {
                Criterion::Attribute => {
                    group.sort_by_key(|d| d.attribute_rank);
                    new_groups
                        .extend(group.linear_group_by_key(|d| d.attribute_rank).map(Vec::from));
                }
                Criterion::Exactness => {
                    group.sort_by_key(|d| d.exact_rank);
                    new_groups.extend(group.linear_group_by_key(|d| d.exact_rank).map(Vec::from));
                }
                Criterion::Proximity => {
                    group.sort_by_key(|d| d.proximity_rank);
                    new_groups
                        .extend(group.linear_group_by_key(|d| d.proximity_rank).map(Vec::from));
                }
                Criterion::Sort if sort_by == [AscDesc::Asc(Member::Field(S("tag")))] => {
                    group.sort_by_key(|d| d.sort_by_rank);
                    new_groups.extend(group.linear_group_by_key(|d| d.sort_by_rank).map(Vec::from));
                }
                Criterion::Sort if sort_by == [AscDesc::Desc(Member::Field(S("tag")))] => {
                    group.sort_by_key(|d| Reverse(d.sort_by_rank));
                    new_groups.extend(group.linear_group_by_key(|d| d.sort_by_rank).map(Vec::from));
                }
                Criterion::Typo => {
                    group.sort_by_key(|d| d.typo_rank);
                    new_groups.extend(group.linear_group_by_key(|d| d.typo_rank).map(Vec::from));
                }
                Criterion::Words => {
                    group.sort_by_key(|d| d.word_rank);
                    new_groups.extend(group.linear_group_by_key(|d| d.word_rank).map(Vec::from));
                }
                Criterion::Asc(field_name) if field_name == "asc_desc_rank" => {
                    group.sort_by_key(|d| d.asc_desc_rank);
                    new_groups
                        .extend(group.linear_group_by_key(|d| d.asc_desc_rank).map(Vec::from));
                }
                Criterion::Desc(field_name) if field_name == "asc_desc_rank" => {
                    group.sort_by_key(|d| Reverse(d.asc_desc_rank));
                    new_groups
                        .extend(group.linear_group_by_key(|d| d.asc_desc_rank).map(Vec::from));
                }
                Criterion::Asc(_) | Criterion::Desc(_) | Criterion::Sort => {
                    new_groups.push(group.clone())
                }
            }
        }
        groups = std::mem::take(&mut new_groups);
    }

    if authorize_typo && optional_words {
        groups.into_iter().flatten().collect()
    } else if optional_words {
        groups.into_iter().flatten().filter(|d| d.typo_rank == 0).collect()
    } else if authorize_typo {
        groups.into_iter().flatten().filter(|d| d.word_rank == 0).collect()
    } else {
        groups.into_iter().flatten().filter(|d| d.word_rank == 0 && d.typo_rank == 0).collect()
    }
}

fn execute_filter(filter: &str, document: &TestDocument) -> Option<String> {
    let mut id = None;
    if let Some((field, filter)) = filter.split_once("=") {
        if field == "tag" && document.tag == filter {
            id = Some(document.id.clone())
        } else if field == "asc_desc_rank"
            && document.asc_desc_rank == filter.parse::<u32>().unwrap()
        {
            id = Some(document.id.clone())
        }
    } else if let Some(("asc_desc_rank", filter)) = filter.split_once("<") {
        if document.asc_desc_rank < filter.parse().unwrap() {
            id = Some(document.id.clone())
        }
    } else if let Some(("asc_desc_rank", filter)) = filter.split_once(">") {
        if document.asc_desc_rank > filter.parse().unwrap() {
            id = Some(document.id.clone())
        }
    } else if filter.starts_with("_geoRadius") {
        id = (document.geo_rank < 100000).then(|| document.id.clone());
    } else if filter.starts_with("NOT _geoRadius") {
        id = (document.geo_rank > 1000000).then(|| document.id.clone());
    }
    id
}

pub fn expected_filtered_ids(filters: Vec<Either<Vec<&str>, &str>>) -> HashSet<String> {
    let dataset: HashSet<TestDocument> =
        serde_json::Deserializer::from_str(CONTENT).into_iter().map(|r| r.unwrap()).collect();

    let mut filtered_ids: HashSet<_> = dataset.iter().map(|d| d.id.clone()).collect();
    for either in filters {
        let ids = match either {
            Left(array) => array
                .into_iter()
                .map(|f| {
                    let ids: HashSet<String> =
                        dataset.iter().filter_map(|d| execute_filter(f, d)).collect();
                    ids
                })
                .reduce(|a, b| a.union(&b).cloned().collect())
                .unwrap(),
            Right(filter) => {
                let ids: HashSet<String> =
                    dataset.iter().filter_map(|d| execute_filter(filter, d)).collect();
                ids
            }
        };

        filtered_ids = filtered_ids.intersection(&ids).cloned().collect();
    }

    filtered_ids
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Hash)]
pub struct TestDocument {
    pub id: String,
    pub word_rank: u32,
    pub typo_rank: u32,
    pub proximity_rank: u32,
    pub attribute_rank: u32,
    pub exact_rank: u32,
    pub asc_desc_rank: u32,
    pub sort_by_rank: u32,
    pub geo_rank: u32,
    pub title: String,
    pub description: String,
    pub tag: String,
}
