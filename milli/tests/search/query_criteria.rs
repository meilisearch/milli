use big_s::S;
use milli::{Criterion, Search, SearchResult};

use crate::search::{self, EXTERNAL_DOCUMENTS_IDS};
use Criterion::*;

const ALLOW_TYPOS: bool = true;
const DISALLOW_TYPOS: bool = false;
const ALLOW_OPTIONAL_WORDS: bool = true;
const DISALLOW_OPTIONAL_WORDS: bool = false;

macro_rules! test_criterion {
    ($func:ident, $optional_word:ident, $authorize_typos:ident $(, $criterion:expr)?) => {
        #[test]
        fn $func() {
            let criteria = vec![$($criterion)?];
            let index = search::setup_search_index_with_criteria(&criteria);
            let mut rtxn = index.read_txn().unwrap();

            let mut search = Search::new(&mut rtxn, &index);
            search.query(search::TEST_QUERY);
            search.limit(EXTERNAL_DOCUMENTS_IDS.len());
            search.authorize_typos($authorize_typos);
            search.optional_words($optional_word);

            let SearchResult { documents_ids, .. } = search.execute().unwrap();

            let expected_external_ids: Vec<_> = search::expected_order(&criteria, $authorize_typos, $optional_word)
                .into_iter()
                .map(|d| d.id).collect();
            let documents_ids = search::internal_to_external_ids(&index, &documents_ids);
            assert_eq!(documents_ids, expected_external_ids);
        }
    }
}

#[rustfmt::skip]
test_criterion!(none_allow_typo,                     ALLOW_OPTIONAL_WORDS,      ALLOW_TYPOS);
#[rustfmt::skip]
test_criterion!(none_disallow_typo,                  DISALLOW_OPTIONAL_WORDS,   DISALLOW_TYPOS);
#[rustfmt::skip]
test_criterion!(words_allow_typo,                    ALLOW_OPTIONAL_WORDS,      ALLOW_TYPOS,    Words);
#[rustfmt::skip]
test_criterion!(attribute_allow_typo,                DISALLOW_OPTIONAL_WORDS,   ALLOW_TYPOS,    Attribute);
#[rustfmt::skip]
test_criterion!(attribute_disallow_typo,             DISALLOW_OPTIONAL_WORDS,   DISALLOW_TYPOS, Attribute);
#[rustfmt::skip]
test_criterion!(exactness_allow_typo,                DISALLOW_OPTIONAL_WORDS,   ALLOW_TYPOS,    Exactness);
#[rustfmt::skip]
test_criterion!(exactness_disallow_typo,             DISALLOW_OPTIONAL_WORDS,   DISALLOW_TYPOS, Exactness);
#[rustfmt::skip]
test_criterion!(proximity_allow_typo,                DISALLOW_OPTIONAL_WORDS,   ALLOW_TYPOS,    Proximity);
#[rustfmt::skip]
test_criterion!(proximity_disallow_typo,             DISALLOW_OPTIONAL_WORDS,   DISALLOW_TYPOS, Proximity);
#[rustfmt::skip]
test_criterion!(asc_allow_typo,                      DISALLOW_OPTIONAL_WORDS,   ALLOW_TYPOS,    Asc(S("asc_desc_rank")));
#[rustfmt::skip]
test_criterion!(asc_disallow_typo,                   DISALLOW_OPTIONAL_WORDS,   DISALLOW_TYPOS, Asc(S("asc_desc_rank")));
#[rustfmt::skip]
test_criterion!(desc_allow_typo,                     DISALLOW_OPTIONAL_WORDS,   ALLOW_TYPOS,    Desc(S("asc_desc_rank")));
#[rustfmt::skip]
test_criterion!(desc_disallow_typo,                  DISALLOW_OPTIONAL_WORDS,   DISALLOW_TYPOS, Desc(S("asc_desc_rank")));
#[rustfmt::skip]
test_criterion!(asc_unexisting_field_allow_typo,     DISALLOW_OPTIONAL_WORDS,   ALLOW_TYPOS,    Asc(S("unexisting_field")));
#[rustfmt::skip]
test_criterion!(asc_unexisting_field_disallow_typo,  DISALLOW_OPTIONAL_WORDS,   DISALLOW_TYPOS, Asc(S("unexisting_field")));
#[rustfmt::skip]
test_criterion!(desc_unexisting_field_allow_typo,    DISALLOW_OPTIONAL_WORDS,   ALLOW_TYPOS,    Desc(S("unexisting_field")));
#[rustfmt::skip]
test_criterion!(desc_unexisting_field_disallow_typo, DISALLOW_OPTIONAL_WORDS,   DISALLOW_TYPOS, Desc(S("unexisting_field")));

#[test]
fn criteria_mixup() {
    use Criterion::*;
    let index = search::setup_search_index_with_criteria(&vec![
        Words,
        Attribute,
        Desc(S("asc_desc_rank")),
        Exactness,
        Proximity,
        Typo,
    ]);

    #[rustfmt::skip]
    let criteria_mix = {
        // Criterion doesn't implement Copy, we create a new Criterion using a closure
        let desc = || Desc(S("asc_desc_rank"));
        // all possible criteria order
        vec![
            vec![Words, Attribute,  desc(),     Exactness,  Proximity,  Typo],
            vec![Words, Attribute,  desc(),     Exactness,  Typo,       Proximity],
            vec![Words, Attribute,  desc(),     Proximity,  Exactness,  Typo],
            vec![Words, Attribute,  desc(),     Proximity,  Typo,       Exactness],
            vec![Words, Attribute,  desc(),     Typo,       Exactness,  Proximity],
            vec![Words, Attribute,  desc(),     Typo,       Proximity,  Exactness],
            vec![Words, Attribute,  Exactness,  desc(),     Proximity,  Typo],
            vec![Words, Attribute,  Exactness,  desc(),     Typo,       Proximity],
            vec![Words, Attribute,  Exactness,  Proximity,  desc(),     Typo],
            vec![Words, Attribute,  Exactness,  Proximity,  Typo,       desc()],
            vec![Words, Attribute,  Exactness,  Typo,       desc(),     Proximity],
            vec![Words, Attribute,  Exactness,  Typo,       Proximity,  desc()],
            vec![Words, Attribute,  Proximity,  desc(),     Exactness,  Typo],
            vec![Words, Attribute,  Proximity,  desc(),     Typo,       Exactness],
            vec![Words, Attribute,  Proximity,  Exactness,  desc(),     Typo],
            vec![Words, Attribute,  Proximity,  Exactness,  Typo,       desc()],
            vec![Words, Attribute,  Proximity,  Typo,       desc(),     Exactness],
            vec![Words, Attribute,  Proximity,  Typo,       Exactness,  desc()],
            vec![Words, Attribute,  Typo,       desc(),     Exactness,  Proximity],
            vec![Words, Attribute,  Typo,       desc(),     Proximity,  Exactness],
            vec![Words, Attribute,  Typo,       Exactness,  desc(),     Proximity],
            vec![Words, Attribute,  Typo,       Exactness,  Proximity,  desc()],
            vec![Words, Attribute,  Typo,       Proximity,  desc(),     Exactness],
            vec![Words, Attribute,  Typo,       Proximity,  Exactness,  desc()],
            vec![Words, desc(),     Attribute,  Exactness,  Proximity,  Typo],
            vec![Words, desc(),     Attribute,  Exactness,  Typo,       Proximity],
            vec![Words, desc(),     Attribute,  Proximity,  Exactness,  Typo],
            vec![Words, desc(),     Attribute,  Proximity,  Typo,       Exactness],
            vec![Words, desc(),     Attribute,  Typo,       Exactness,  Proximity],
            vec![Words, desc(),     Attribute,  Typo,       Proximity,  Exactness],
            vec![Words, desc(),     Exactness,  Attribute,  Proximity,  Typo],
            vec![Words, desc(),     Exactness,  Attribute,  Typo,       Proximity],
            vec![Words, desc(),     Exactness,  Proximity,  Attribute,  Typo],
            vec![Words, desc(),     Exactness,  Proximity,  Typo,       Attribute],
            vec![Words, desc(),     Exactness,  Typo,       Attribute,  Proximity],
            vec![Words, desc(),     Exactness,  Typo,       Proximity,  Attribute],
            vec![Words, desc(),     Proximity,  Attribute,  Exactness,  Typo],
            vec![Words, desc(),     Proximity,  Attribute,  Typo,       Exactness],
            vec![Words, desc(),     Proximity,  Exactness,  Attribute,  Typo],
            vec![Words, desc(),     Proximity,  Exactness,  Typo,       Attribute],
            vec![Words, desc(),     Proximity,  Typo,       Attribute,  Exactness],
            vec![Words, desc(),     Proximity,  Typo,       Exactness,  Attribute],
            vec![Words, desc(),     Typo,       Attribute,  Exactness,  Proximity],
            vec![Words, desc(),     Typo,       Attribute,  Proximity,  Exactness],
            vec![Words, desc(),     Typo,       Exactness,  Attribute,  Proximity],
            vec![Words, desc(),     Typo,       Exactness,  Proximity,  Attribute],
            vec![Words, desc(),     Typo,       Proximity,  Attribute,  Exactness],
            vec![Words, desc(),     Typo,       Proximity,  Exactness,  Attribute],
            vec![Words, Exactness,  Attribute,  desc(),     Proximity,  Typo],
            vec![Words, Exactness,  Attribute,  desc(),     Typo,       Proximity],
            vec![Words, Exactness,  Attribute,  Proximity,  desc(),     Typo],
            vec![Words, Exactness,  Attribute,  Proximity,  Typo,       desc()],
            vec![Words, Exactness,  Attribute,  Typo,       desc(),     Proximity],
            vec![Words, Exactness,  Attribute,  Typo,       Proximity,  desc()],
            vec![Words, Exactness,  desc(),     Attribute,  Proximity,  Typo],
            vec![Words, Exactness,  desc(),     Attribute,  Typo,       Proximity],
            vec![Words, Exactness,  desc(),     Proximity,  Attribute,  Typo],
            vec![Words, Exactness,  desc(),     Proximity,  Typo,       Attribute],
            vec![Words, Exactness,  desc(),     Typo,       Attribute,  Proximity],
            vec![Words, Exactness,  desc(),     Typo,       Proximity,  Attribute],
            vec![Words, Exactness,  Proximity,  Attribute,  desc(),     Typo],
            vec![Words, Exactness,  Proximity,  Attribute,  Typo,       desc()],
            vec![Words, Exactness,  Proximity,  desc(),     Attribute,  Typo],
            vec![Words, Exactness,  Proximity,  desc(),     Typo,       Attribute],
            vec![Words, Exactness,  Proximity,  Typo,       Attribute,  desc()],
            vec![Words, Exactness,  Proximity,  Typo,       desc(),     Attribute],
            vec![Words, Exactness,  Typo,       Attribute,  desc(),     Proximity],
            vec![Words, Exactness,  Typo,       Attribute,  Proximity,  desc()],
            vec![Words, Exactness,  Typo,       desc(),     Attribute,  Proximity],
            vec![Words, Exactness,  Typo,       desc(),     Proximity,  Attribute],
            vec![Words, Exactness,  Typo,       Proximity,  Attribute,  desc()],
            vec![Words, Exactness,  Typo,       Proximity,  desc(),     Attribute],
            vec![Words, Proximity,  Attribute,  desc(),     Exactness,  Typo],
            vec![Words, Proximity,  Attribute,  desc(),     Typo,       Exactness],
            vec![Words, Proximity,  Attribute,  Exactness,  desc(),     Typo],
            vec![Words, Proximity,  Attribute,  Exactness,  Typo,       desc()],
            vec![Words, Proximity,  Attribute,  Typo,       desc(),     Exactness],
            vec![Words, Proximity,  Attribute,  Typo,       Exactness,  desc()],
            vec![Words, Proximity,  desc(),     Attribute,  Exactness,  Typo],
            vec![Words, Proximity,  desc(),     Attribute,  Typo,       Exactness],
            vec![Words, Proximity,  desc(),     Exactness,  Attribute,  Typo],
            vec![Words, Proximity,  desc(),     Exactness,  Typo,       Attribute],
            vec![Words, Proximity,  desc(),     Typo,       Attribute,  Exactness],
            vec![Words, Proximity,  desc(),     Typo,       Exactness,  Attribute],
            vec![Words, Proximity,  Exactness,  Attribute,  desc(),     Typo],
            vec![Words, Proximity,  Exactness,  Attribute,  Typo,       desc()],
            vec![Words, Proximity,  Exactness,  desc(),     Attribute,  Typo],
            vec![Words, Proximity,  Exactness,  desc(),     Typo,       Attribute],
            vec![Words, Proximity,  Exactness,  Typo,       Attribute,  desc()],
            vec![Words, Proximity,  Exactness,  Typo,       desc(),     Attribute],
            vec![Words, Proximity,  Typo,       Attribute,  desc(),     Exactness],
            vec![Words, Proximity,  Typo,       Attribute,  Exactness,  desc()],
            vec![Words, Proximity,  Typo,       desc(),     Attribute,  Exactness],
            vec![Words, Proximity,  Typo,       desc(),     Exactness,  Attribute],
            vec![Words, Proximity,  Typo,       Exactness,  Attribute,  desc()],
            vec![Words, Proximity,  Typo,       Exactness,  desc(),     Attribute],
            vec![Words, Typo,       Attribute,  desc(),     Exactness,  Proximity],
            vec![Words, Typo,       Attribute,  desc(),     Proximity,  Exactness],
            vec![Words, Typo,       Attribute,  Exactness,  desc(),     Proximity],
            vec![Words, Typo,       Attribute,  Exactness,  Proximity,  desc()],
            vec![Words, Typo,       Attribute,  Proximity,  desc(),     Exactness],
            vec![Words, Typo,       Attribute,  Proximity,  Exactness,  desc()],
            vec![Words, Typo,       desc(),     Attribute,  Proximity,  Exactness],
            vec![Words, Typo,       desc(),     Exactness,  Attribute,  Proximity],
            vec![Words, Typo,       desc(),     Exactness,  Attribute,  Proximity],
            vec![Words, Typo,       desc(),     Exactness,  Proximity,  Attribute],
            vec![Words, Typo,       desc(),     Proximity,  Attribute,  Exactness],
            vec![Words, Typo,       desc(),     Proximity,  Exactness,  Attribute],
            vec![Words, Typo,       Exactness,  Attribute,  desc(),     Proximity],
            vec![Words, Typo,       Exactness,  Attribute,  Proximity,  desc()],
            vec![Words, Typo,       Exactness,  desc(),     Attribute,  Proximity],
            vec![Words, Typo,       Exactness,  desc(),     Proximity,  Attribute],
            vec![Words, Typo,       Exactness,  Proximity,  Attribute,  desc()],
            vec![Words, Typo,       Exactness,  Proximity,  desc(),     Attribute],
            vec![Words, Typo,       Proximity,  Attribute,  desc(),     Exactness],
            vec![Words, Typo,       Proximity,  Attribute,  Exactness,  desc()],
            vec![Words, Typo,       Proximity,  desc(),     Attribute,  Exactness],
            vec![Words, Typo,       Proximity,  desc(),     Exactness,  Attribute],
            vec![Words, Typo,       Proximity,  Exactness,  Attribute,  desc()],
            vec![Words, Typo,       Proximity,  Exactness,  desc(),     Attribute],
        ]
    };

    for criteria in criteria_mix {
        eprintln!("Testing with criteria order: {:?}", &criteria);
        //update criteria
        let mut wtxn = index.write_txn().unwrap();
        index.put_criteria(&mut wtxn, &criteria).unwrap();
        wtxn.commit().unwrap();

        let mut rtxn = index.read_txn().unwrap();

        let mut search = Search::new(&mut rtxn, &index);
        search.query(search::TEST_QUERY);
        search.limit(EXTERNAL_DOCUMENTS_IDS.len());
        search.optional_words(ALLOW_OPTIONAL_WORDS);
        search.authorize_typos(ALLOW_TYPOS);

        let SearchResult { documents_ids, .. } = search.execute().unwrap();

        let expected_external_ids: Vec<_> =
            search::expected_order(&criteria, ALLOW_OPTIONAL_WORDS, ALLOW_TYPOS)
                .into_iter()
                .map(|d| d.id)
                .collect();
        let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

        assert_eq!(documents_ids, expected_external_ids);
    }
}
