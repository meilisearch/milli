use std::borrow::Cow;
use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::{
    search::{word_derivations, WordDerivationsCache},
    FieldId, TreeLevel,
};
use crate::{DocumentId, Index};

use self::asc_desc::AscDesc;
use self::attribute::Attribute;
use self::exactness::Exactness;
use self::initial::Initial;
use self::proximity::Proximity;
use self::r#final::Final;
use self::typo::Typo;
use self::words::Words;
use super::query_tree::{Operation, PrimitiveQueryPart, Query, QueryKind};

mod asc_desc;
mod attribute;
mod exactness;
pub mod r#final;
mod initial;
mod proximity;
mod typo;
mod words;

pub trait Criterion {
    fn next(&mut self, params: &mut CriterionParameters)
        -> anyhow::Result<Option<CriterionResult>>;
}

/// The result of a call to the parent criterion.
#[derive(Debug, Clone, PartialEq)]
pub struct CriterionResult {
    /// The query tree that must be used by the children criterion to fetch candidates.
    query_tree: Option<Operation>,
    /// The candidates that this criterion is allowed to return subsets of,
    /// if None, it is up to the child to compute the candidates itself.
    candidates: Option<RoaringBitmap>,
    /// The candidates, coming from facet filters, that this criterion is allowed to return subsets of.
    filtered_candidates: Option<RoaringBitmap>,
    /// Candidates that comes from the current bucket of the initial criterion.
    bucket_candidates: Option<RoaringBitmap>,
}

#[derive(Debug, PartialEq)]
pub struct CriterionParameters<'a> {
    wdcache: &'a mut WordDerivationsCache,
    excluded_candidates: &'a RoaringBitmap,
}

/// Either a set of candidates that defines the candidates
/// that are allowed to be returned,
/// or the candidates that must never be returned.
#[derive(Debug)]
enum Candidates {
    Allowed(RoaringBitmap),
    Forbidden(RoaringBitmap),
}

impl Default for Candidates {
    fn default() -> Self {
        Self::Forbidden(RoaringBitmap::new())
    }
}

pub trait Context<'c> {
    fn documents_ids(&self) -> heed::Result<RoaringBitmap>;
    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;
    fn word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;
    fn word_pair_proximity_docids(
        &self,
        left: &str,
        right: &str,
        proximity: u8,
    ) -> heed::Result<Option<RoaringBitmap>>;
    fn word_prefix_pair_proximity_docids(
        &self,
        left: &str,
        right: &str,
        proximity: u8,
    ) -> heed::Result<Option<RoaringBitmap>>;
    fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>>;
    fn in_prefix_cache(&self, word: &str) -> bool;
    fn docid_words_positions(
        &self,
        docid: DocumentId,
    ) -> heed::Result<HashMap<String, RoaringBitmap>>;
    fn word_position_iterator(
        &self,
        word: &str,
        level: TreeLevel,
        in_prefix_cache: bool,
        left: Option<u32>,
        right: Option<u32>,
    ) -> heed::Result<
        Box<
            dyn Iterator<Item = heed::Result<((&'c str, TreeLevel, u32, u32), RoaringBitmap)>> + 'c,
        >,
    >;
    fn word_position_last_level(
        &self,
        word: &str,
        in_prefix_cache: bool,
    ) -> heed::Result<Option<TreeLevel>>;
    fn synonyms(&self, word: &str) -> heed::Result<Option<Vec<Vec<String>>>>;
    fn searchable_fields_ids(&self) -> heed::Result<Vec<FieldId>>;
    fn field_id_word_count_docids(
        &self,
        field_id: FieldId,
        word_count: u8,
    ) -> heed::Result<Option<RoaringBitmap>>;
    fn word_level_position_docids(
        &self,
        word: &str,
        level: TreeLevel,
        left: u32,
        right: u32,
    ) -> Result<Option<RoaringBitmap>, heed::Error>;
}
pub struct CriteriaBuilder<'t> {
    rtxn: &'t heed::RoTxn<'t>,
    index: &'t Index,
    words_fst: fst::Set<Cow<'t, [u8]>>,
    words_prefixes_fst: fst::Set<Cow<'t, [u8]>>,
}

impl<'c> Context<'c> for CriteriaBuilder<'c> {
    fn documents_ids(&self) -> heed::Result<RoaringBitmap> {
        self.index.documents_ids(self.rtxn)
    }

    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_docids.get(self.rtxn, &word)
    }

    fn word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_prefix_docids.get(self.rtxn, &word)
    }

    fn word_pair_proximity_docids(
        &self,
        left: &str,
        right: &str,
        proximity: u8,
    ) -> heed::Result<Option<RoaringBitmap>> {
        let key = (left, right, proximity);
        self.index.word_pair_proximity_docids.get(self.rtxn, &key)
    }

    fn word_prefix_pair_proximity_docids(
        &self,
        left: &str,
        right: &str,
        proximity: u8,
    ) -> heed::Result<Option<RoaringBitmap>> {
        let key = (left, right, proximity);
        self.index.word_prefix_pair_proximity_docids.get(self.rtxn, &key)
    }

    fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>> {
        &self.words_fst
    }

    fn in_prefix_cache(&self, word: &str) -> bool {
        self.words_prefixes_fst.contains(word)
    }

    fn docid_words_positions(
        &self,
        docid: DocumentId,
    ) -> heed::Result<HashMap<String, RoaringBitmap>> {
        let mut words_positions = HashMap::new();
        for result in self.index.docid_word_positions.prefix_iter(self.rtxn, &(docid, ""))? {
            let ((_, word), positions) = result?;
            words_positions.insert(word.to_string(), positions);
        }
        Ok(words_positions)
    }

    fn word_position_iterator(
        &self,
        word: &str,
        level: TreeLevel,
        in_prefix_cache: bool,
        left: Option<u32>,
        right: Option<u32>,
    ) -> heed::Result<
        Box<
            dyn Iterator<Item = heed::Result<((&'c str, TreeLevel, u32, u32), RoaringBitmap)>> + 'c,
        >,
    > {
        let range = {
            let left = left.unwrap_or(u32::min_value());
            let right = right.unwrap_or(u32::max_value());
            let left = (word, level, left, left);
            let right = (word, level, right, right);
            left..=right
        };
        let db = match in_prefix_cache {
            true => self.index.word_prefix_level_position_docids,
            false => self.index.word_level_position_docids,
        };

        Ok(Box::new(db.range(self.rtxn, &range)?))
    }

    fn word_position_last_level(
        &self,
        word: &str,
        in_prefix_cache: bool,
    ) -> heed::Result<Option<TreeLevel>> {
        let range = {
            let left = (word, TreeLevel::min_value(), u32::min_value(), u32::min_value());
            let right = (word, TreeLevel::max_value(), u32::max_value(), u32::max_value());
            left..=right
        };
        let db = match in_prefix_cache {
            true => self.index.word_prefix_level_position_docids,
            false => self.index.word_level_position_docids,
        };
        let last_level = db
            .remap_data_type::<heed::types::DecodeIgnore>()
            .range(self.rtxn, &range)?
            .last()
            .transpose()?
            .map(|((_, level, _, _), _)| level);

        Ok(last_level)
    }

    fn synonyms(&self, word: &str) -> heed::Result<Option<Vec<Vec<String>>>> {
        self.index.words_synonyms(self.rtxn, &[word])
    }

    fn searchable_fields_ids(&self) -> heed::Result<Vec<FieldId>> {
        match self.index.searchable_fields_ids(self.rtxn)? {
            Some(searchable_fields_ids) => Ok(searchable_fields_ids),
            None => Ok(self.index.fields_ids_map(self.rtxn)?.ids().collect()),
        }
    }

    fn field_id_word_count_docids(
        &self,
        field_id: FieldId,
        word_count: u8,
    ) -> heed::Result<Option<RoaringBitmap>> {
        let key = (field_id, word_count);
        self.index.field_id_word_count_docids.get(self.rtxn, &key)
    }

    fn word_level_position_docids(
        &self,
        word: &str,
        level: TreeLevel,
        left: u32,
        right: u32,
    ) -> Result<Option<RoaringBitmap>, heed::Error> {
        let key = (word, level, left, right);
        self.index.word_level_position_docids.get(self.rtxn, &key)
    }
}

impl<'t> CriteriaBuilder<'t> {
    pub fn new(rtxn: &'t heed::RoTxn<'t>, index: &'t Index) -> anyhow::Result<Self> {
        let words_fst = index.words_fst(rtxn)?;
        let words_prefixes_fst = index.words_prefixes_fst(rtxn)?;
        Ok(Self { rtxn, index, words_fst, words_prefixes_fst })
    }

    pub fn build(
        &'t self,
        query_tree: Option<Operation>,
        primitive_query: Option<Vec<PrimitiveQueryPart>>,
        filtered_candidates: Option<RoaringBitmap>,
    ) -> anyhow::Result<Final<'t>> {
        use crate::criterion::Criterion as Name;

        let primitive_query = primitive_query.unwrap_or_default();

        let mut criterion =
            Box::new(Initial::new(query_tree, filtered_candidates)) as Box<dyn Criterion>;
        for name in self.index.criteria(&self.rtxn)? {
            criterion = match name {
                Name::Typo => Box::new(Typo::new(self, criterion)),
                Name::Words => Box::new(Words::new(self, criterion)),
                Name::Proximity => Box::new(Proximity::new(self, criterion)),
                Name::Attribute => Box::new(Attribute::new(self, criterion)),
                Name::Exactness => Box::new(Exactness::new(self, criterion, &primitive_query)?),
                Name::Asc(field) => {
                    Box::new(AscDesc::asc(&self.index, &self.rtxn, criterion, field)?)
                }
                Name::Desc(field) => {
                    Box::new(AscDesc::desc(&self.index, &self.rtxn, criterion, field)?)
                }
            };
        }

        Ok(Final::new(self, criterion))
    }
}

pub fn resolve_query_tree<'t>(
    ctx: &'t dyn Context,
    query_tree: &Operation,
    wdcache: &mut WordDerivationsCache,
) -> anyhow::Result<RoaringBitmap> {
    fn resolve_operation<'t>(
        ctx: &'t dyn Context,
        query_tree: &Operation,
        wdcache: &mut WordDerivationsCache,
    ) -> anyhow::Result<RoaringBitmap> {
        use Operation::{And, Or, Phrase, Query};

        match query_tree {
            And(ops) => {
                let mut ops = ops
                    .iter()
                    .map(|op| resolve_operation(ctx, op, wdcache))
                    .collect::<anyhow::Result<Vec<_>>>()?;

                ops.sort_unstable_by_key(|cds| cds.len());

                let mut candidates = RoaringBitmap::new();
                let mut first_loop = true;
                for docids in ops {
                    if first_loop {
                        candidates = docids;
                        first_loop = false;
                    } else {
                        candidates.intersect_with(&docids);
                    }
                }
                Ok(candidates)
            }
            Phrase(words) => {
                let mut candidates = RoaringBitmap::new();
                let mut first_loop = true;
                for slice in words.windows(2) {
                    let (left, right) = (&slice[0], &slice[1]);
                    match ctx.word_pair_proximity_docids(left, right, 1)? {
                        Some(pair_docids) => {
                            if pair_docids.is_empty() {
                                return Ok(RoaringBitmap::new());
                            } else if first_loop {
                                candidates = pair_docids;
                                first_loop = false;
                            } else {
                                candidates &= pair_docids;
                            }
                        }
                        None => return Ok(RoaringBitmap::new()),
                    }
                }
                Ok(candidates)
            }
            Or(_, ops) => {
                let mut candidates = RoaringBitmap::new();
                for op in ops {
                    let docids = resolve_operation(ctx, op, wdcache)?;
                    candidates.union_with(&docids);
                }
                Ok(candidates)
            }
            Query(q) => Ok(query_docids(ctx, q, wdcache)?),
        }
    }

    resolve_operation(ctx, query_tree, wdcache)
}

fn all_word_pair_proximity_docids<T: AsRef<str>, U: AsRef<str>>(
    ctx: &dyn Context,
    left_words: &[(T, u8)],
    right_words: &[(U, u8)],
    proximity: u8,
) -> anyhow::Result<RoaringBitmap> {
    let mut docids = RoaringBitmap::new();
    for (left, _l_typo) in left_words {
        for (right, _r_typo) in right_words {
            let current_docids = ctx
                .word_pair_proximity_docids(left.as_ref(), right.as_ref(), proximity)?
                .unwrap_or_default();
            docids.union_with(&current_docids);
        }
    }
    Ok(docids)
}

fn query_docids(
    ctx: &dyn Context,
    query: &Query,
    wdcache: &mut WordDerivationsCache,
) -> anyhow::Result<RoaringBitmap> {
    match &query.kind {
        QueryKind::Exact { word, .. } => {
            if query.prefix && ctx.in_prefix_cache(&word) {
                Ok(ctx.word_prefix_docids(&word)?.unwrap_or_default())
            } else if query.prefix {
                let words = word_derivations(&word, true, 0, ctx.words_fst(), wdcache)?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    let current_docids = ctx.word_docids(&word)?.unwrap_or_default();
                    docids.union_with(&current_docids);
                }
                Ok(docids)
            } else {
                Ok(ctx.word_docids(&word)?.unwrap_or_default())
            }
        }
        QueryKind::Tolerant { typo, word } => {
            let words = word_derivations(&word, query.prefix, *typo, ctx.words_fst(), wdcache)?;
            let mut docids = RoaringBitmap::new();
            for (word, _typo) in words {
                let current_docids = ctx.word_docids(&word)?.unwrap_or_default();
                docids.union_with(&current_docids);
            }
            Ok(docids)
        }
    }
}

fn query_pair_proximity_docids(
    ctx: &dyn Context,
    left: &Query,
    right: &Query,
    proximity: u8,
    wdcache: &mut WordDerivationsCache,
) -> anyhow::Result<RoaringBitmap> {
    if proximity >= 8 {
        let mut candidates = query_docids(ctx, left, wdcache)?;
        let right_candidates = query_docids(ctx, right, wdcache)?;
        candidates.intersect_with(&right_candidates);
        return Ok(candidates);
    }

    let prefix = right.prefix;
    match (&left.kind, &right.kind) {
        (QueryKind::Exact { word: left, .. }, QueryKind::Exact { word: right, .. }) => {
            if prefix && ctx.in_prefix_cache(&right) {
                Ok(ctx
                    .word_prefix_pair_proximity_docids(left.as_str(), right.as_str(), proximity)?
                    .unwrap_or_default())
            } else if prefix {
                let r_words = word_derivations(&right, true, 0, ctx.words_fst(), wdcache)?;
                all_word_pair_proximity_docids(ctx, &[(left, 0)], &r_words, proximity)
            } else {
                Ok(ctx
                    .word_pair_proximity_docids(left.as_str(), right.as_str(), proximity)?
                    .unwrap_or_default())
            }
        }
        (QueryKind::Tolerant { typo, word: left }, QueryKind::Exact { word: right, .. }) => {
            let l_words =
                word_derivations(&left, false, *typo, ctx.words_fst(), wdcache)?.to_owned();
            if prefix && ctx.in_prefix_cache(&right) {
                let mut docids = RoaringBitmap::new();
                for (left, _) in l_words {
                    let current_docids = ctx
                        .word_prefix_pair_proximity_docids(
                            left.as_ref(),
                            right.as_ref(),
                            proximity,
                        )?
                        .unwrap_or_default();
                    docids.union_with(&current_docids);
                }
                Ok(docids)
            } else if prefix {
                let r_words = word_derivations(&right, true, 0, ctx.words_fst(), wdcache)?;
                all_word_pair_proximity_docids(ctx, &l_words, &r_words, proximity)
            } else {
                all_word_pair_proximity_docids(ctx, &l_words, &[(right, 0)], proximity)
            }
        }
        (QueryKind::Exact { word: left, .. }, QueryKind::Tolerant { typo, word: right }) => {
            let r_words = word_derivations(&right, prefix, *typo, ctx.words_fst(), wdcache)?;
            all_word_pair_proximity_docids(ctx, &[(left, 0)], &r_words, proximity)
        }
        (
            QueryKind::Tolerant { typo: l_typo, word: left },
            QueryKind::Tolerant { typo: r_typo, word: right },
        ) => {
            let l_words =
                word_derivations(&left, false, *l_typo, ctx.words_fst(), wdcache)?.to_owned();
            let r_words = word_derivations(&right, prefix, *r_typo, ctx.words_fst(), wdcache)?;
            all_word_pair_proximity_docids(ctx, &l_words, &r_words, proximity)
        }
    }
}

#[cfg(test)]
pub mod test {
    use maplit::hashmap;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    use super::*;
    use std::collections::HashMap;

    fn s(s: &str) -> String {
        s.to_string()
    }
    pub struct TestContext<'t> {
        words_fst: fst::Set<Cow<'t, [u8]>>,
        word_docids: HashMap<String, RoaringBitmap>,
        word_prefix_docids: HashMap<String, RoaringBitmap>,
        word_pair_proximity_docids: HashMap<(String, String, i32), RoaringBitmap>,
        word_prefix_pair_proximity_docids: HashMap<(String, String, i32), RoaringBitmap>,
        docid_words: HashMap<u32, Vec<String>>,
    }

    impl<'c> Context<'c> for TestContext<'c> {
        fn documents_ids(&self) -> heed::Result<RoaringBitmap> {
            Ok(self.word_docids.iter().fold(RoaringBitmap::new(), |acc, (_, docids)| acc | docids))
        }

        fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
            Ok(self.word_docids.get(&word.to_string()).cloned())
        }

        fn word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
            Ok(self.word_prefix_docids.get(&word.to_string()).cloned())
        }

        fn word_pair_proximity_docids(
            &self,
            left: &str,
            right: &str,
            proximity: u8,
        ) -> heed::Result<Option<RoaringBitmap>> {
            let key = (left.to_string(), right.to_string(), proximity.into());
            Ok(self.word_pair_proximity_docids.get(&key).cloned())
        }

        fn word_prefix_pair_proximity_docids(
            &self,
            left: &str,
            right: &str,
            proximity: u8,
        ) -> heed::Result<Option<RoaringBitmap>> {
            let key = (left.to_string(), right.to_string(), proximity.into());
            Ok(self.word_prefix_pair_proximity_docids.get(&key).cloned())
        }

        fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>> {
            &self.words_fst
        }

        fn in_prefix_cache(&self, word: &str) -> bool {
            self.word_prefix_docids.contains_key(&word.to_string())
        }

        fn docid_words_positions(
            &self,
            docid: DocumentId,
        ) -> heed::Result<HashMap<String, RoaringBitmap>> {
            if let Some(docid_words) = self.docid_words.get(&docid) {
                Ok(docid_words
                    .iter()
                    .enumerate()
                    .map(|(i, w)| {
                        (w.clone(), RoaringBitmap::from_sorted_iter(std::iter::once(i as u32)))
                    })
                    .collect())
            } else {
                Ok(HashMap::new())
            }
        }

        fn word_position_iterator(
            &self,
            _word: &str,
            _level: TreeLevel,
            _in_prefix_cache: bool,
            _left: Option<u32>,
            _right: Option<u32>,
        ) -> heed::Result<
            Box<
                dyn Iterator<Item = heed::Result<((&'c str, TreeLevel, u32, u32), RoaringBitmap)>>
                    + 'c,
            >,
        > {
            todo!()
        }

        fn word_position_last_level(
            &self,
            _word: &str,
            _in_prefix_cache: bool,
        ) -> heed::Result<Option<TreeLevel>> {
            todo!()
        }

        fn synonyms(&self, _word: &str) -> heed::Result<Option<Vec<Vec<String>>>> {
            todo!()
        }

        fn searchable_fields_ids(&self) -> heed::Result<Vec<FieldId>> {
            todo!()
        }

        fn word_level_position_docids(
            &self,
            _word: &str,
            _level: TreeLevel,
            _left: u32,
            _right: u32,
        ) -> Result<Option<RoaringBitmap>, heed::Error> {
            todo!()
        }

        fn field_id_word_count_docids(
            &self,
            _field_id: FieldId,
            _word_count: u8,
        ) -> heed::Result<Option<RoaringBitmap>> {
            todo!()
        }
    }

    impl<'a> Default for TestContext<'a> {
        fn default() -> TestContext<'a> {
            let mut rng = StdRng::seed_from_u64(102);
            let rng = &mut rng;

            fn random_postings<R: Rng>(rng: &mut R, len: usize) -> RoaringBitmap {
                let mut values = Vec::<u32>::with_capacity(len);
                while values.len() != len {
                    values.push(rng.gen());
                }
                values.sort_unstable();

                RoaringBitmap::from_sorted_iter(values.into_iter())
            }

            let word_docids = hashmap! {
                s("hello")      => random_postings(rng,   1500),
                s("hi")         => random_postings(rng,   4000),
                s("word")       => random_postings(rng,   2500),
                s("split")      => random_postings(rng,    400),
                s("ngrams")     => random_postings(rng,   1400),
                s("world")      => random_postings(rng, 15_000),
                s("earth")      => random_postings(rng,   8000),
                s("2021")       => random_postings(rng,    100),
                s("2020")       => random_postings(rng,    500),
                s("is")         => random_postings(rng, 50_000),
                s("this")       => random_postings(rng, 50_000),
                s("good")       => random_postings(rng,   1250),
                s("morning")    => random_postings(rng,    125),
            };

            let mut docid_words = HashMap::new();
            for (word, docids) in word_docids.iter() {
                for docid in docids {
                    let words = docid_words.entry(docid).or_insert(vec![]);
                    words.push(word.clone());
                }
            }

            let word_prefix_docids = hashmap! {
                s("h")   => &word_docids[&s("hello")] | &word_docids[&s("hi")],
                s("wor") => &word_docids[&s("word")]  | &word_docids[&s("world")],
                s("20")  => &word_docids[&s("2020")]  | &word_docids[&s("2021")],
            };

            let mut word_pair_proximity_docids = HashMap::new();
            let mut word_prefix_pair_proximity_docids = HashMap::new();
            for (lword, lcandidates) in &word_docids {
                for (rword, rcandidates) in &word_docids {
                    if lword == rword {
                        continue;
                    }
                    let candidates = lcandidates & rcandidates;
                    for candidate in candidates {
                        if let Some(docid_words) = docid_words.get(&candidate) {
                            let lposition = docid_words.iter().position(|w| w == lword).unwrap();
                            let rposition = docid_words.iter().position(|w| w == rword).unwrap();
                            let key = if lposition < rposition {
                                (s(lword), s(rword), (rposition - lposition) as i32)
                            } else {
                                (s(lword), s(rword), (lposition - rposition + 1) as i32)
                            };
                            let docids = word_pair_proximity_docids
                                .entry(key)
                                .or_insert(RoaringBitmap::new());
                            docids.push(candidate);
                        }
                    }
                }
                for (pword, pcandidates) in &word_prefix_docids {
                    if lword.starts_with(pword) {
                        continue;
                    }
                    let candidates = lcandidates & pcandidates;
                    for candidate in candidates {
                        if let Some(docid_words) = docid_words.get(&candidate) {
                            let lposition = docid_words.iter().position(|w| w == lword).unwrap();
                            let rposition =
                                docid_words.iter().position(|w| w.starts_with(pword)).unwrap();
                            let key = if lposition < rposition {
                                (s(lword), s(pword), (rposition - lposition) as i32)
                            } else {
                                (s(lword), s(pword), (lposition - rposition + 1) as i32)
                            };
                            let docids = word_prefix_pair_proximity_docids
                                .entry(key)
                                .or_insert(RoaringBitmap::new());
                            docids.push(candidate);
                        }
                    }
                }
            }

            let mut keys = word_docids.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let words_fst = fst::Set::from_iter(keys).unwrap().map_data(|v| Cow::Owned(v)).unwrap();

            TestContext {
                words_fst,
                word_docids,
                word_prefix_docids,
                word_pair_proximity_docids,
                word_prefix_pair_proximity_docids,
                docid_words,
            }
        }
    }
}
