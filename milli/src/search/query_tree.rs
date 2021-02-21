#![allow(unused)]

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::{fmt, cmp, mem};

use meilisearch_tokenizer::{TokenKind, tokenizer::TokenStream};
use roaring::RoaringBitmap;
use slice_group_by::GroupBy;

use crate::Index;

type IsOptionalWord = bool;
type IsPrefix = bool;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Operation {
    And(Vec<Operation>),
    Consecutive(Vec<Operation>),
    Or(IsOptionalWord, Vec<Operation>),
    Query(Query),
}

impl fmt::Debug for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn pprint_tree(f: &mut fmt::Formatter<'_>, op: &Operation, depth: usize) -> fmt::Result {
            match op {
                Operation::And(children) => {
                    writeln!(f, "{:1$}AND", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                },
                Operation::Consecutive(children) => {
                    writeln!(f, "{:1$}CONSECUTIVE", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                },
                Operation::Or(true, children) => {
                    writeln!(f, "{:1$}OR(WORD)", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                },
                Operation::Or(false, children) => {
                    writeln!(f, "{:1$}OR", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                },
                Operation::Query(query) => writeln!(f, "{:2$}{:?}", "", query, depth * 2),
            }
        }

        pprint_tree(f, self, 0)
    }
}

impl Operation {
    fn tolerant(prefix: IsPrefix, s: &str) -> Operation {
        Operation::Query(Query { prefix, kind: QueryKind::tolerant(2, s.to_string()) })
    }

    fn exact(prefix: IsPrefix, s: &str) -> Operation {
        Operation::Query(Query { prefix, kind: QueryKind::exact(s.to_string()) })
    }

    fn phrase(words: Vec<String>) -> Operation {
        Operation::consecutive(
            words.into_iter().map(|s| {
                Operation::Query(Query { prefix: false, kind: QueryKind::exact(s) })
            }).collect()
        )
    }

    fn and(mut ops: Vec<Self>) -> Self {
        if ops.len() == 1 {
            ops.pop().unwrap()
        } else {
            Self::And(ops)
        }
    }

    pub fn or(word_branch: IsOptionalWord, mut ops: Vec<Self>) -> Self {
        if ops.len() == 1 {
            ops.pop().unwrap()
        } else {
            Self::Or(word_branch, ops)
        }
    }

    fn consecutive(mut ops: Vec<Self>) -> Self {
        if ops.len() == 1 {
            ops.pop().unwrap()
        } else {
            Self::Consecutive(ops)
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Query {
    pub prefix: IsPrefix,
    pub kind: QueryKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QueryKind {
    Tolerant { typo: u8, word: String },
    Exact { original_typo: u8, word: String },
}

impl QueryKind {
    fn exact(word: String) -> Self {
        QueryKind::Exact { original_typo: 0, word }
    }

    fn tolerant(typo: u8, word: String) -> Self {
        QueryKind::Tolerant { typo, word }
    }

    pub fn typo(&self) -> u8 {
        match self {
            QueryKind::Tolerant { typo, .. } => *typo,
            QueryKind::Exact { original_typo, .. } => *original_typo,
        }
    }

    pub fn word(&self) -> &str {
        match self {
            QueryKind::Tolerant { word, .. } => word,
            QueryKind::Exact { word, .. } => word,
        }
    }
}

impl fmt::Debug for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Query { prefix, kind } = self;
        let prefix = if *prefix { String::from("Prefix") } else { String::default() };
        match kind {
            QueryKind::Exact { word, .. } => {
                f.debug_struct(&(prefix + "Exact")).field("word", &word).finish()
            },
            QueryKind::Tolerant { typo, word } => {
                f.debug_struct(&(prefix + "Tolerant")).field("word", &word).field("max typo", &typo).finish()
            },
        }
    }
}

trait Context {
    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;
    fn synonyms<S: AsRef<str>>(&self, words: &[S]) -> heed::Result<Option<Vec<Vec<String>>>>;
}

/// The query tree builder is the interface to build a query tree.
pub struct QueryTreeBuilder<'a> {
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> Context for QueryTreeBuilder<'a> {
    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_docids.get(self.rtxn, word)
    }

    fn synonyms<S: AsRef<str>>(&self, _words: &[S]) -> heed::Result<Option<Vec<Vec<String>>>> {
        Ok(None)
    }
}

impl<'a> QueryTreeBuilder<'a> {
    /// Create a `QueryTreeBuilder` from a heed ReadOnly transaction `rtxn`
    /// and an Index `index`.
    pub fn new(rtxn: &'a heed::RoTxn<'a>, index: &'a Index) -> Self {
        Self { rtxn, index }
    }

    /// Build the query tree:
    /// - if `optional_words` is set to `false` the query tree will be
    ///   generated forcing all query words to be present in each matching documents
    ///   (the criterion `words` will be ignored)
    /// - if `authorize_typos` is set to `false` the query tree will be generated
    ///   forcing all query words to match documents without any typo
    ///   (the criterion `typo` will be ignored)
    pub fn build(&self, optional_words: bool, authorize_typos: bool, query: TokenStream) -> Option<Operation> {
        let primitive_query = create_primitive_query(query);
        if !primitive_query.is_empty() {
            Some(create_query_tree(self, optional_words, authorize_typos, primitive_query))
        } else {
            None
        }
    }
}

/// Split the word depending the frequency in documents of subwords.
fn split_best_frequency<'a>(ctx: &impl Context, word: &'a str) -> heed::Result<Option<Operation>> {
    let chars = word.char_indices().skip(1);
    let mut best = None;

    for (i, _) in chars {
        let (left, right) = word.split_at(i);

        let left_freq = ctx.word_docids(left)?.map(|docids| docids.len()).unwrap_or(0);
        let right_freq = ctx.word_docids(right)?.map(|docids| docids.len()).unwrap_or(0);

        let min_freq = cmp::min(left_freq, right_freq);
        if min_freq != 0 && best.map_or(true, |(old, _, _)| min_freq > old) {
            best = Some((min_freq, left, right));
        }
    }

    Ok(best.map(|(_, left, right)| Operation::Consecutive(
        vec![
            Operation::Query(Query { prefix: false, kind: QueryKind::exact(left.to_string()) }),
            Operation::Query(Query { prefix: false, kind: QueryKind::exact(right.to_string()) })
        ]
    )))
}

/// Return the `QueryKind` of a word depending on `authorize_typos`
/// and the provided word length.
fn typos(word: String, authorize_typos: bool) -> QueryKind {
    if authorize_typos {
        match word.len() {
            0..=4 => QueryKind::exact(word),
            5..=8 => QueryKind::tolerant(1, word),
            _     => QueryKind::tolerant(2, word),
        }
    } else {
        QueryKind::exact(word)
    }
}

/// Fetch synonyms from the `Context` for the provided word
/// and create the list of operations for the query tree
fn synonyms(ctx: &impl Context, word: &[&str]) -> heed::Result<Option<Vec<Operation>>> {
    let synonyms = ctx.synonyms(word)?;

    Ok(synonyms.map(|synonyms| {
        synonyms.into_iter().map(|synonym| {
            let words = synonym.into_iter().map(|word| {
                Operation::Query(Query { prefix: false, kind: QueryKind::exact(word) })
            }).collect();
            Operation::and(words)
        }).collect()
    }))
}

/// The query tree builder is the interface to build a query tree.
pub struct MatchingWords {
    inner: BTreeMap<String, IsPrefix>
}

impl MatchingWords {
    /// List all words which can be considered as a match for the query tree.
    pub fn from_query_tree(tree: &Operation, fst: &fst::Set<Cow<[u8]>>) -> Self {
        Self { inner: fetch_words(tree, fst).into_iter().collect() }
    }

    /// Return true if the word match.
    pub fn is_match(&self, word: &str) -> bool {
        fn first_char(s: &str) -> Option<&str> {
            s.chars().next().map(|c| &s[..c.len_utf8()])
        }

        match first_char(word) {
            Some(first) => {
                let left = first.to_owned();
                let right = word.to_owned();
                self.inner.range(left..=right).any(|(w, is_prefix)| *is_prefix || *w == word)
            },
            None => false
        }
    }
}

type FetchedWords = Vec<(String, IsPrefix)>;

/// Lists all words which can be considered as a match for the query tree.
fn fetch_words(tree: &Operation, fst: &fst::Set<Cow<[u8]>>) -> FetchedWords {
    fn resolve_branch(tree: &[Operation], fst: &fst::Set<Cow<[u8]>>) -> FetchedWords {
        tree.iter().map(|op| resolve_ops(op, fst)).flatten().collect()
    }

    fn resolve_query(query: &Query, fst: &fst::Set<Cow<[u8]>>) -> FetchedWords {
        match query.kind.clone() {
            QueryKind::Exact { word, .. } => vec![(word, query.prefix)],
            QueryKind::Tolerant { typo, word } => {
                if let Ok(words) = super::word_typos(&word, query.prefix, typo, fst) {
                    words.into_iter().map(|(w, _)| (w, query.prefix)).collect()
                } else {
                    vec![(word, query.prefix)]
                }
            }
        }
    }

    fn resolve_ops(tree: &Operation, fst: &fst::Set<Cow<[u8]>>) -> FetchedWords {
        match tree {
            Operation::Or(_, ops) | Operation::And(ops) | Operation::Consecutive(ops) => {
                resolve_branch(ops.as_slice(), fst)
            },
            Operation::Query(ops) => {
                resolve_query(ops, fst)
            },
        }
    }

    let mut words = resolve_ops(tree, fst);
    words.sort_unstable();
    words.dedup();
    words
}

/// Main function that create the final query tree from the primitive query.
fn create_query_tree(ctx: &impl Context, optional_words: bool, authorize_typos: bool, query: PrimitiveQuery) -> Operation {
    /// This function match on the `PrimitiveQueryPart` create an operation from it.
    fn resolve_primitive_part(ctx: &impl Context, authorize_typos: bool, part: PrimitiveQueryPart) -> Operation {
        match part {
            // 1. try to split word in 2
            // 2. try to fetch synonyms
            // 3. create an operation containing the word
            // 4. wrap all in an OR operation
            PrimitiveQueryPart::Word(word, is_prefix) => {
                // TODO: handle Error
                let split = split_best_frequency(ctx, &word).ok().flatten();
                // TODO: handle Error
                let synonyms = synonyms(ctx, &[&word]).ok().flatten();
                let mut children = synonyms.unwrap_or_else(|| Vec::with_capacity(2));
                if let Some(child) = split { children.push(child); }
                children.push(Operation::Query(Query { prefix: is_prefix, kind: typos(word, authorize_typos) }));
                Operation::or(false, children)
            },
            // create a CONSECUTIVE operation wrapping all word in the phrase
            PrimitiveQueryPart::Phrase(words) => {
                Operation::phrase(words)
            },
        }
    }

    /// Create all ngrams 1..=3 generating query tree branch,
    /// the query `many the fish` will create a tree.
    fn ngrams(ctx: &impl Context, authorize_typos: bool, query: &[PrimitiveQueryPart]) -> Operation {
        const MAX_NGRAM: usize = 3;
        let mut op_children = Vec::new();

        for sub_query in query.linear_group_by(|a, b| !(a.is_phrase() || b.is_phrase()) ) {
            let mut or_op_children = Vec::new();

            for ngram in 1..=MAX_NGRAM.min(sub_query.len()) {
                if let Some(group) = sub_query.get(..ngram) {
                    let mut and_op_children = Vec::new();
                    let tail = &sub_query[ngram..];
                    let is_last = tail.is_empty();

                    match group {
                        [part] => {
                            and_op_children.push(resolve_primitive_part(ctx, authorize_typos, part.clone()));
                        },
                        words => {
                            let is_prefix = words.last().map(|part| part.is_prefix()).unwrap_or(false);
                            let words: Vec<_> = words.iter().filter_map(| part| {
                                if let PrimitiveQueryPart::Word(word, _) = part {
                                    Some(word.as_str())
                                } else {
                                    None
                                }
                            }).collect();

                            // TODO: handle Error
                            let mut operations = synonyms(ctx, &words).ok().flatten().unwrap_or_else(|| Vec::with_capacity(1));

                            let concat = words.concat();
                            let query = Query { prefix: is_prefix, kind: typos(concat, authorize_typos) };
                            operations.push(Operation::Query(query));
                            and_op_children.push(Operation::or(false, operations));
                        }
                    }

                    if !is_last {
                        and_op_children.push(ngrams(ctx, authorize_typos, tail));
                    }
                    or_op_children.push(Operation::and(and_op_children));
                }
            }
            op_children.push(Operation::or(false, or_op_children));
        }

        Operation::and(op_children)
    }

    /// Create a new branch removing the last non-phrase query part,
    /// the query `many the fish` will create a tree.
    fn optional_word(ctx: &impl Context, authorize_typos: bool, query: PrimitiveQuery) -> Operation {
        let word_count = query.iter().filter(|part| !part.is_phrase()).count();
        let mut operation_children = Vec::new();

        for count in (1..=word_count).rev() {
            let mut tmp_count = 0;

            // keep only the N firsts non-quoted words, where N = count
            // quoted words are allways kept
            let query: Vec<_> = query.iter().cloned().filter(|part| {
                if !part.is_phrase()  {
                    tmp_count += 1;
                    tmp_count <= count
                } else { true }
            }).collect();

            operation_children.push(ngrams(ctx, authorize_typos, query.as_slice()));
        }

        Operation::or(true, operation_children)
    }

    if optional_words {
        optional_word(ctx, authorize_typos, query)
    } else {
        ngrams(ctx, authorize_typos, query.as_slice())
    }
}

type PrimitiveQuery = Vec<PrimitiveQueryPart>;

#[derive(Debug, Clone)]
enum PrimitiveQueryPart {
    Phrase(Vec<String>),
    Word(String, IsPrefix),
}

impl PrimitiveQueryPart {
    fn is_phrase(&self) -> bool {
        matches!(self, Self::Phrase(_))
    }

    fn is_prefix(&self) -> bool {
        matches!(self, Self::Word(_, is_prefix) if *is_prefix)
    }
}

/// Create primitive query from tokenized query string,
/// the primitive query is an intermediate state to build the query tree.
fn create_primitive_query(query: TokenStream) -> PrimitiveQuery {
    let mut primitive_query = Vec::new();
    let mut phrase = Vec::new();
    let mut quoted = false;

    let mut peekable = query.peekable();
    while let Some(token) = peekable.next() {
        match token.kind {
            TokenKind::Word => {
                // 1. if the word is quoted we push it in a phrase-buffer waiting for the ending quote,
                // 2. if the word is not the last token of the query we push it as a non-prefix word,
                // 3. if the word is the last token of the query we push it as a prefix word.
                if quoted {
                    phrase.push(token.word.to_string());
                } else if peekable.peek().is_some() {
                    primitive_query.push(PrimitiveQueryPart::Word(token.word.to_string(), false));
                } else {
                    primitive_query.push(PrimitiveQueryPart::Word(token.word.to_string(), true));
                }
            },
            TokenKind::Separator(_) => {
                let quote_count = token.word.chars().filter(|&s| s == '"').count();
                // swap quoted state if we encounter a double quote
                if quote_count % 2 != 0 {
                    quoted = !quoted;
                }
                if !phrase.is_empty() && quote_count > 0 {
                    primitive_query.push(PrimitiveQueryPart::Phrase(mem::take(&mut phrase)));
                }
            },
            _ => (),
        }
    }

    // If a quote is never closed, we consider all of the end of the query as a phrase.
    if !phrase.is_empty() {
        primitive_query.push(PrimitiveQueryPart::Phrase(mem::take(&mut phrase)));
    }

    primitive_query
}

#[cfg(test)]
mod test {
    use fst::Set;
    use maplit::hashmap;
    use meilisearch_tokenizer::{Analyzer, AnalyzerConfig};
    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::*;
    use std::collections::HashMap;
    #[derive(Debug)]
    struct TestContext {
        synonyms: HashMap<Vec<String>, Vec<Vec<String>>>,
        postings: HashMap<String, RoaringBitmap>,
    }

    impl TestContext {
        fn build(&self, optional_words: bool, authorize_typos: bool, query: TokenStream) -> Option<Operation> {
            let primitive_query = create_primitive_query(query);
            if !primitive_query.is_empty() {
                Some(create_query_tree(self, optional_words, authorize_typos, primitive_query))
            } else {
                None
            }
        }
    }

    impl Context for TestContext {
        fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
            Ok(self.postings.get(word).cloned())
        }

        fn synonyms<S: AsRef<str>>(&self, words: &[S]) -> heed::Result<Option<Vec<Vec<String>>>> {
            let words: Vec<_> = words.iter().map(|s| s.as_ref().to_owned()).collect();
            Ok(self.synonyms.get(&words).cloned())
        }
    }

    impl Default for TestContext {

        fn default() -> TestContext {
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

            TestContext {
                synonyms: hashmap!{
                    vec![String::from("hello")] => vec![
                        vec![String::from("hi")],
                        vec![String::from("good"), String::from("morning")],
                    ],
                    vec![String::from("world")] => vec![
                        vec![String::from("earth")],
                        vec![String::from("nature")],
                    ],
                    // new york city
                    vec![String::from("nyc")] => vec![
                        vec![String::from("new"), String::from("york")],
                        vec![String::from("new"), String::from("york"), String::from("city")],
                    ],
                    vec![String::from("new"), String::from("york")] => vec![
                        vec![String::from("nyc")],
                        vec![String::from("new"), String::from("york"), String::from("city")],
                    ],
                    vec![String::from("new"), String::from("york"), String::from("city")] => vec![
                        vec![String::from("nyc")],
                        vec![String::from("new"), String::from("york")],
                    ],
                },
                postings: hashmap!{
                    String::from("hello")      => random_postings(rng,   1500),
                    String::from("hi")         => random_postings(rng,   4000),
                    String::from("word")       => random_postings(rng,   2500),
                    String::from("split")      => random_postings(rng,    400),
                    String::from("ngrams")     => random_postings(rng,   1400),
                    String::from("world")      => random_postings(rng, 15_000),
                    String::from("earth")      => random_postings(rng,   8000),
                    String::from("2021")       => random_postings(rng,    100),
                    String::from("2020")       => random_postings(rng,    500),
                    String::from("is")         => random_postings(rng, 50_000),
                    String::from("this")       => random_postings(rng, 50_000),
                    String::from("good")       => random_postings(rng,   1250),
                    String::from("morning")    => random_postings(rng,    125),
                },
            }
        }
    }

    #[test]
    fn prefix() {
        let query = "hey friends";
        let stop_words = &Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(false, vec![
            Operation::And(vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("hey".to_string()) }),
                Operation::Query(Query { prefix: true, kind: QueryKind::tolerant(1, "friends".to_string()) }),
            ]),
            Operation::Query(Query { prefix: true, kind: QueryKind::tolerant(2, "heyfriends".to_string()) }),
        ]);

        let query_tree = TestContext::default().build(false, true, tokens).unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn no_prefix() {
        let query = "hey friends ";
        let stop_words = &Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(false, vec![
            Operation::And(vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("hey".to_string()) }),
                Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "friends".to_string()) }),
            ]),
            Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(2, "heyfriends".to_string()) }),
        ]);

        let query_tree = TestContext::default().build(false, true, tokens).unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn synonyms() {
        let query = "hello world ";
        let stop_words = &Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(false, vec![
            Operation::And(vec![
                Operation::Or(false, vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("hi".to_string()) }),
                    Operation::And(vec![
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("good".to_string()) }),
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("morning".to_string()) }),
                    ]),
                    Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "hello".to_string()) }),
                ]),
                Operation::Or(false, vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("earth".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("nature".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "world".to_string()) }),
                ]),
            ]),
            Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(2, "helloworld".to_string()) }),
        ]);

        let query_tree = TestContext::default().build(false, true, tokens).unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn complex_synonyms() {
        let query = "new york city ";
        let stop_words = &Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(false, vec![
            Operation::And(vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("new".to_string()) }),
                Operation::Or(false, vec![
                    Operation::And(vec![
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("york".to_string()) }),
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("city".to_string()) }),
                    ]),
                    Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "yorkcity".to_string()) }),
                ]),
            ]),
            Operation::And(vec![
                Operation::Or(false, vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("nyc".to_string()) }),
                    Operation::And(vec![
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("new".to_string()) }),
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("york".to_string()) }),
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("city".to_string()) }),
                    ]),
                    Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "newyork".to_string()) }),
                ]),
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("city".to_string()) }),
            ]),
            Operation::Or(false, vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("nyc".to_string()) }),
                Operation::And(vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("new".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("york".to_string()) }),
                ]),
                Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(2, "newyorkcity".to_string()) }),
            ]),
        ]);

        let query_tree = TestContext::default().build(false, true, tokens).unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn ngrams() {
        let query = "n grams ";
        let stop_words = &Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(false, vec![
            Operation::And(vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("n".to_string()) }),
                Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "grams".to_string()) }),
            ]),
            Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "ngrams".to_string()) }),
        ]);

        let query_tree = TestContext::default().build(false, true, tokens).unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn word_split() {
        let query = "wordsplit fish ";
        let stop_words = &Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(false, vec![
            Operation::And(vec![
                Operation::Or(false, vec![
                    Operation::Consecutive(vec![
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("word".to_string()) }),
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact("split".to_string()) }),
                    ]),
                    Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(2, "wordsplit".to_string()) }),
                ]),
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("fish".to_string()) })
            ]),
            Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(2, "wordsplitfish".to_string()) }),
        ]);

        let query_tree = TestContext::default().build(false, true, tokens).unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn phrase() {
        let query = "\"hey friends\" \" \" \"wooop";
        let stop_words = &Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::And(vec![
            Operation::Consecutive(vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("hey".to_string()) }),
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("friends".to_string()) }),
            ]),
            Operation::Query(Query { prefix: false, kind: QueryKind::exact("wooop".to_string()) }),
        ]);

        let query_tree = TestContext::default().build(false, true, tokens).unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn optional_word() {
        let query = "hey my friend ";
        let stop_words = &Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(true, vec![
            Operation::Or(false, vec![
                Operation::And(vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("hey".to_string()) }),
                    Operation::Or(false, vec![
                        Operation::And(vec![
                            Operation::Query(Query { prefix: false, kind: QueryKind::exact("my".to_string()) }),
                            Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "friend".to_string()) }),
                        ]),
                        Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "myfriend".to_string()) })
                    ])
                ]),
                Operation::And(vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "heymy".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "friend".to_string()) }),
                ]),
                Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(2, "heymyfriend".to_string()) }),
            ]),
            Operation::Or(false, vec![
                Operation::And(vec![
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("hey".to_string()) }),
                    Operation::Query(Query { prefix: false, kind: QueryKind::exact("my".to_string()) }),
                ]),
                Operation::Query(Query { prefix: false, kind: QueryKind::tolerant(1, "heymy".to_string()) }),
            ]),
            Operation::Query(Query { prefix: false, kind: QueryKind::exact("hey".to_string()) }),
        ]);
        let query_tree = TestContext::default().build(true, true, tokens).unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn no_typo() {
        let query = "hey friends ";
        let stop_words = &Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(false, vec![
            Operation::And(vec![
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("hey".to_string()) }),
                Operation::Query(Query { prefix: false, kind: QueryKind::exact("friends".to_string()) }),
            ]),
            Operation::Query(Query { prefix: false, kind: QueryKind::exact("heyfriends".to_string()) }),
        ]);
        let query_tree = TestContext::default().build(false, false, tokens).unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn fetching_words() {
        let query = "wordsplit nyc world";
        let stop_words = &Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let context = TestContext::default();
        let query_tree = context.build(false, true, tokens).unwrap();

        let expected = vec![
            ("city".to_string(), false),
            ("earth".to_string(), false),
            ("nature".to_string(), false),
            ("new".to_string(), false),
            ("nyc".to_string(), false),
            ("split".to_string(), false),
            ("word".to_string(), false),
            ("word".to_string(), true),
            ("world".to_string(), true),
            ("york".to_string(), false),

        ];

        let mut keys = context.postings.keys().collect::<Vec<_>>();
        keys.sort_unstable();
        let set = fst::Set::from_iter(keys).unwrap().map_data(|v| Cow::Owned(v)).unwrap();

        let words = fetch_words(&query_tree, &set);

        assert_eq!(expected, words);
    }
}
