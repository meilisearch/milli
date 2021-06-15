use std::{cmp, fmt, mem};

use fst::Set;
use meilisearch_tokenizer::token::SeparatorKind;
use meilisearch_tokenizer::tokenizer::TokenStream;
use meilisearch_tokenizer::TokenKind;
use roaring::RoaringBitmap;
use slice_group_by::GroupBy;

use crate::Index;

type IsOptionalWord = bool;
type IsPrefix = bool;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Operation {
    And(Vec<Operation>),
    // serie of consecutive non prefix and exact words
    Phrase(Vec<String>),
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
                }
                Operation::Phrase(children) => {
                    writeln!(f, "{:2$}PHRASE {:?}", "", children, depth * 2)
                }
                Operation::Or(true, children) => {
                    writeln!(f, "{:1$}OR(WORD)", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                }
                Operation::Or(false, children) => {
                    writeln!(f, "{:1$}OR", "", depth * 2)?;
                    children.iter().try_for_each(|c| pprint_tree(f, c, depth + 1))
                }
                Operation::Query(query) => writeln!(f, "{:2$}{:?}", "", query, depth * 2),
            }
        }

        pprint_tree(f, self, 0)
    }
}

impl Operation {
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

    fn phrase(mut words: Vec<String>) -> Self {
        if words.len() == 1 {
            Self::Query(Query { prefix: false, kind: QueryKind::exact(words.pop().unwrap()) })
        } else {
            Self::Phrase(words)
        }
    }

    pub fn query(&self) -> Option<&Query> {
        match self {
            Operation::Query(query) => Some(query),
            _ => None,
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
    pub fn exact(word: String) -> Self {
        QueryKind::Exact { original_typo: 0, word }
    }

    pub fn exact_with_typo(original_typo: u8, word: String) -> Self {
        QueryKind::Exact { original_typo, word }
    }

    pub fn tolerant(typo: u8, word: String) -> Self {
        QueryKind::Tolerant { typo, word }
    }

    pub fn is_tolerant(&self) -> bool {
        matches!(self, QueryKind::Tolerant { .. })
    }

    pub fn is_exact(&self) -> bool {
        matches!(self, QueryKind::Exact { .. })
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
            }
            QueryKind::Tolerant { typo, word } => f
                .debug_struct(&(prefix + "Tolerant"))
                .field("word", &word)
                .field("max typo", &typo)
                .finish(),
        }
    }
}

trait Context {
    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;
    fn synonyms<S: AsRef<str>>(&self, words: &[S]) -> heed::Result<Option<Vec<Vec<String>>>>;
    fn word_documents_count(&self, word: &str) -> heed::Result<Option<u64>> {
        match self.word_docids(word)? {
            Some(rb) => Ok(Some(rb.len())),
            None => Ok(None),
        }
    }
}

/// The query tree builder is the interface to build a query tree.
pub struct QueryTreeBuilder<'a> {
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
    optional_words: bool,
    authorize_typos: bool,
    words_limit: Option<usize>,
}

impl<'a> Context for QueryTreeBuilder<'a> {
    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_docids.get(self.rtxn, word)
    }

    fn synonyms<S: AsRef<str>>(&self, words: &[S]) -> heed::Result<Option<Vec<Vec<String>>>> {
        self.index.words_synonyms(self.rtxn, words)
    }

    fn word_documents_count(&self, word: &str) -> heed::Result<Option<u64>> {
        self.index.word_documents_count(self.rtxn, word)
    }
}

impl<'a> QueryTreeBuilder<'a> {
    /// Create a `QueryTreeBuilder` from a heed ReadOnly transaction `rtxn`
    /// and an Index `index`.
    pub fn new(rtxn: &'a heed::RoTxn<'a>, index: &'a Index) -> Self {
        Self { rtxn, index, optional_words: true, authorize_typos: true, words_limit: None }
    }

    /// if `optional_words` is set to `false` the query tree will be
    /// generated forcing all query words to be present in each matching documents
    /// (the criterion `words` will be ignored).
    /// default value if not called: `true`
    #[allow(unused)]
    pub fn optional_words(&mut self, optional_words: bool) -> &mut Self {
        self.optional_words = optional_words;
        self
    }

    /// if `authorize_typos` is set to `false` the query tree will be generated
    /// forcing all query words to match documents without any typo
    /// (the criterion `typo` will be ignored).
    /// default value if not called: `true`
    #[allow(unused)]
    pub fn authorize_typos(&mut self, authorize_typos: bool) -> &mut Self {
        self.authorize_typos = authorize_typos;
        self
    }

    /// Limit words and phrases that will be taken for query building.
    /// Any beyond `words_limit` will be ignored.
    pub fn words_limit(&mut self, words_limit: usize) -> &mut Self {
        self.words_limit = Some(words_limit);
        self
    }

    /// Build the query tree:
    /// - if `optional_words` is set to `false` the query tree will be
    ///   generated forcing all query words to be present in each matching documents
    ///   (the criterion `words` will be ignored)
    /// - if `authorize_typos` is set to `false` the query tree will be generated
    ///   forcing all query words to match documents without any typo
    ///   (the criterion `typo` will be ignored)
    pub fn build(&self, query: TokenStream) -> anyhow::Result<Option<(Operation, PrimitiveQuery)>> {
        let stop_words = self.index.stop_words(self.rtxn)?;
        let primitive_query = create_primitive_query(query, stop_words, self.words_limit);
        if !primitive_query.is_empty() {
            let qt = create_query_tree(
                self,
                self.optional_words,
                self.authorize_typos,
                &primitive_query,
            )?;
            Ok(Some((qt, primitive_query)))
        } else {
            Ok(None)
        }
    }
}

/// Split the word depending on the frequency of subwords in the database documents.
fn split_best_frequency(ctx: &impl Context, word: &str) -> heed::Result<Option<Operation>> {
    let chars = word.char_indices().skip(1);
    let mut best = None;

    for (i, _) in chars {
        let (left, right) = word.split_at(i);

        let left_freq = ctx.word_documents_count(left)?.unwrap_or(0);
        let right_freq = ctx.word_documents_count(right)?.unwrap_or(0);

        let min_freq = cmp::min(left_freq, right_freq);
        if min_freq != 0 && best.map_or(true, |(old, _, _)| min_freq > old) {
            best = Some((min_freq, left, right));
        }
    }

    Ok(best.map(|(_, left, right)| Operation::Phrase(vec![left.to_string(), right.to_string()])))
}

/// Return the `QueryKind` of a word depending on `authorize_typos`
/// and the provided word length.
fn typos(word: String, authorize_typos: bool) -> QueryKind {
    if authorize_typos {
        match word.len() {
            0..=4 => QueryKind::exact(word),
            5..=8 => QueryKind::tolerant(1, word),
            _ => QueryKind::tolerant(2, word),
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
        synonyms
            .into_iter()
            .map(|synonym| {
                let words = synonym
                    .into_iter()
                    .map(|word| {
                        Operation::Query(Query { prefix: false, kind: QueryKind::exact(word) })
                    })
                    .collect();
                Operation::and(words)
            })
            .collect()
    }))
}

/// Main function that creates the final query tree from the primitive query.
fn create_query_tree(
    ctx: &impl Context,
    optional_words: bool,
    authorize_typos: bool,
    query: &[PrimitiveQueryPart],
) -> anyhow::Result<Operation> {
    /// Matches on the `PrimitiveQueryPart` and create an operation from it.
    fn resolve_primitive_part(
        ctx: &impl Context,
        authorize_typos: bool,
        part: PrimitiveQueryPart,
    ) -> anyhow::Result<Operation> {
        match part {
            // 1. try to split word in 2
            // 2. try to fetch synonyms
            // 3. create an operation containing the word
            // 4. wrap all in an OR operation
            PrimitiveQueryPart::Word(word, prefix) => {
                let mut children = synonyms(ctx, &[&word])?.unwrap_or_default();
                if let Some(child) = split_best_frequency(ctx, &word)? {
                    children.push(child);
                }
                children
                    .push(Operation::Query(Query { prefix, kind: typos(word, authorize_typos) }));
                Ok(Operation::or(false, children))
            }
            // create a CONSECUTIVE operation wrapping all word in the phrase
            PrimitiveQueryPart::Phrase(words) => Ok(Operation::phrase(words)),
        }
    }

    /// Create all ngrams 1..=3 generating query tree branches.
    fn ngrams(
        ctx: &impl Context,
        authorize_typos: bool,
        query: &[PrimitiveQueryPart],
    ) -> anyhow::Result<Operation> {
        const MAX_NGRAM: usize = 3;
        let mut op_children = Vec::new();

        for sub_query in query.linear_group_by(|a, b| !(a.is_phrase() || b.is_phrase())) {
            let mut or_op_children = Vec::new();

            for ngram in 1..=MAX_NGRAM.min(sub_query.len()) {
                if let Some(group) = sub_query.get(..ngram) {
                    let mut and_op_children = Vec::new();
                    let tail = &sub_query[ngram..];
                    let is_last = tail.is_empty();

                    match group {
                        [part] => {
                            let operation =
                                resolve_primitive_part(ctx, authorize_typos, part.clone())?;
                            and_op_children.push(operation);
                        }
                        words => {
                            let is_prefix = words.last().map_or(false, |part| part.is_prefix());
                            let words: Vec<_> = words
                                .iter()
                                .filter_map(|part| {
                                    if let PrimitiveQueryPart::Word(word, _) = part {
                                        Some(word.as_str())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            let mut operations = synonyms(ctx, &words)?.unwrap_or_default();
                            let concat = words.concat();
                            let query =
                                Query { prefix: is_prefix, kind: typos(concat, authorize_typos) };
                            operations.push(Operation::Query(query));
                            and_op_children.push(Operation::or(false, operations));
                        }
                    }

                    if !is_last {
                        let ngrams = ngrams(ctx, authorize_typos, tail)?;
                        and_op_children.push(ngrams);
                    }
                    or_op_children.push(Operation::and(and_op_children));
                }
            }
            op_children.push(Operation::or(false, or_op_children));
        }

        Ok(Operation::and(op_children))
    }

    /// Create a new branch removing the last non-phrase query parts.
    fn optional_word(
        ctx: &impl Context,
        authorize_typos: bool,
        query: PrimitiveQuery,
    ) -> anyhow::Result<Operation> {
        let number_phrases = query.iter().filter(|p| p.is_phrase()).count();
        let mut operation_children = Vec::new();

        let start = number_phrases + (number_phrases == 0) as usize;
        for len in start..=query.len() {
            let mut word_count = len - number_phrases;
            let query: Vec<_> = query
                .iter()
                .filter(|p| {
                    if p.is_phrase() {
                        true
                    } else if word_count != 0 {
                        word_count -= 1;
                        true
                    } else {
                        false
                    }
                })
                .cloned()
                .collect();

            let ngrams = ngrams(ctx, authorize_typos, &query)?;
            operation_children.push(ngrams);
        }

        Ok(Operation::or(true, operation_children))
    }

    if optional_words {
        optional_word(ctx, authorize_typos, query.to_vec())
    } else {
        ngrams(ctx, authorize_typos, query)
    }
}

pub type PrimitiveQuery = Vec<PrimitiveQueryPart>;

#[derive(Debug, Clone)]
pub enum PrimitiveQueryPart {
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
fn create_primitive_query(
    query: TokenStream,
    stop_words: Option<Set<&[u8]>>,
    words_limit: Option<usize>,
) -> PrimitiveQuery {
    let mut primitive_query = Vec::new();
    let mut phrase = Vec::new();
    let mut quoted = false;

    let parts_limit = words_limit.unwrap_or(usize::MAX);

    let mut peekable = query.peekable();
    while let Some(token) = peekable.next() {
        // early return if word limit is exceeded
        if primitive_query.len() >= parts_limit {
            return primitive_query;
        }

        match token.kind {
            TokenKind::Word | TokenKind::StopWord => {
                // 1. if the word is quoted we push it in a phrase-buffer waiting for the ending quote,
                // 2. if the word is not the last token of the query and is not a stop_word we push it as a non-prefix word,
                // 3. if the word is the last token of the query we push it as a prefix word.
                if quoted {
                    phrase.push(token.word.to_string());
                } else if peekable.peek().is_some() {
                    if !stop_words
                        .as_ref()
                        .map_or(false, |swords| swords.contains(token.word.as_ref()))
                    {
                        primitive_query
                            .push(PrimitiveQueryPart::Word(token.word.to_string(), false));
                    }
                } else {
                    primitive_query.push(PrimitiveQueryPart::Word(token.word.to_string(), true));
                }
            }
            TokenKind::Separator(separator_kind) => {
                let quote_count = token.word.chars().filter(|&s| s == '"').count();
                // swap quoted state if we encounter a double quote
                if quote_count % 2 != 0 {
                    quoted = !quoted;
                }
                // if there is a quote or a hard separator we close the phrase.
                if !phrase.is_empty() && (quote_count > 0 || separator_kind == SeparatorKind::Hard)
                {
                    primitive_query.push(PrimitiveQueryPart::Phrase(mem::take(&mut phrase)));
                }
            }
            _ => (),
        }
    }

    // If a quote is never closed, we consider all of the end of the query as a phrase.
    if !phrase.is_empty() {
        primitive_query.push(PrimitiveQueryPart::Phrase(mem::take(&mut phrase)));
    }

    primitive_query
}

/// Returns the maximum number of typos that this Operation allows.
pub fn maximum_typo(operation: &Operation) -> usize {
    use Operation::{And, Or, Phrase, Query};
    match operation {
        Or(_, ops) => ops.iter().map(maximum_typo).max().unwrap_or(0),
        And(ops) => ops.iter().map(maximum_typo).sum::<usize>(),
        Query(q) => q.kind.typo() as usize,
        // no typo allowed in phrases
        Phrase(_) => 0,
    }
}

/// Returns the maximum proximity that this Operation allows.
pub fn maximum_proximity(operation: &Operation) -> usize {
    use Operation::{And, Or, Phrase, Query};
    match operation {
        Or(_, ops) => ops.iter().map(maximum_proximity).max().unwrap_or(0),
        And(ops) => {
            ops.iter().map(maximum_proximity).sum::<usize>() + ops.len().saturating_sub(1) * 7
        }
        Query(_) | Phrase(_) => 0,
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use maplit::hashmap;
    use meilisearch_tokenizer::{Analyzer, AnalyzerConfig};
    use rand::{rngs::StdRng, Rng, SeedableRng};

    use super::*;

    #[derive(Debug)]
    struct TestContext {
        synonyms: HashMap<Vec<String>, Vec<Vec<String>>>,
        postings: HashMap<String, RoaringBitmap>,
    }

    impl TestContext {
        fn build(
            &self,
            optional_words: bool,
            authorize_typos: bool,
            words_limit: Option<usize>,
            query: TokenStream,
        ) -> anyhow::Result<Option<(Operation, PrimitiveQuery)>> {
            let primitive_query = create_primitive_query(query, None, words_limit);
            if !primitive_query.is_empty() {
                let qt =
                    create_query_tree(self, optional_words, authorize_typos, &primitive_query)?;
                Ok(Some((qt, primitive_query)))
            } else {
                Ok(None)
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
                synonyms: hashmap! {
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
                postings: hashmap! {
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
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: true,
                        kind: QueryKind::tolerant(1, "friends".to_string()),
                    }),
                ]),
                Operation::Query(Query {
                    prefix: true,
                    kind: QueryKind::tolerant(2, "heyfriends".to_string()),
                }),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn no_prefix() {
        let query = "hey friends ";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::tolerant(1, "friends".to_string()),
                    }),
                ]),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::tolerant(2, "heyfriends".to_string()),
                }),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn synonyms() {
        let query = "hello world ";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Or(
                        false,
                        vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("hi".to_string()),
                            }),
                            Operation::And(vec![
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("good".to_string()),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("morning".to_string()),
                                }),
                            ]),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "hello".to_string()),
                            }),
                        ],
                    ),
                    Operation::Or(
                        false,
                        vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("earth".to_string()),
                            }),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("nature".to_string()),
                            }),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "world".to_string()),
                            }),
                        ],
                    ),
                ]),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::tolerant(2, "helloworld".to_string()),
                }),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn complex_synonyms() {
        let query = "new york city ";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("new".to_string()),
                    }),
                    Operation::Or(
                        false,
                        vec![
                            Operation::And(vec![
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("york".to_string()),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("city".to_string()),
                                }),
                            ]),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "yorkcity".to_string()),
                            }),
                        ],
                    ),
                ]),
                Operation::And(vec![
                    Operation::Or(
                        false,
                        vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("nyc".to_string()),
                            }),
                            Operation::And(vec![
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("new".to_string()),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("york".to_string()),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("city".to_string()),
                                }),
                            ]),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "newyork".to_string()),
                            }),
                        ],
                    ),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("city".to_string()),
                    }),
                ]),
                Operation::Or(
                    false,
                    vec![
                        Operation::Query(Query {
                            prefix: false,
                            kind: QueryKind::exact("nyc".to_string()),
                        }),
                        Operation::And(vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("new".to_string()),
                            }),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("york".to_string()),
                            }),
                        ]),
                        Operation::Query(Query {
                            prefix: false,
                            kind: QueryKind::tolerant(2, "newyorkcity".to_string()),
                        }),
                    ],
                ),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn ngrams() {
        let query = "n grams ";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("n".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::tolerant(1, "grams".to_string()),
                    }),
                ]),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::tolerant(1, "ngrams".to_string()),
                }),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn word_split() {
        let query = "wordsplit fish ";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Or(
                        false,
                        vec![
                            Operation::Phrase(vec!["word".to_string(), "split".to_string()]),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(2, "wordsplit".to_string()),
                            }),
                        ],
                    ),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("fish".to_string()),
                    }),
                ]),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::tolerant(2, "wordsplitfish".to_string()),
                }),
            ],
        );

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn phrase() {
        let query = "\"hey friends\" \" \" \"wooop";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::And(vec![
            Operation::Phrase(vec!["hey".to_string(), "friends".to_string()]),
            Operation::Query(Query { prefix: false, kind: QueryKind::exact("wooop".to_string()) }),
        ]);

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn phrase_with_hard_separator() {
        let query = "\"hey friends. wooop wooop\"";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::And(vec![
            Operation::Phrase(vec!["hey".to_string(), "friends".to_string()]),
            Operation::Phrase(vec!["wooop".to_string(), "wooop".to_string()]),
        ]);

        let (query_tree, _) =
            TestContext::default().build(false, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn optional_word() {
        let query = "hey my friend ";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(
            true,
            vec![
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::exact("hey".to_string()),
                }),
                Operation::Or(
                    false,
                    vec![
                        Operation::And(vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("hey".to_string()),
                            }),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("my".to_string()),
                            }),
                        ]),
                        Operation::Query(Query {
                            prefix: false,
                            kind: QueryKind::tolerant(1, "heymy".to_string()),
                        }),
                    ],
                ),
                Operation::Or(
                    false,
                    vec![
                        Operation::And(vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::exact("hey".to_string()),
                            }),
                            Operation::Or(
                                false,
                                vec![
                                    Operation::And(vec![
                                        Operation::Query(Query {
                                            prefix: false,
                                            kind: QueryKind::exact("my".to_string()),
                                        }),
                                        Operation::Query(Query {
                                            prefix: false,
                                            kind: QueryKind::tolerant(1, "friend".to_string()),
                                        }),
                                    ]),
                                    Operation::Query(Query {
                                        prefix: false,
                                        kind: QueryKind::tolerant(1, "myfriend".to_string()),
                                    }),
                                ],
                            ),
                        ]),
                        Operation::And(vec![
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "heymy".to_string()),
                            }),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "friend".to_string()),
                            }),
                        ]),
                        Operation::Query(Query {
                            prefix: false,
                            kind: QueryKind::tolerant(2, "heymyfriend".to_string()),
                        }),
                    ],
                ),
            ],
        );
        let (query_tree, _) =
            TestContext::default().build(true, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn optional_word_phrase() {
        let query = "\"hey my\"";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Phrase(vec!["hey".to_string(), "my".to_string()]);
        let (query_tree, _) =
            TestContext::default().build(true, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn optional_word_multiple_phrases() {
        let query = r#""hey" my good "friend""#;
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(
            true,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("friend".to_string()),
                    }),
                ]),
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("my".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("friend".to_string()),
                    }),
                ]),
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Or(
                        false,
                        vec![
                            Operation::And(vec![
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("my".to_string()),
                                }),
                                Operation::Query(Query {
                                    prefix: false,
                                    kind: QueryKind::exact("good".to_string()),
                                }),
                            ]),
                            Operation::Query(Query {
                                prefix: false,
                                kind: QueryKind::tolerant(1, "mygood".to_string()),
                            }),
                        ],
                    ),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("friend".to_string()),
                    }),
                ]),
            ],
        );
        let (query_tree, _) =
            TestContext::default().build(true, true, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn no_typo() {
        let query = "hey friends ";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::Or(
            false,
            vec![
                Operation::And(vec![
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("hey".to_string()),
                    }),
                    Operation::Query(Query {
                        prefix: false,
                        kind: QueryKind::exact("friends".to_string()),
                    }),
                ]),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::exact("heyfriends".to_string()),
                }),
            ],
        );
        let (query_tree, _) =
            TestContext::default().build(false, false, None, tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }

    #[test]
    fn words_limit() {
        let query = "\"hey my\" good friend";
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());
        let result = analyzer.analyze(query);
        let tokens = result.tokens();

        let expected = Operation::And(vec![
            Operation::Phrase(vec!["hey".to_string(), "my".to_string()]),
            Operation::Query(Query { prefix: false, kind: QueryKind::exact("good".to_string()) }),
        ]);

        let (query_tree, _) =
            TestContext::default().build(false, false, Some(2), tokens).unwrap().unwrap();

        assert_eq!(expected, query_tree);
    }
}
