mod datasets_paths;
mod utils;

use criterion::{criterion_group, criterion_main};
use milli::update::Settings;
use milli::CriterionImplementationStrategy;
use utils::{IndexConf, IndexSettingsConf, SearchBenchConf};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn base_conf(builder: &mut Settings) {
    let displayed_fields = ["title", "body", "url"].iter().map(|s| s.to_string()).collect();
    builder.set_displayed_fields(displayed_fields);

    let searchable_fields = ["title", "body"].iter().map(|s| s.to_string()).collect();
    builder.set_searchable_fields(searchable_fields);
}

fn bench_wiki(c: &mut criterion::Criterion) {
    let index_conf: IndexConf = IndexConf {
        dataset: datasets_paths::SMOL_WIKI_ARTICLES,
        configure: base_conf,
        ..IndexConf::BASE
    };

    #[rustfmt::skip]
    let benches = vec![
        // First all the benches done on the index with only the proximity criterion
        (
            IndexSettingsConf {
                criterion: Some(&["proximity"]),
            }, 
            vec![      
                SearchBenchConf {
                    group_name: "proximity criterion",
                    queries: vec![
                        "herald sings ",
                        "april paris ",
                        "tea two ",
                        "diesel engine ",
                    ],
                    optional_words: false,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "proximity criterion set-based",
                    queries: vec![
                        "herald sings ",
                        "april paris ",
                        "tea two ",
                        "diesel engine ",
                    ],
                    optional_words: false,
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased, 
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "proximity criterion iterative",
                    queries: vec![
                        "herald sings ",
                        "april paris ",
                        "tea two ",
                        "diesel engine ",
                    ],
                    optional_words: false,
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative, 
                    ..SearchBenchConf::BASE
                },
            ]
        ),
        // Then all the benches done on the index with only the typo criterion
        (
            IndexSettingsConf {
                criterion: Some(&["typo"]),
            }, 
            vec![   
                SearchBenchConf {
                    group_name: "typo criterion",
                    queries: vec![
                        "migrosoft ",
                        "linax ",
                        "Disnaylande ",
                        "phytogropher ",
                        "nympalidea ",
                        "aritmetric ",
                        "the fronce ",
                        "sisan ",
                    ],
                    optional_words: false,
                    ..SearchBenchConf::BASE
                },
            ]
        ),
        // Then all the benches done on the index with only the words criterion
        (
            IndexSettingsConf {
                criterion: Some(&["words"]),
            }, 
            vec![   
                SearchBenchConf {
                    group_name: "words criterion",
                    queries: vec![
                        "the black saint and the sinner lady and the good doggo ", // four words to pop, 27 results
                        "Kameya Tokujirō mingus monk ",                            // two words to pop, 55
                        "Ulrich Hensel meilisearch milli ",                        // two words to pop, 306
                        "Idaho Bellevue pizza ",                                   // one word to pop, 800
                        "Abraham machin ",                                         // one word to pop, 1141
                    ],
                    ..SearchBenchConf::BASE
                }
            ]
        ),

        // /* the we bench some global / normal search with all the default criterion in the default
        //  * order */
        (
            IndexSettingsConf::BASE,
            vec![
                SearchBenchConf {
                    group_name: "basic placeholder",
                    queries: vec![""],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "basic without quote",
                    queries: vec![
                        "mingus ",
                        "miles davis ",
                        "rock and roll ",
                        "machine ",
                        "spain ",
                        "japan ",
                        "france ",
                        "film ",
                        "the black saint and the sinner lady and the",
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "basic without quote set-based",
                    queries: vec![
                        "mingus",
                        "miles davis",
                        "rock and roll",
                        "machine",
                        "spain",
                        "japan",
                        "france",
                        "film",
                        "the black saint and the sinner lady and the",
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "basic with quote",
                    queries: vec![
                        "\"mingus\"",
                        "\"miles davis\"",
                        "\"rock and roll\"",
                        "\"machine\"",
                        "\"spain\"",
                        "\"japan\"",
                        "\"france\"",
                        "\"film\"",
                        "\"the black saint and the sinner lady\" and the",
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "prefix search",
                    queries: vec![
                        "t",
                        "c",
                        "g",
                        "j",
                        "q",
                        "x",
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "prefix search set-based",
                    queries: vec![
                        "t",
                        "c",
                        "g",
                        "j",
                        "q",
                        "x",
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "prefix search iterative",
                    queries: vec![
                        "t",
                        "c",
                        "g",
                        "j",
                        "q",
                        "x",
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "words + prefix search",
                    queries: vec![
                        "the love of a new f",
                        "aesthetic sense of w",
                        "aesthetic sense of wo",
                        "once upon a time in ho",
                        "once upon a time in hol",
                        "once upon a time in hollywood a",
                        "belgium ardennes festival l",
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "words + prefix search set-based",
                    queries: vec![
                        "the love of a new f",
                        "aesthetic sense of w",
                        "aesthetic sense of wo",
                        "once upon a time in ho",
                        "once upon a time in hol",
                        "once upon a time in hollywood a",
                        "belgium ardennes festival l",
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "words + prefix search iterative",
                    queries: vec![
                        "the love of a new f",
                        "aesthetic sense of w",
                        "aesthetic sense of wo",
                        "once upon a time in ho",
                        "once upon a time in hol",
                        "once upon a time in hollywood a",
                        "belgium ardennes festival l",
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "10x 'a' or 'b'",
                    queries: vec![
                        "a a a a a a a a a a",
                        "b b b b b b b b b b",
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "10x 'a' or 'b' - set-based",
                    queries: vec![
                        "a a a a a a a a a a",
                        "b b b b b b b b b b",
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "10x 'a' or 'b' - iterative",
                    queries: vec![
                        "a a a a a a a a a a",
                        "b b b b b b b b b b",
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
            ] 
        )
    ];

    utils::run_benches(index_conf, c, &benches);
}

criterion_group!(
    name = benches;
    config = { criterion::Criterion::default().sample_size(10) };
    targets = bench_wiki
);
criterion_main!(benches);
