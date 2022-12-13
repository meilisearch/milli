mod datasets_paths;
mod utils;

use std::collections::HashMap;

use criterion::{criterion_group, criterion_main};
use milli::update::Settings;
use milli::CriterionImplementationStrategy;
use utils::{IndexConf, IndexSettingsConf, SearchBenchConf};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn base_conf(builder: &mut Settings) {
    let displayed_fields =
        ["id", "title", "album", "artist", "genre", "country", "released", "duration"]
            .iter()
            .map(|s| s.to_string())
            .collect();
    builder.set_displayed_fields(displayed_fields);

    let searchable_fields =
        ["title", "album", "artist", "genre"].iter().map(|s| s.to_string()).collect();
    builder.set_searchable_fields(searchable_fields);

    let faceted_fields = ["released-timestamp", "duration-float", "genre", "country", "artist"]
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut synonyms = HashMap::new();
    synonyms.insert("ily".to_owned(), vec!["i love you".to_owned()]);
    synonyms.insert("rnr".to_owned(), vec!["rock and roll".to_owned()]);
    synonyms.insert(
        "mftomp".to_owned(),
        vec![
            "songs from the original motion picture".to_owned(),
            "music from the original motion picture".to_owned(),
            "music from the motion picture".to_owned(),
            "songs from the motion picture".to_owned(),
            "songs from the original soundtrack".to_owned(),
            "original soundtrack".to_owned(),
        ],
    );
    builder.set_synonyms(synonyms);

    builder.set_filterable_fields(faceted_fields);
}

fn bench_songs(c: &mut criterion::Criterion) {
    let base_index_conf = IndexConf {
        dataset: datasets_paths::SMOL_SONGS,
        primary_key: Some("id"),
        configure: base_conf,
        ..IndexConf::BASE
    };

    let default_criterion: Vec<String> =
        milli::default_criteria().iter().map(|criteria| criteria.to_string()).collect();
    let default_criterion = default_criterion.iter().map(|s| s.as_str());
    let asc_default: Vec<&str> =
        std::iter::once("released-timestamp:asc").chain(default_criterion.clone()).collect();
    let desc_default: Vec<&str> =
        std::iter::once("released-timestamp:desc").chain(default_criterion.clone()).collect();

    #[rustfmt::skip]
    let benches = &[
        // First all the benches done on the index with only the proximity criterion
        (
            IndexSettingsConf {
                criterion: Some(&["proximity"]),
            }, 
            vec![
                SearchBenchConf {
                    group_name: "proximity criterion",
                    queries: vec![
                        "black saint sinner lady ",
                        "les dangeureuses 1960 ",
                        "The Disneyland Sing-Along Chorus ",
                        "Under Great Northern Lights ",
                        "7000 Danses Un Jour Dans Notre Vie ",
                    ],
                    optional_words: false,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "proximity criterion set-based",
                    queries: vec![
                        "black saint sinner lady ",
                        "les dangeureuses 1960 ",
                        "The Disneyland Sing-Along Chorus ",
                        "Under Great Northern Lights ",
                        "7000 Danses Un Jour Dans Notre Vie ",
                    ],
                    optional_words: false,
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "proximity criterion iterative",
                    queries: vec![
                        "black saint sinner lady ",
                        "les dangeureuses 1960 ",
                        "The Disneyland Sing-Along Chorus ",
                        "Under Great Northern Lights ",
                        "7000 Danses Un Jour Dans Notre Vie ",
                    ],
                    optional_words: false,
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
            ]
        ),
        // Then with only the typo criterion
        (
            IndexSettingsConf {
                criterion: Some(&["typo"]),
            }, 
            vec![
                SearchBenchConf {
                    group_name: "typo criterion",
                    queries: vec![
                        "mongus ",
                        "thelonius monk ",
                        "Disnaylande ",
                        "the white striper ",
                        "indochie ",
                        "indochien ",
                        "klub des loopers ",
                        "fear of the duck ",
                        "michel depech ",
                        "stromal ",
                        "dire straights ",
                        "Arethla Franklin ",
                    ],
                    optional_words: false,
                    ..SearchBenchConf::BASE
                },
            ]
        ),
        // Then with only the words criterion
        (
            IndexSettingsConf {
                criterion: Some(&["words"]),
            }, 
            vec![
                SearchBenchConf {
                    group_name: "words criterion",
                    queries: vec![
                        "the black saint and the sinner lady and the good doggo ", // four words to pop
                        "les liaisons dangeureuses 1793 ",                         // one word to pop
                        "The Disneyland Children's Sing-Alone song ",              // two words to pop
                        "seven nation mummy ",                                     // one word to pop
                        "7000 Danses / Le Baiser / je me trompe de mots ",         // four words to pop
                        "Bring Your Daughter To The Slaughter but now this is not part of the title ", // nine words to pop
                        "whathavenotnsuchforth and a good amount of words to pop to match the first one ", // 13
                    ],
                    ..SearchBenchConf::BASE
                }
            ]
        ),
        // Then with only the released-timestamp:asc criterion
        (
            IndexSettingsConf {
                criterion: Some(&["released-timestamp:asc"]),
            }, 
            vec![
                SearchBenchConf {
                    group_name: "asc",
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "asc set-based",
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "asc iterative",
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
            ]
        ),
        // Then with only the released-timestamp:desc criterion
        (
            IndexSettingsConf {
                criterion: Some(&["released-timestamp:desc"]),
            }, 
            vec![
                SearchBenchConf {
                    group_name: "desc",
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "desc set-based",
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "desc iterative",
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
            ]
        ),
        // Then with the asc criterion on top of the default criterion 
        (
            IndexSettingsConf {
                criterion: Some(&asc_default[..]),
            }, 
            vec![
                SearchBenchConf {
                    group_name: "asc + default",
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "asc + default set-based",
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "asc + default iterative",
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
            ]
        ),
        // Then with the desc criterion on top of the default criterion 
        (
            IndexSettingsConf {
                criterion: Some(&desc_default[..]),
            }, 
            vec![
                SearchBenchConf {
                    group_name: "desc + default",
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "desc + default set-based",
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "desc + default iterative",
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
            ]
        ),
        // Then with the default index config
        (
            IndexSettingsConf::BASE, 
            vec![
                SearchBenchConf {
                    group_name: "basic filter: <=",
                    filter: Some("released-timestamp <= 946728000"), // year 2000
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "basic filter: TO",
                    filter: Some("released-timestamp 946728000 TO 1262347200"), // year 2000 to 2010
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "big filter",
                    filter: Some("released-timestamp != 1262347200 AND (NOT (released-timestamp = 946728000)) AND (duration-float = 1 OR (duration-float 1.1 TO 1.5 AND released-timestamp > 315576000))"),
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "big IN filter",
                    filter: Some("NOT \"released-timestamp\" IN [-126230400, -1399075200, -160444800, -189388800, -220924800, -252460800, -283996800, -31536000, -347155200, -378691200, -473385600, -631152000, -694310400, -94694400, 0, 1000166400, 1009843200, 1041379200, 1070323200, 1072915200, 1075852800, 1078099200, 1088640000, 1096588800, 1099612800, 1104537600, 1121644800, 1136073600, 1150156800, 1159660800, 1162339200, 1167609600, 1171584000, 1183248000, 1184112000, 1190419200, 1199145600, 1203724800, 1204329600, 1216339200, 1228089600, 1230768000, 1233446400, 1247097600, 1247961600, 1252886400, 126230400, 1262304000, 1268956800, 1283212800, 1285027200, 1293840000, 1295913600, 1296518400, 1306886400, 1312156800, 1320105600, 1321228800, 1321660800, 1322179200, 1322438400, 1325376000, 1327536000, 1338336000, 1347840000, 1351728000, 1353801600, 1356048000, 1356998400, 1369440000, 1370044800, 1372636800, 1382572800, 1382659200, 1384905600, 1388534400, 1393804800, 1397260800, 1401148800, 1411948800, 1420070400, 1426377600, 1427846400, 1433116800, 1439078400, 1440028800, 1446336000, 1451606400, 1456704000, 1464739200, 1466899200, 1467676800, 1470355200, 1483228800, 1493942400, 1495756800, 1497484800, 1506816000, 1512432000, 1514764800, 1521158400, 1522972800, 1524182400, 1528416000, 1529539200, 1533859200, 1536105600, 1536278400, 1543622400, 1546300800, 1547164800, 1550188800, 1551398400, 1564704000, 1572566400, 157766400, 1577836800, 1585267200, 1587772800, 1597968000, 1601251200, 189302400, 220924800, 252460800, 283996800, 31536000, 315532800, 347155200, 378691200, 410227200, 436492800, 441763200, 473385600, 504921600, 536457600, 567993600, 599616000, 606009600, 63072000, 631152000, 662688000, 672192000, 694224000, 725846400, 738892800, 757382400, 788918400, 790128000, 797212800, 820454400, 852076800, 854755200, 864518400, 866592000, 868233600, 872121600, 883612800, 886291200, 893980800, 912470400, 915148800, 938736000, 946684800, 94694400, 959904000, 965088000, 978307200, 987033600]"),
                    ..SearchBenchConf::BASE
                },
        
                /* the we bench some global / normal search with all the default criterion in the default
                 * order */
                SearchBenchConf {
                    group_name: "basic placeholder",
                    queries: vec![""],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "basic without quote",
                    queries: vec![
                        "john ",
                        "david ",
                        "charles ",
                        "david bowie ",
                        "michael jackson ",
                        "thelonious monk ",
                        "charles mingus ",
                        "marcus miller ",
                        "tamo ",
                        "Notstandskomitee ",
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "basic without quote set-based",
                    queries: vec![
                        "john ",
                        "david ",
                        "charles ",
                        "david bowie ",
                        "michael jackson ",
                        "thelonious monk ",
                        "charles mingus ",
                        "marcus miller ",
                        "tamo ",
                        "Notstandskomitee ",
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "basic with quote",
                    queries: vec![
                        "\"john\" ",
                        "\"david\" ",
                        "\"charles\" ",
                        "\"david bowie\" ",
                        "\"michael jackson\" ",
                        "\"thelonious monk\" ",
                        "\"charles mingus\" ",
                        "\"marcus miller\" ",
                        "\"tamo\" ",
                        "\"Notstandskomitee\" ",
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "prefix search",
                    queries: vec![
                        "s", // 500k+ results
                        "a", //
                        "b", //
                        "i", //
                        "x", // only 7k results
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "prefix search set-based",
                    queries: vec![
                        "s", // 500k+ results
                        "a", //
                        "b", //
                        "i", //
                        "x", // only 7k results
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "prefix search iterative",
                    queries: vec![
                        "s", // 500k+ results
                        "a", //
                        "b", //
                        "i", //
                        "x", // only 7k results
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "words + prefix search",
                    queries: vec![
                        "Someone I l",
                        "billie e",
                        "billie ei",
                        "i am getting o",
                        "i am getting ol",
                        "i am getting old",
                        "prologue 1 a 1",
                        "prologue 1 a 10"
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "words + prefix search set-based",
                    queries: vec![
                        "Someone I l",
                        "billie e",
                        "billie ei",
                        "i am getting o",
                        "i am getting ol",
                        "i am getting old",
                        "prologue 1 a 1",
                        "prologue 1 a 10"
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "words + prefix search iterative",
                    queries: vec![
                        "Someone I l",
                        "billie e",
                        "billie ei",
                        "i am getting o",
                        "i am getting ol",
                        "i am getting old",
                        "prologue 1 a 1",
                        "prologue 1 a 10"
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "large offset",
                    queries: vec![
                        "rock and r",
                    ],
                    offset: Some(770),
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "large offset set-based",
                    queries: vec![
                        "rock and r",
                    ],
                    offset: Some(770),
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "large offset iterative",
                    queries: vec![
                        "rock and r",
                    ],
                    offset: Some(770),
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "many common and different words",
                    queries: vec![
                        "Rock You Hip Hop Folk World Country Electronic Love The",
                        "Rock You Hip Hop Folk World Country Electronic Love",
                        "Rock You Hip Hop Folk World Country Electronic",
                    ],
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
                SearchBenchConf {
                    group_name: "long gibberish",
                    queries: vec![
                        "abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab",
                        "abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab abababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab",
                    ],
                    ..SearchBenchConf::BASE
                },                
                SearchBenchConf {
                    group_name: "phrase 7 words",
                    queries: vec![
                        "\"Music From The Original Motion Picture Soundtrack\""
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "phrase 9 words",
                    queries: vec![
                        "\"Songs And Music From The Original Motion Picture Soundtrack\""
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "synonyms easy",
                    queries: vec![
                        "ily rnr",
                        "rnr ily",
                        "rnr rnr",
                        "ily ily",
                        "mftomp"
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "synonyms easy - set-based",
                    queries: vec![
                        "ily rnr",
                        "rnr ily",
                        "rnr rnr",
                        "ily ily",
                        "mftomp"
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "synonyms easy - iterative",
                    queries: vec![
                        "ily rnr",
                        "rnr ily",
                        "rnr rnr",
                        "ily ily",
                        "mftomp"
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "synonyms medium",
                    queries: vec![
                        "mftomp ily",
                        "mftomp rnr",
                        "rnr mftomp",
                        "ily mftomp",
                        "mftomp rnr ily"
                    ],
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "synonyms medium - set-based",
                    queries: vec![
                        "mftomp ily",
                        "mftomp rnr",
                        "rnr mftomp",
                        "ily mftomp",
                        "mftomp rnr ily"
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlySetBased,
                    ..SearchBenchConf::BASE
                },
                SearchBenchConf {
                    group_name: "synonyms medium - iterative",
                    queries: vec![
                        "mftomp ily",
                        "mftomp rnr",
                        "rnr mftomp",
                        "ily mftomp",
                        "mftomp rnr ily"
                    ],
                    criterion_implementation_strategy: CriterionImplementationStrategy::OnlyIterative,
                    ..SearchBenchConf::BASE
                }
            ]
        )
    ];

    /* we bench the filters with the default request */

    utils::run_benches(base_index_conf, c, benches);
}

criterion_group!(
    name = benches;
    config = { criterion::Criterion::default().sample_size(10) };
    targets = bench_songs
);
criterion_main!(benches);
