Benchmarks
==========

## TOC

- [Run the benchmarks](#run-the-benchmarks)
- [Comparison between benchmarks](#comparison-between-benchmarks)
- [Datasets](#datasets)

## Run the benchmarks

### On our private server

The Meili team has self-hosted his own GitHub runner to run benchmarks on our dedicated bare metal server.

To trigger the benchmark workflow:
- Go to the `Actions` tab of this repository.
- Select the `Benchmarks` workflow on the left.
- Click on `Run workflow` in the blue banner.
- Select the branch on which you want to run the benchmarks and select the dataset you want (default: `songs`).
- Finally, click on `Run workflow`.

This GitHub workflow will run the benchmarks and push the `critcmp` report to a DigitalOcean Space (= S3).

The name of the uploaded file is displayed in the workflow.

_[More about critcmp](https://github.com/BurntSushi/critcmp)._

💡 To compare the just-uploaded benchmark with another one, check out the [next section](#comparison-between-benchmarks).

### On your machine

To run all the benchmarks (~5h):

```bash
cargo bench
```

To run only the `songs` (~1h), `wiki` (~3h) or `indexing` (~4h) benchmark:

```bash
cargo bench --bench <dataset name>
```

By default, the benchmarks will be downloaded and uncompressed automatically in the target directory.<br>
If you don't want to download the datasets every time you update something on the code, you can specify a custom directory with the environment variable `MILLI_BENCH_DATASETS_PATH`:

```bash
mkdir ~/datasets
MILLI_BENCH_DATASETS_PATH=~/datasets cargo bench --bench songs # the three datasets are downloaded
touch build.rs
MILLI_BENCH_DATASETS_PATH=~/datasets cargo bench --bench songs # the code is compiled again but the datasets are not downloaded
```

## Comparison between benchmarks

The benchmark reports we push are generated with `critcmp`. Thus, we use `critcmp` to generate comparison results between 2 benchmarks.

We provide a script to download and display the comparison report.

Requirements:
- `grep`
- `curl`
- [`critcmp`](https://github.com/BurntSushi/critcmp)

List the available file in the DO Space:

```bash
./benchmarks/script/list.sh
```
```bash
songs_main_09a4321.json
songs_geosearch_24ec456.json
```

Run the comparison script:

```bash
./benchmarks/scripts/compare.sh songs_main_09a4321.json songs_geosearch_24ec456.json
```

## Datasets

The benchmarks are available for the following datasets:
- `songs`
- `wiki`
- `movies`

### Songs

`songs` is a subset of the [`songs.csv` dataset](https://milli-benchmarks.fra1.digitaloceanspaces.com/datasets/songs.csv.gz).

It was generated with this command:

```bash
xsv sample --seed 42 1000000 songs.csv -o smol-songs.csv
```

_[Download the generated `songs` dataset](https://milli-benchmarks.fra1.digitaloceanspaces.com/datasets/smol-songs.csv.gz)._

### Wiki

`wiki` is a subset of the [`wikipedia-articles.csv` dataset](https://milli-benchmarks.fra1.digitaloceanspaces.com/datasets/wiki-articles.csv.gz).

It was generated with the following command:

```bash
xsv sample --seed 42 500000 wiki-articles.csv -o smol-wiki-articles.csv
```

### Movies

`movies` is a really small dataset we uses as our example in the [getting started](https://docs.meilisearch.com/learn/getting_started/)

_[Download the `movies` dataset](https://docs.meilisearch.com/movies.json)._

