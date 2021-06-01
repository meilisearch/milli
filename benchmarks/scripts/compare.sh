#!/usr/bin/env bash

# Requirements:
# - critcmp. See: https://github.com/BurntSushi/critcmp
# - wget

# Usage
# $ bash compare.sh json_file1 json_file1
# ex: bash compare.sh songs_main_09a4321.json songs_geosearch_24ec456.json

# Checking that critcmp is installed
command -v critcmp > /dev/null 2>&1
if [[ "$?" -ne 0 ]]; then
    echo 'You must install critcmp to make this script working.'
    echo '$ cargo install critcmp'
    echo 'See: https://github.com/BurntSushi/critcmp'
    exit 1
fi

# Checking that wget is installed
command -v wget > /dev/null 2>&1
if [[ "$?" -ne 0 ]]; then
    echo 'You must install wget to make this script working.'
    exit 1
fi

if [[ $# -ne 2 ]]
  then
    echo 'Need 2 arguments.'
    echo 'Usage: '
    echo '  $ bash compare.sh file_to_download1 file_to_download2'
    echo 'Ex:'
    echo '  $ bash compare.sh songs_main_09a4321.json songs_geosearch_24ec456.json'
    exit 1
fi

file1="$1"
file2="$2"
s3_url='https://milli-benchmarks.fra1.digitaloceanspaces.com/critcmp_results'
file1_s3_url="$s3_url/$file1"
file2_s3_url="$s3_url/$file2"
file1_local_path="/tmp/$file1"
file2_local_path="/tmp/$file2"

if [[ ! -f "$file1_local_path" ]]; then
    wget "$file1_s3_url" -O "$file1_local_path"
    if [[ "$?" -ne 0 ]]; then
	    echo 'wget command failed. Check your configuration'
	    exit 1
    fi
else
    echo "$file1 already present in /tmp, no need to download."
fi

if [[ ! -f "$file2_local_path" ]]; then
    wget "$file2_s3_url" -O "$file2_local_path"
    if [[ "$?" -ne 0 ]]; then
	    echo 'wget command failed. Check your configuration'
	    exit 1
    fi
else
    echo "$file2 already present in /tmp, no need to download."
fi

critcmp --color always "$file1_local_path" "$file2_local_path"
