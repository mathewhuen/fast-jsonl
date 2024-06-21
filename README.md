# fast-jsonl

A very simple library for reading large JSONL files.

This library uses a line-bit cache that must be calculated once per a file and
can be used across threads and runtimes until the JSONL file is changed.

## Why?

fast-jsonl is intended mostly for data science and machine learning
workflows where JSONL data is common and large dataset sizes make it
impractical to load the entire dataset into memory (especially when using
multiprocessing).

## Quickstart

### Install

```shell
pip install fast-jsonl
```

### Using JSONL

```python
import fast_jsonl as fj

path = "path_to_file.jsonl"
reader = fj.Reader(path)

print(reader[0])  # print a specific line

for line in reader:  # iterate through lines
    print(line)

print(reader[10:20])  # slice the data
```

## Parameters

### Cache file path
By default, a cache is stored the first time `Reader` is ever called on a
specific file and saved at `<parent>/.fj_cache/<modified-name>.cache.json`
where `<parent>` is the parent directory of the target file and
`<modified-name>` is a modified version of the target filename.

A path for the cache file can be specified by passing `cache_path` to the
reader:

```python
import fast_jsonl as fj

path = "path_to_file.jsonl"
reader = fj.Reader(path, cache_path="path-to-cache")
```

Fast-JSONL will always use the extension `.cache.json` for default paths, but
you are free to specify any extension.

### Re-generating a cache

When initializing, the reader first checks if there is already a valid cache
file at the expected cache file path (either the default path or an explicit
user-passed path).

If a valid cache file exists, the reader will use this file and not generate a
new cache file.
However, you can override this behavior in one of several ways.
- Pass `force_cache=True` to `fast_jsonl.Reader()`:
  - Force the reader to generate a new cache file regardless of whether or not
    one exists.
- Pass `check_cache_time=True` to `fast_jsonl.Reader()`:
  - The reader checks file modification timestamps to see if the data file was
    modified after caching. If it was, a new cache file is generated.
  - Note that this approach only verifies modification times and does not check
    if content was actually changed.
- Pass `check_cache_hash=True` to `fast_jsonl.Reader()`:
  - The reader checks the file content hash and compares it to the hash saved
    during initial caching. Different hashes will trigger a re-cache.

Caches can also be re-generated after reader initialization:
```python
reader.recache()
```
By default, the reader will then re-generate a cache file given the paths
passed during reader initialization.
If you want to save the new cache in a new location, simply pass a `cache_path`
argument to `reader.recache()`.

## Comments about JSONL

If you're working with *large* JSONL files that are either too large to fit in
memory (and you need to index or call specific lines from), this library should help.

If you're working in a production environment, 9 times out of 10 you should
probably invest in a more scalable format than JSONL.
However, especially in machine learning and data science, there are a lot of
use cases for large JSONL files.
