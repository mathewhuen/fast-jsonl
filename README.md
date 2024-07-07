# fast-jsonl

A very simple library for reading large JSONL files.

This library uses a line-byte cache that must be calculated once per a file and
can be used across threads and runtimes until the JSONL file is changed.

## Why?

fast-jsonl is intended for data science and machine learning workflows that use
large JSONL files that cannot be practically loaded into memory (especially
when using multiprocessing).

In most JSONL reading scenarios, fast-jsonl should work well. Some examples of
use-cases for fast-jsonl:
- Random access to one or multiple JSONL files (e.g., PyTorch DataLoaders),
- Slices of or index-based access to one or multiple JSONL files (e.g.,
  exploratory data analysis), or
- Combining multiple data files into a single data object.

When to not use fast-jsonl for reading?
- If your workflow uses only a single small JSONL file, then just loading the
  data into memory and using directly should be sufficient.
- If your workflow uses a single large JSONL file, and you will only read data
  sequentially starting from the first line, we recommend using the
  [jsonlines](https://github.com/wbolster/jsonlines) or
  [orjsonl](https://github.com/umarbutler/orjsonl) libraries.
- If you are working in a production environment and data files are often
  modified. In this scenario, it would probably be better to invest in a
  traditional database.

## Quickstart

### Install

```shell
pip install fast-jsonl
```

### Using fast-jsonl

```python
import fast_jsonl as fj

path = "path_to_file.jsonl"
reader = fj.Reader(path)

print(reader[0])  # print a specific line

for line in reader:  # iterate through lines
    print(line)

print(reader[10:20])  # slice the data
```

fast_jsonl can also read from multiple JSONL files:
```python
import fast_jsonl as fj

paths = ["path_to_file_0.jsonl", "path_to_file_1.jsonl"]
reader = fj.MultiReader(paths)
```

If the target JSONL file has changed, make sure to generate a new cache file by
passing `force_cache=True`:
```python
import fast_jsonl as fj

reader = fj.Reader("<path-to-changed-file.jsonl>", force_cache=True)
```

## Parameters

### Cache file path

By default, a cache is stored the first time `Reader` is ever called on a
specific file and saved at
`<user-home>/.local/share/fj_cache/<modified-name>/<hash>.cache.json`
where `<usser-home>` is the user's home directory, `<modified-name>` is the
target file's path with all directory separators replaced with "--", and
`<hash>` is a hash of the target file.
Hashes are used to allow semi-readable cache names (via the modified name)
while avoiding potential collisions introduced by replacing the path separator
with "--".

A path for the cache file can be specified by passing `cache_path` to the
reader:

```python
import fast_jsonl as fj

path = "path_to_file.jsonl"
reader = fj.Reader(path, cache_path="path-to-cache")
```

fast-jsonl uses the extension `.cache.json` for default paths when the cache
path is not given, but you are free to specify any extension.

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

## TODO

- Add tests for pre-caching CLI.
- Add benchmarks code and section to readme.
- Add multi-threaded caching.
- Add support for faster JSON backends (ex: orjson)
- Change slicing to use serial loading to avoid redundant byte seek calls.
- Allow multi-threaded slicing for faster slice loading.
- Change slicing to cutoff at 0 and len(reader)-1 so that out of bounds slices
  behave like builtin lists.
