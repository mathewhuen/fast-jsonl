# Copyright 2024 Mathew Huerta-Enochian
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

r"""
Modules for working with the JSONL cache file.

These functions are not intended for users to call directly. Instead, users
should initialize a :class:`fast_jsonl.reader.Reader` instance to work with
files.
"""

import os
import json
import hashlib
from pathlib import Path
from typing import Optional, Union

import fast_jsonl as fj


def filepath_to_cachepath_local(file_path):
    r"""
    Get a cache file path based on the location of the given file path.
    """
    path = Path(file_path)
    # assert path.exists()
    cache_dir = path.parent / ".fj_cache"
    if not cache_dir.exists():
        cache_dir.mkdir(parents=True)
    return cache_dir / f"{path.name.replace('.', '-')}.cache.json"


def filepath_to_cachepath_user(file_path):
    r"""
    Get a cache file path based on the user's home directory.
    """
    path = Path(file_path)
    cache_dir = Path.home() / ".local/share/fj_cache"
    if not cache_dir.exists():
        cache_dir.mkdir(parents=True)
    name = path.resolve().as_posix().replace(".", "-").replace("/", "--")
    return cache_dir / f"{name}.cache.json"


def filepath_to_cachepath(file_path):
    r"""
    Get an inferred cache path for a given file path.

    The inferred cache path can be controlled using an environment variable.
    If FAST_JSONL_DIR_METHOD is set to "local", the cache path will be in a
    subdirectory in the target file path's directory. If it is set to "user",
    the cache path will be in <home>/.local/share/fj_cache/ where <home> is the
    user's home directory.


    Args:
        file_path (str or pathlike): The path to the target JSONL file.
    """
    if fj.constants.DIR_METHOD == "local":
        return filepath_to_cachepath_local(file_path)
    elif fj.constants.DIR_METHOD == "user":
        return filepath_to_cachepath_user(file_path)
    else:
        message = (
            f"Unknown value for {fj.constants.DIR_METHOD_ENV} environment "
            f'variable: "{fj.constants.DIR_METHOD}".'
        )
        raise NotImplementedError(message)


def scan_lines(file_path):
    r"""Scan a JSONL file and generate cache data."""
    data = dict()
    f = open(file_path, "r")
    index = 0
    position = 0
    line = f.readline()
    while True:
        if line.endswith("\n"):
            line = line[:-1]
        elif line == "":
            break
        try:
            json_data = json.loads(line)
            data[index] = position
            index += 1
        except:
            pass
        position = f.tell()
        line = f.readline()
    return data


def get_mtime(path):
    return Path(path).stat().st_mtime


def get_hash(file_path, algorithm=hashlib.blake2b, chunksize=8192):
    with open(file_path, "rb") as f:
        file_hash = algorithm()
        while chunk := f.read(chunksize):
            file_hash.update(chunk)
    return file_hash.hexdigest()


def scan_meta(file_path):
    return {
        "mtime": get_mtime(file_path),
        "hash": get_hash(file_path),
    }


def generate_cache_data(file_path):
    # linedata = scan_lines(file_path)
    # metadata = scan_meta(file_path)
    # cache = {
    #    "meta": metadata,
    #    "lines": linedata,
    # }
    # return cache
    return {
        "meta": scan_meta(file_path),
        "lines": scan_lines(file_path),
    }


def save_json(data, path):
    with open(path, "w") as f:
        json.dump(data, f)


def load_json(path):
    with open(path, "rb") as f:
        return json.load(f)


def make_cache(file_path, cache_path=None, cache=None):
    if cache is None:
        cache = generate_cache_data(file_path)
    if cache_path is None:
        cache_path = filepath_to_cachepath(file_path)
    save_json(cache, cache_path)
    return cache


def load_cache(cache_path):
    data = load_json(cache_path)
    data["lines"] = {int(ind): pos for ind, pos in data["lines"].items()}
    return data


def cache_exists(*, file_path=None, cache_path=None):
    r"""
    Return True iff a cache file exists at the target `cache_path`\.
    """
    if cache_path is None:
        cache_path = filepath_to_cachepath(file_path)
    else:
        cache_path = Path(cache_path)
    return cache_path.exists()


def cache_time_valid(file_path, cache):
    r"""
    Return True iff the file's mtime matches the saved mtime.

    Args:
        file_path (str or pathlike, optional): The path to the target file.
        cache (dict): The loaded cache.
    """
    cached_mtime = cache["meta"]["mtime"]
    file_mtime = get_mtime(file_path)
    return cached_mtime >= file_mtime


def cache_hash_valid(file_path, cache):
    r"""
    Return True iff the file hash matches the saved hash.

    This function may be slow for very large JSONL files since the entire file
    must be hashed.

    Args:
        file_path (str or pathlike, optional): The path to the target file.
        cache (dict): The loaded cache.
    """
    cached_hash = cache["meta"]["hash"]
    file_hash = get_hash(file_path)
    return cached_hash == file_hash


def cache_init(
    path: Union[str, os.PathLike],
    cache_path: Optional[Union[str, os.PathLike]] = None,
    force_cache: bool = False,
    check_cache_time: bool = False,
    check_cache_hash: bool = False,
    **kwargs,
):
    r"""
    Initialize cache, modifying existing cache file if necessary.

    Args:
        path (str or pathlike): The path to the JSONL file to cache.
        cache_path (str or pathlike, optional): The path at which a cache file
            should be saved.
            If `cache_path` is None, the correct cache path will be inferred
            from `path` (see :func:`filepath_to_cachepath` for more details).
            If a cache exists at the given or inferred `cache_path`\, the
            default behavior is to use the existing cache (see `force_cache`\,
            `check_cache_time`\, and `check_cache_has` arguments for overriding
            this behavior).
            Defaults to None.
        force_cache (bool, optional): If True, a new cache will always be
            created.
            Defaults to False.
        check_cache_time (bool, optional): If True and a cache already exists
            at the given or inferred `cache_path`\, the last modified time of
            the file at `path` is compared to the recorded time in the cache.
            If the file's last modified time is newer, a new cache is
            generated.
            Defaults to False.
        check_cache_has (bool, optional): If True and a cache already exists
            at the given or inferred `cache_path`\, the hash of the file at
            `path` is compared to the recorded hash in the cache. If the hashes
            do not match, a new cache is generated.
            Defaults to False.

    Note:
        :func:`cache_init` is called by :class:`fast_jsonl.reader.Reader` to
        initialize the cache for a file.
    """
    if cache_path is None:
        cache_path = filepath_to_cachepath(path)
    else:
        message = (
            f"The given file path {path} and cache file path {cache_path} "
            "resolve to the same location."
        )
        assert Path(cache_path).resolve() != Path(path).resolve(), message

    lock = fj.lock.Lock(cache_path)
    lock.acquire()

    if force_cache:
        cache = make_cache(path, cache_path=cache_path)
    else:
        cache = None

    if not cache_exists(cache_path=cache_path):
        cache = make_cache(path, cache_path=cache_path)
    else:
        cache = load_cache(cache_path)

    if check_cache_time and cache and not cache_time_valid(path, cache):
        cache = make_cache(path, cache_path=cache_path)

    if check_cache_hash and cache and not cache_hash_valid(path, cache):
        cache = make_cache(path, cache_path=cache_path)

    # cache exists and is valid
    if cache is None:
        cache = load_cache(cache_path)

    lock.release()
    return cache
