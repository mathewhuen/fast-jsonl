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
:class:`Reader` is the recommended way to work with large JSONL files.
This class records the byte location of each line in JSONL files on first load
and then uses the location for quickly reading lines.

This is helpful for files that are too large to be loaded into memory but need
non-serial access. I.e., ideal for large JSONL files that should be read by
index like in an ML training workflow.

Use like:

>>> import fast_jsonl as fj
>>> reader = fj.Reader("path_to_file.jsonl")
>>>
>>> # Slice or index to get subsets:
>>> range_of_values = reader[100:105]
>>>
>>> # Use with a PyTorch DataLoader:
>>> from torch.utils.data import DataLoader
>>> data = DataLoader(reader)

If your workflow may lead to changes in the original data file, the cache will
need to be re-generated. Include one of the following flags when initializing
the reader to enforce this:

* `force_cache=True`\: Always re-cache.
* `check_cache_time=True`\: Re-cache if the file was modified after caching.
* `check_cache_hash=True`\: Re-cache if the hash is different from the cached
  hash.

Note that currently, cache checks are only performed on initial load and JSONL
data files should not be modified during runtime. We plan on adding continuous
checks to catch data changes in the future.
"""

import os
import json
from collections.abc import Iterable
from typing import List, Optional, Union

import fast_jsonl as fj


class Reader:
    r"""Class for reading JSONL files."""

    def __init__(
        self,
        path: Union[str, os.PathLike],
        cache_path: Optional[Union[str, os.PathLike]] = None,
        force_cache: bool = False,
        check_cache_time: bool = False,
        check_cache_hash: bool = False,
        **kwargs,
    ):
        r"""
        Initialize :class:`Reader`\.

        Args:
            path (str or pathlike): Path to JSONL file to parse.
            cache_path (str or pathlike, optional): Path to JSONL cache file.
                If None, inferred from `path` argument.
                Defaults to None.
            force_cache (bool, optional): If True, generate a new cache file
                regardless of if one already exists.
                Defaults to False.
            check_cache_time (bool, optional): If True, overwrite an existing
                cache file if the mtime of the target JSONL file specified by
                `path` is newer than the recorded mtime in the existing cache
                file.
            check_cache_hash (bool, optional): If True, overwrite an existing
                cache file if the hash of the target JSONL file specified by
                `path` is newer than the recorded hash in the existing cache
                file.

        Note:
            The `force_cache` argument overrides the two cache check arguments:
            if `force_cache=True` is passed, both `check_cache_time` and
            `check_cache_hash` will be ignored.
        """
        self.path = path
        self.cache_path = cache_path
        self.cache = None
        self.recache(
            force_cache=force_cache,
            check_cache_time=check_cache_time,
            check_cache_hash=check_cache_hash,
            **kwargs,
        )

    def recache(
        self,
        cache_path: Optional[Union[str, os.PathLike]] = None,
        force_cache: bool = False,
        check_cache_time: bool = False,
        check_cache_hash: bool = False,
        **kwargs,
    ):
        r"""Recache the file."""
        if cache_path is None:
            cache_path = self.cache_path
        else:
            self.cache_path = cache_path
        self.cache = fj.cache.cache_init(
            self.path,
            cache_path=self.cache_path,
            force_cache=force_cache,
            check_cache_time=check_cache_time,
            check_cache_hash=check_cache_hash,
            **kwargs,
        )

    def force_recache(
        self,
        cache_path: Optional[Union[str, os.PathLike]] = None,
    ):
        r"""
        A convenience wrapper for :meth:`Reader.recache` with
        `force_cache=True`\.

        Args:
            cache_path (str or os.PathLike, optional): Defaults to None
        """
        self.recache(cache_path=cache_path, force_cache=True)

    def __len__(self):
        return len(self.cache["lines"])

    def _slice(self, start, stop, step):
        if step is None:
            step = 1
        if start is None:
            start = 0 if step > 0 else (len(self) - 1)
        if stop is None:
            stop = len(self) if step > 0 else 0
        return [self._getitem(idx) for idx in range(start, stop, step)]

    def _getitem(self, idx):
        position = self.cache["lines"][idx]
        f = open(self.path, "r")
        f.seek(position)
        line = f.readline()
        if line.endswith("\n"):
            line = line[:-1]
        elif line == "":  # Tried to read from beyond the last line
            message = (
                f"The Reader failed to get the {idx} line from the data file. "
                "It appears that the number of cached lines is greater than "
                "the number of actual lines."
            )
            raise RuntimeError(message)
        try:
            return json.loads(line)
        except Exception as e:
            message = (
                f"JSONL data at line {idx} could not be parsed into a JSON "
                "object. Please check that it is not malformed."
            )
            raise RuntimeError(message)

    def _getitems(self, ids):
        return [self._getitem(idx) for idx in ids]

    def __getitem__(self, idx):
        r"""
        Get item(s) from the target JSONL file.

        Call using slice or index syntax:

        >>> import fast_jsonl as fj
        >>> reader = fj.Reader("path_to_file.jsonl")
        >>> reader[0]  # get first line
        >>> reader[4:8:-1]  # get 7th through 4th lines in reverse order
        """
        if isinstance(idx, slice):
            return self._slice(idx.start, idx.stop, idx.step)
        elif isinstance(idx, Iterable):
            return self._getitems(idx)
        return self._getitem(idx)

    def __iter__(self):
        for idx in range(len(self)):
            yield self[idx]


class MultiReader(Reader):
    r"""Class for reading multiple JSONL files."""

    def __init__(
        self,
        path: List[Union[str, os.PathLike]],
        cache_path: List[Optional[Union[str, os.PathLike]]] = None,
        force_cache: bool = False,
        check_cache_time: bool = False,
        check_cache_hash: bool = False,
        **kwargs,
    ):
        r"""
        Initialize :class:`MultiReader`\.

        The api for :class:`MultiReader` is the same as that for
        :class:`Reader`\ and can be used almost identically to read from
        multiple JSONL as if they were a single file.

        The biggest difference when working with this class is that a list of
        file paths should be passed when initializing an instance, rather than
        just a single path when initializing :class:`Reader`\.

        Under the hood, :class:`MultiReader` creates a :class:`Reader` instance
        for each file path.

        Args:
            path (list[str or pathlike]): List of paths to JSONL files.
            cache_path (list[str or pathlike], optional): Paths to JSONL cache
                files.
                If None, inferred from `path` argument.
                Defaults to None.
            force_cache (bool, optional): If True, generate a new cache file
                regardless of if one already exists.
                Defaults to False.
            check_cache_time (bool, optional): If True, overwrite an existing
                cache file if the mtime of the target JSONL file specified by
                `path` is newer than the recorded mtime in the existing cache
                file.
            check_cache_hash (bool, optional): If True, overwrite an existing
                cache file if the hash of the target JSONL file specified by
                `path` is newer than the recorded hash in the existing cache
                file.

        Note:
            The `force_cache` argument overrides the two cache check arguments:
            if `force_cache=True` is passed, both `check_cache_time` and
            `check_cache_hash` will be ignored.
        """
        if cache_path is None:
            cache_path = [None for _ in path]
        self.path = path
        self.cache_path = cache_path
        self.readers = None
        self.readers_info = None
        self.recache(
            force_cache=force_cache,
            check_cache_time=check_cache_time,
            check_cache_hash=check_cache_hash,
        )

    def recache(
        self,
        cache_path: Optional[Union[str, os.PathLike]] = None,
        force_cache: bool = False,
        check_cache_time: bool = False,
        check_cache_hash: bool = False,
        **kwargs,
    ):
        if cache_path is None:
            cache_path = self.cache_path
        else:
            assert isinstance(cache_path, list)
            assert len(cache_path) == len(self.path)
            self.cache_path = cache_path
        self.readers = [
            Reader(
                single_path,
                cache_path=single_cache_path,
                force_cache=force_cache,
                check_cache_time=check_cache_time,
                check_cache_hash=check_cache_hash,
                **kwargs,
            )
            for single_path, single_cache_path in zip(
                self.path,
                self.cache_path,
            )
        ]
        lengths = [len(reader) for reader in self.readers]
        self.readers_info = dict()
        cumulative = {
            i: sum(lengths[: i + 1]) for i in range(len(self.readers))
        }
        self.readers_info["cumulative_sizes"] = cumulative
        self.readers_info["instance_reader_map"] = {
            idx: group
            for group in range(len(self.readers))
            for idx in range(
                cumulative[group - 1] if group > 0 else 0,
                cumulative[group],
            )
        }

    def __len__(self):
        return sum(len(reader) for reader in self.readers)

    def _getitem(self, idx):
        reader_index = self.readers_info["instance_reader_map"][idx]
        if reader_index > 0:
            floor = self.readers_info["cumulative_sizes"][reader_index - 1]
        else:
            floor = 0
        sub_index = idx - floor
        return self.readers[reader_index][sub_index]
