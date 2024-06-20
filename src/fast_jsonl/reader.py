import os
import json
from typing import Optional, Union

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
            path (str or pathlike):
            cache_path (str or pathlike, optional):
            force_cache (bool, optional):
            check_cache_time (bool, optional):
            check_cache_hash (bool, optional):

        Note:
            The `force_cache` argument overrides the two cache check arguments:
            if `force_cache=True` is passed, both `check_cache_time` and
            `check_cache_hash` will be ignored.
        """
        self.path = path
        self.cache_path = cache_path
        self.cache = fj.cache.cache_init(
            path,
            cache_path=cache_path,
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
            cache_path=cache_path,
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
        A convenience wrapper for :meth:`Reader.recache()` with
        `force_cache=True`\.

        Args:
            cache_path (str or os.PathLike, optional): Defaults to None
        """
        self.recache(cache_path=cache_path, force_cache=True)

    def __len__(self):
        return len(self.cache["lines"])

    def _slice(self, start, stop, step):
        # output = list()
        # for idx in range(start, stop, step):
        #    output.append(self._getitem(idx))
        # return output
        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        if step is None:
            step = 1
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

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            return self._slice(idx.start, idx.stop, idx.step)
        return self._getitem(idx)

    def __iter__(self):
        for idx in range(len(self)):
            yield self[idx]
