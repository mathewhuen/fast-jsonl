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
CLI for basic cache management.

Use `fj_precache` (mapped to :func:`precache`) from a shell to precache one or
more files. The following flags can be specified:
* `--files`\: A comma-separated list of files to cache.
* `--threads`\: The number of threads to use for caching. Each thread can
  process one file at a time.
* `--verbose`\: Print progress.
"""

import logging

logger = logging.getLogger(__name__)

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fast_jsonl as fj


def precache_message(count, total, error_record=None):
    if error_record is None:
        print(f"({count}/{total}) Caching completed successfully.")
    else:
        print(f"({count}/{total}) Caching failed with: {error_record}")


def precache_singlethreaded(files, verbose):
    count = 0
    total = len(files)
    for file in files:
        error = False
        error_record = None
        try:
            _ = fj.cache.cache_init(file)
        except Exception as e:
            error = True
            error_record = repr(e)
        count += 1
        if verbose:
            precache_message(count, total, error_record)


def precache_multithreaded(files, threads, verbose):
    count = 0
    total = len(files)
    executor = ThreadPoolExecutor(max_workers=threads)
    futures = [executor.submit(fj.cache.cache_init, file) for file in files]
    for future in futures:
        error = False
        error_record = None
        try:
            _ = future.result(timeout=None)
        except Exception as e:
            error = True
            error_record = repr(e)
        count += 1
        if verbose:
            precache_message(count, total, error_record)


def get_precache_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--files",
        "-f",
        help="A comma-separated list of files to parse.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--threads",
        "-t",
        help=(
            "The number of threads to use. If 1, threading is not used."
            "Defaults to 1."
        ),
        type=int,
        default=1,
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Print caching progress",
        action="store_true",
    )
    return parser


def get_precache_args(parser, files, threads, verbose):
    args = parser.parse_args().__dict__

    # override if arguments passed manually
    if files is not None:
        args["files"] = files
    if threads is not None:
        args["threads"] = threads
    if verbose is not None:
        args["verbose"] = verbose
    return args


def precache(
    files: str = None,
    threads: int = None,
    verbose: bool = None,
):
    r"""
    Pre-cache one or more files.
    """
    args = get_precache_args(
        parser=get_precache_arg_parser(),
        files=files,
        threads=threads,
        verbose=verbose,
    )
    files = [Path(file) for file in args["files"].split(",")]

    if args["threads"] > 1:
        precache_multithreaded(files, args["threads"], args["verbose"])
    else:
        precache_singlethreaded(files, args["verbose"])
