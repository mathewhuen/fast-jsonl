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
Convenience module for working with filelocks.

:class:`Lock` is initialized automatically when loading caches to ensure that
multiple concurrent calls for caching do not proceed in parallel.
This means that all concurrent cache calls will wait until the filelock is
lifted, and if the JSONL file is not changed (which it should not be changed
during runtime) redundant caching can be avoided.

.. note::
   Lockfiles are currently saved using a similar strategy with that of
   cache file paths:

   * If the environment variable `FAST_JSONL_DIR_METHOD` is unset
     or set to "user", lockfiles will be stored at
     `<home>/.local/share/fj_locks/<modified-path>/<path-hash>.lock` where
     `<home>` is the user's home directory, `<modified-path>` is the posix path
     to the target file with all "/" replaced with "--", and `<path-hash>` a
     sha256 hash of the unmodified file path.

   * If `FAST_JSONL_DIR_METHOD` is set to "local", lockfiles are saved at
     `<file-directory>/.locks/<file-name>.lock` where `<file-directory>` is the
     directory containing the target file and `<file-name>` is the unmodified
     name of the target file.
"""

from pathlib import Path
from filelock import FileLock

import fast_jsonl as fj


def get_filelock_path_local(path):
    r"""
    Get a filelock path as a subdirectory of on the target file's
    directory.

    Args:
        path (str or pathlike): The target file path.
    """
    path = Path(path)
    lockdir = path.parent / ".locks"
    if not lockdir.exists():
        lockdir.mkdir(parents=True, exist_ok=True)
    lock_path = lockdir / path.name
    lock_path = lock_path.resolve().as_posix() + ".lock"
    return lock_path


def get_filelock_path_user(path):
    r"""
    Get a filelock path as a subdirectory of the user's home directory.

    Args:
        path (str or pathlike): The target file path.
    """
    path = Path(path)
    lockdir = Path.home() / ".local/share/fj_locks"
    # lockdir = path.parent / ".locks"
    if not lockdir.exists():
        lockdir.mkdir(parents=True, exist_ok=True)
    posix_path = path.resolve().as_posix()
    modified_path = posix_path.replace("/", "--")
    locksubdir = lockdir / modified_path
    if not locksubdir.exists():
        locksubdir.mkdir(parents=True, exist_ok=True)
    hashed_name = fj.cache.get_text_hash(posix_path)
    return locksubdir / f"{hashed_name}.lock"


class Lock:
    r"""
    Convenience class for handling `filelock.FileLock`\.
    """
    def __init__(self, path):
        r"""
        Initialize :class:`Lock`\.

        Args:
            path (str or pathlike): The target file path.
        """
        self.path = self.get_filelock_path(path)
        self.lock = FileLock(self.path)

    @staticmethod
    def get_filelock_path(path):
        r"""
        Get a filelock path.

        Args:
            path (str or pathlike): The target file path.
        """
        if fj.constants.DIR_METHOD == "local":
            return get_filelock_path_local(path)
        elif fj.constants.DIR_METHOD == "user":
            return get_filelock_path_user(path)
        else:
            message = (
                f"Unknown value for {fj.constants.DIR_METHOD_ENV} environment "
                f'variable: "{fj.constants.DIR_METHOD}".'
            )
            raise ValueError(message)

    def acquire(self):
        r"""Acquire the lock."""
        self.lock.acquire()

    def release(self):
        r"""Release the lock."""
        self.lock.release()
