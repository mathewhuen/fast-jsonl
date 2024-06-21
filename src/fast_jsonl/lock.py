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

Note that lockfiles are currently saved at
`<file-directory>/.locks/<file-name>.lock` where `<file-directory>` is the
directory containing the target JSONL file and `<file-name>` is the target
JSONL file name.
"""

from pathlib import Path

from filelock import FileLock


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
        Get a filelock path as a subdirectory of on the target file's
        directory.

        Args:
            path (str or pathlike): The target file path.
        """
        path = Path(path)
        lockdir = path.parent / ".locks"
        if not lockdir.exists():
            lockdir.mkdir(exist_ok=True)
        lock_path = lockdir / path.name
        lock_path = lock_path.resolve().as_posix() + ".lock"
        return lock_path

    def acquire(self):
        r"""Acquire the lock."""
        self.lock.acquire()

    def release(self):
        r"""Release the lock."""
        self.lock.release()
