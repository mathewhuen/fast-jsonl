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

from pathlib import Path

from filelock import FileLock


class Lock:
    def __init__(self, path):
        self.path = self.get_filelock_path(path)
        self.lock = FileLock(self.path)

    @staticmethod
    def get_filelock_path(path):
        path = Path(path)
        lockdir = path.parent / ".locks"
        if not lockdir.exists():
            lockdir.mkdir(exist_ok=True)
        lock_path = lockdir / path.name
        lock_path = lock_path.resolve().as_posix() + ".lock"
        return lock_path

    def acquire(self):
        self.lock.acquire()

    def release(self):
        self.lock.release()
