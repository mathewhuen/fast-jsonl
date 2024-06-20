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
