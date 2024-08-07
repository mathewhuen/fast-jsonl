import os
import time
import pytest
import threading

import fast_jsonl as fj

import data
import utils


SLEEP_TIME = 0.1
N_THREADS = 4
TOLERANCE = 0.01


class TestLock:
    def process_data(self, path):
        lock = fj.lock.Lock(path)
        lock.acquire()
        time.sleep(SLEEP_TIME)
        lock.release()

    @pytest.mark.parametrize("dir_method", ["user", "local", None])
    def test_lock(self, tmp_path, dir_method):
        with utils.modify_dir_method(dir_method):
            path = tmp_path / "data.json"
            threads = [
                threading.Thread(target=self.process_data, args=(path,))
                for _ in range(N_THREADS)
            ]

            start = time.time()
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            end = time.time()
            measured = (end - start)
            serial = SLEEP_TIME * N_THREADS
            assert measured > serial
