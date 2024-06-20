import os
import time
import pytest
import threading

import fast_jsonl as fj

import data


SLEEP_TIME = 0.1
N_THREADS = 2
TOLERANCE = 0.01


class TestLock:
    def process_data(self, path):
        lock = fj.lock.Lock(path)
        lock.acquire()
        time.sleep(SLEEP_TIME)
        lock.release()

    def test_lock(self, tmp_path):
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
        rate = (end - start) / N_THREADS
        expected_rate = SLEEP_TIME
        assert abs(rate - expected_rate) < TOLERANCE
