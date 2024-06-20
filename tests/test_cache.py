import os
import json
import time
import pytest
import hashlib
import filecmp
import itertools
from pathlib import Path

import fast_jsonl as fj


import data
import utils


class TestCache:
    def get_home(self):
        if os.environ.get("HOME") is not None:
            return Path(os.environ["HOME"])
        if os.environ.get("USERPROFILE") is not None:
            return Path(os.environ["USERPROFILE"])
        raise RuntimeError()

    def _get_expected_cachepath_local(self, path):
        target_dir = path.parent / ".fj_cache"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{path.name.replace('.', '-')}.cache.json"
        return target_path

    def test_filepath_to_cachepath_local(self, tmp_path):
        path = tmp_path / "file.jsonl"
        cache_path = fj.cache.filepath_to_cachepath_local(path)
        target_path = self._get_expected_cachepath_local(path)
        assert cache_path.resolve() == target_path.resolve()

    def _get_expected_cachepath_user(self, path):
        target_dir = self.get_home() / ".local/share/fj_cache"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = (
            target_dir
            / f"{path.as_posix().replace('/', '--').replace('.', '-')}.cache.json"
        )
        return target_path

    def test_filepath_to_cachepath_user(self, tmp_path):
        path = tmp_path / "file.jsonl"
        cache_path = fj.cache.filepath_to_cachepath_user(path)
        target_path = self._get_expected_cachepath_user(path)
        assert cache_path.resolve() == target_path.resolve()

    @pytest.mark.parametrize("dir_method", ["local", "user"])
    def test_filepath_to_cachepath(self, tmp_path, dir_method):
        path = tmp_path / "file.jsonl"
        with utils.modify_dir_method(dir_method):
            cache_path = fj.cache.filepath_to_cachepath(path)
            if dir_method == "local":
                target_path = self._get_expected_cachepath_local(path)
            else:
                target_path = self._get_expected_cachepath_user(path)
            assert cache_path == target_path

    def _assert_cache_lines(self, path, cache_lines, target_data):
        f = open(path, "rb")
        byte_data = f.read()
        for i in range(len(cache_lines)):
            line = byte_data[cache_lines[i]:cache_lines.get(i+1)].decode()
            if line.endswith("\n"):
                line = line[:-1]
            line_data = json.loads(line)
            assert line_data == target_data[i]

    @pytest.mark.parametrize(
        "target_data",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_scan_lines(self, tmp_path, target_data):
        path = tmp_path / "data.jsonl"
        data.save_data(path, target_data)
        cache_lines = fj.cache.scan_lines(path)
        self._assert_cache_lines(
            path,
            cache_lines=cache_lines,
            target_data=target_data,
        )

    def test_get_mtime(self, tmp_path):
        path = tmp_path / "data.jsonl"
        start = time.time()
        time.sleep(0.01)
        data.save_data(path, data.empty_zero)
        time.sleep(0.01)
        end = time.time()
        mtime = fj.cache.get_mtime(path)
        assert start <= mtime <= end

    def test_get_hash(self, tmp_path):
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_zero)
        cache_hash = fj.cache.get_hash(path)
        target_hash = hashlib.blake2b()
        with open(path, "rb") as f:
            target_hash.update(f.read())
        assert cache_hash == target_hash.hexdigest()

    def test_scan_meta(self, tmp_path):
        path = tmp_path / "data.jsonl"

        start = time.time()
        time.sleep(0.01)
        data.save_data(path, data.empty_zero)
        time.sleep(0.01)
        end = time.time()

        meta = fj.cache.scan_meta(path)

        target_hash = hashlib.blake2b()
        with open(path, "rb") as f:
            target_hash.update(f.read())

        assert meta["hash"] == target_hash.hexdigest()
        assert start <= meta["mtime"] <= end

    def test_generate_cache_data(self, tmp_path):
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_zero)

        cache_0 = {
            "meta": fj.cache.scan_meta(path),
            "lines": fj.cache.scan_lines(path),
        }
        cache_1 = fj.cache.generate_cache_data(path)

        assert cache_0 == cache_1

    def test_save_json(self, tmp_path):
        path_0 = tmp_path / "data_0.json"
        path_1 = tmp_path / "data_1.json"
        with open(path_0, "w") as f:
            json.dump(data.empty_zero, f)
        fj.cache.save_json(data.empty_zero, path_1)
        assert filecmp.cmp(path_0, path_1)

    def test_load_json(self, tmp_path):
        path = tmp_path / "data.json"
        with open(path, "w") as f:
            json.dump(data.empty_zero, f)
        loaded_data = fj.cache.load_json(path)
        assert loaded_data == data.empty_zero

    @pytest.mark.parametrize(
        "target_data",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_make_cache(self, tmp_path, target_data):
        path = tmp_path / "data.jsonl"
        cache_path = tmp_path / "cache.json"

        start = time.time()
        time.sleep(0.01)
        data.save_data(path, target_data)
        time.sleep(0.01)
        end = time.time()

        cache = fj.cache.make_cache(path, cache_path=cache_path)
        with open(cache_path, "rb") as f:
            cache_loaded = json.load(f)
        cache_loaded["lines"] = {
            int(k): v for k, v in cache_loaded["lines"].items()
        }

        target_hash = hashlib.blake2b()
        with open(path, "rb") as f:
            target_hash.update(f.read())

        assert cache == cache_loaded
        self._assert_cache_lines(
            path,
            cache_lines=cache["lines"],
            target_data=target_data,
        )
        assert start <= cache["meta"]["mtime"] <= end
        assert cache["meta"]["hash"] == target_hash.hexdigest()

    @pytest.mark.parametrize(
        "target_data",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_get_cache(self, tmp_path, target_data):
        path = tmp_path / "data.jsonl"
        cache_path = tmp_path / "cache.json"
        data.save_data(path, target_data)
        fj.cache.make_cache(path, cache_path=cache_path)
        cache = fj.cache.load_cache(cache_path)
        self._assert_cache_lines(
            path,
            cache_lines=cache["lines"],
            target_data=target_data,
        )

    def test_fail_cache_exists(self, tmp_path):
        # check with default path
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_zero)
        assert not fj.cache.cache_exists(file_path=path)
        cache_path = tmp_path / "cache.json"
        assert not fj.cache.cache_exists(cache_path=cache_path)

    def test_cache_exists(self, tmp_path):
        # check with default path
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_zero)
        fj.cache.make_cache(path)
        assert fj.cache.cache_exists(file_path=path)

        # check custom path
        cache_path = tmp_path / "cache.json"
        fj.cache.make_cache(path, cache_path=cache_path)
        assert fj.cache.cache_exists(cache_path=cache_path)

    def test_cache_time_valid(self, tmp_path):
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_zero)
        cache = fj.cache.make_cache(path)
        assert fj.cache.cache_time_valid(file_path=path, cache=cache)

    def test_fail_cache_time_valid(self, tmp_path):
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_zero)
        cache = fj.cache.make_cache(path)
        time.sleep(0.01)
        data.save_data(path, data.empty_ten)
        assert not fj.cache.cache_time_valid(file_path=path, cache=cache)

    def test_cache_hash_valid(self, tmp_path):
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_zero)
        cache = fj.cache.make_cache(path)
        assert fj.cache.cache_hash_valid(file_path=path, cache=cache)

    def test_fail_cache_hash_valid(self, tmp_path):
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_zero)
        cache = fj.cache.make_cache(path)
        data.save_data(path, data.empty_ten)
        assert not fj.cache.cache_hash_valid(file_path=path, cache=cache)

    @pytest.mark.parametrize(
        "cache_path,precache,modify_file,params",
        list(itertools.product(
            [None, "cache.json"],
            [False, True],
            [None, "time", "content"],
            [{}, {"force_cache": True}, {"check_cache_time": True}, {"check_cache_hash": True}],
        ))
    )
    def test_cache_init_params(
        self,
        tmp_path,
        cache_path,
        precache,
        modify_file,
        params,
    ):
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_ten)
        if precache:
            fj.cache.make_cache(path, cache_path=cache_path)
        if precache and modify_file == "time":
            data.save_data(path, data.empty_ten)
        if precache and modify_file == "content":
            data.save_data(path, data.various_ten)
        _ = fj.cache.cache_init(path, cache_path=cache_path, **params)
