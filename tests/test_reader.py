import os
import time
import pytest
import itertools

import fast_jsonl as fj


import data


class TestReader:
    def test_init(self, tmp_path):
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_ten)
        _ = fj.Reader(path)

    @pytest.mark.parametrize(
        "cache_path,precache,modify_file,params",
        list(itertools.product(
            [None, "cache.json"],
            [False, True],
            [None, "time", "content"],
            [{}, {"force_cache": True}, {"check_cache_time": True}, {"check_cache_hash": True}],
        ))
    )
    def test_params(
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
        _ = fj.Reader(path, cache_path=cache_path, **params)

    @pytest.mark.parametrize(
        "instances",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_length(self, tmp_path, instances):
        path = tmp_path / "data.jsonl"
        data.save_data(path, instances)
        reader = fj.Reader(path)
        assert len(reader) == len(instances)

    @pytest.mark.parametrize(
        "instances",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_getitem(self, tmp_path, instances):
        path = tmp_path / "data.jsonl"
        data.save_data(path, instances)
        reader = fj.Reader(path)
        assert all([instances[i] == reader[i] for i in range(len(instances))])

    @pytest.mark.parametrize(
        "instances,start,stop,step",
        list(itertools.product(
            [data.empty_zero, data.empty_ten, data.various_ten],
            [0, None],
            [0, None, -1],
            [1, 2, None],
        ))
    )
    def test_slice(self, tmp_path, instances, start, stop, step):
        path = tmp_path / "data.jsonl"
        data.save_data(path, instances)
        reader = fj.Reader(path)[start:stop:step]
        assert all([a == b for a, b in zip(instances[::step], reader)])

    @pytest.mark.parametrize(
        "instances",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_iterable(self, tmp_path, instances):
        path = tmp_path / "data.jsonl"
        data.save_data(path, instances)
        reader = fj.Reader(path)
        assert all([a == b for a, b in zip(instances, reader)])

    @pytest.mark.parametrize(
        "instances",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_iter(self, tmp_path, instances):
        path = tmp_path / "data.jsonl"
        data.save_data(path, instances)
        reader = iter(fj.Reader(path))
        assert all([a == b for a, b in zip(instances, reader)])

    @pytest.mark.parametrize(
        "cache_name,params",
        list(itertools.product(
            [None, "cache.json"],
            [{}, {"force_cache": True}, {"check_cache_time": True}, {"check_cache_hash": True}],
        ))
    )
    def test_recache(
        self,
        tmp_path,
        cache_name,
        params,
    ):
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_ten)

        reader = fj.Reader(path)

        if params.get("check_cache_time"):
            time.sleep(0.01)

        data.save_data(path, data.various_ten, mode="w")
        if cache_name is not None:
            cache_path = tmp_path / cache_name
        else:
            cache_path = None
        reader.recache(cache_path=cache_path, **params)

        # if any check flag or force flag or new cache path flag is given,
        # will recache.
        if (
            params.get("force_cache")
            or params.get("check_cache_time")
            or params.get("check_cache_hash")
            or cache_path is not None
        ):
            assert list(reader) == data.various_ten
        else:
            # recache not triggered since
            # cache path, force, and check options not given.
            with pytest.raises(RuntimeError):
                _ = list(reader)

    @pytest.mark.parametrize(
        "cache_name",
        [None, "cache.json"],
    )
    def test_force_recache(
        self,
        tmp_path,
        cache_name,
    ):
        path = tmp_path / "data.jsonl"
        data.save_data(path, data.empty_ten)

        reader = fj.Reader(path)

        data.save_data(path, data.various_ten, mode="w")

        if cache_name is not None:
            cache_path = tmp_path / cache_name
        else:
            cache_path = None

        reader.force_recache(cache_path=cache_path)

        assert list(reader) == data.various_ten
