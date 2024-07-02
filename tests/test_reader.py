import os
import time
import copy
import types
import pytest
import itertools
from pathlib import Path
from abc import abstractmethod

import fast_jsonl as fj

import data


class BaseTests:
    reader = None
    n_files = None  # set to a non-negative integer for MultiReader

    @abstractmethod
    def save_data(self, path, data, **kwargs):
        pass

    @abstractmethod
    def get_path_info(self, path, name):
        pass

    @abstractmethod
    def make_cache(self, path, cache_path):
        pass

    def copy_inds(self, inds, n=2):
        if isinstance(inds, types.GeneratorType):
            inds = list(inds)
            return [(i for i in inds) for _ in range(n)]
        return [copy.deepcopy(inds) for _ in range(n)]

    def test_init(self, tmp_path):
        path = self.get_path_info(tmp_path)
        self.save_data(path, data.empty_ten)
        _ = self.reader(path)

    @pytest.mark.parametrize(
        "cache_name,precache,modify_file,params",
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
        cache_name,
        precache,
        modify_file,
        params,
    ):
        # path = tmp_path / "data.jsonl"
        path = self.get_path_info(tmp_path)
        if cache_name is not None:
            # cache_path = tmp_path / cache_name
            cache_path = self.get_path_info(tmp_path, cache_name)
        else:
            cache_path = None
        self.save_data(path, data.empty_ten)
        if precache:
            self.make_cache(path, cache_path=cache_path)
            # fj.cache.make_cache(path, cache_path=cache_path)
        if precache and modify_file == "time":
            self.save_data(path, data.empty_ten)
        if precache and modify_file == "content":
            self.save_data(path, data.various_ten)
        _ = self.reader(path, cache_path=cache_path, **params)

    @pytest.mark.parametrize(
        "instances",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_length(self, tmp_path, instances):
        # path = tmp_path / "data.jsonl"
        path = self.get_path_info(tmp_path)
        self.save_data(path, instances)
        reader = self.reader(path)
        n_files = self.n_files or 1
        assert len(reader) == len(instances) * n_files

    @pytest.mark.parametrize(
        "instances",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_getitem(self, tmp_path, instances):
        # path = tmp_path / "data.jsonl"
        path = self.get_path_info(tmp_path)
        self.save_data(path, instances)
        reader = self.reader(path)
        assert all([instances[i] == reader[i] for i in range(len(instances))])

    @pytest.mark.parametrize(
        "instances,inds",
        [
            (data.empty_ten, [0, 2, 4]),
            (data.various_ten, [0, 2, 4]),
            (data.various_ten, (i for i in range(3))),
            (data.various_ten, (i for i in range(2, 6, 2))),
        ],
    )
    def test_multi_index_getitem(self, tmp_path, instances, inds):
        path = self.get_path_info(tmp_path)
        self.save_data(path, instances)
        reader = self.reader(path)
        inds_0, inds_1 = self.copy_inds(inds, n=2)  # copy in case of generator
        assert [instances[idx] for idx in inds_0] == reader[inds_1]

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
        # path = tmp_path / "data.jsonl"
        path = self.get_path_info(tmp_path)
        self.save_data(path, instances)
        reader = self.reader(path)[start:stop:step]
        assert all([a == b for a, b in zip(instances[::step], reader)])

    @pytest.mark.parametrize(
        "instances",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_iterable(self, tmp_path, instances):
        # path = tmp_path / "data.jsonl"
        path = self.get_path_info(tmp_path)
        self.save_data(path, instances)
        reader = self.reader(path)
        assert all([a == b for a, b in zip(instances, reader)])

    @pytest.mark.parametrize(
        "instances",
        [data.empty_zero, data.empty_ten, data.various_ten],
    )
    def test_iter(self, tmp_path, instances):
        # path = tmp_path / "data.jsonl"
        path = self.get_path_info(tmp_path)
        self.save_data(path, instances)
        reader = iter(self.reader(path))
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
        # path = tmp_path / "data.jsonl"
        path = self.get_path_info(tmp_path)
        self.save_data(path, data.empty_ten)

        reader = self.reader(path)

        if params.get("check_cache_time"):
            time.sleep(0.01)

        self.save_data(path, data.various_ten, mode="w")
        if cache_name is not None:
            # cache_path = tmp_path / cache_name
            cache_path = self.get_path_info(tmp_path, cache_name)
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
            n_files = self.n_files or 1
            assert list(reader) == data.various_ten * n_files
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
        # path = tmp_path / "data.jsonl"
        path = self.get_path_info(tmp_path)
        self.save_data(path, data.empty_ten)

        reader = self.reader(path)

        self.save_data(path, data.various_ten, mode="w")

        if cache_name is not None:
            # cache_path = tmp_path / cache_name
            cache_path = self.get_path_info(tmp_path, cache_name)
        else:
            cache_path = None

        reader.force_recache(cache_path=cache_path)

        n_files = self.n_files or 1
        assert list(reader) == data.various_ten * n_files


class TestReader(BaseTests):
    reader = fj.Reader

    def get_path_info(self, path, name="data.jsonl"):
        return path / name

    def make_cache(self, path, cache_path):
        fj.cache.make_cache(path, cache_path=cache_path)

    def save_data(self, path, datum, **kwargs):
        data.save_data(path, datum, **kwargs)


class TestMultiReader(BaseTests):
    reader = fj.MultiReader
    n_files = 3

    def get_path_info(self, path, name=None):
        paths = list()
        if name is None:
            name = Path("data.jsonl")
        else:
            name = Path(name)
        for i in range(self.n_files):
            paths.append(path / f"{name.stem}_{i}{name.suffix}")
        return paths

    def save_data(self, path, datum, **kwargs):
        for subpath in path:
            data.save_data(subpath, datum, **kwargs)

    def make_cache(self, path, cache_path):
        for i in range(len(path)):
            subpath = path[i]
            cache_subpath = cache_path[i] if cache_path is not None else None
            fj.cache.make_cache(subpath, cache_path=cache_subpath)

    @pytest.mark.parametrize(
        "instances,params",
        list(itertools.product(
            [data.empty_zero, data.empty_ten, data.various_ten],
            [{}, {"force_cache": True}, {"check_cache_time": True}, {"check_cache_hash": True}],
        ))
    )
    def test_same_path(self, tmp_path, instances, params):
        n_files = self.n_files or 1
        path = [tmp_path / "data.jsonl"] * n_files
        data.save_data(path[0], instances)
        reader = self.reader(path, **params)
        _ = self.reader(path, **params)
        assert list(reader) == instances * n_files
