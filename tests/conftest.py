import os
import pytest
import contextlib


@contextlib.contextmanager
def temp_home(home_new):
    HOME = os.environ.get("HOME")
    USERPROFILE = os.environ.get("USERPROFILE")
    if HOME is not None:
        os.environ["HOME"] = home_new
    if USERPROFILE is not None:
        os.environ["ENVIRON"] = home_new
    try:
        yield
    finally:
        if HOME is not None:
            os.environ["HOME"] = HOME
        if USERPROFILE is not None:
            os.environ["USERPROFILE"] = USERPROFILE


@pytest.fixture(autouse=True)
def set_temp_home(tmp_path_factory, monkeypatch):
    path = tmp_path_factory.mktemp("fj_test_home").resolve().as_posix()
    with temp_home(path):
        yield
    return None
