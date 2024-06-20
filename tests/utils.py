import contextlib
import fast_jsonl as fj


@contextlib.contextmanager
def modify_dir_method(method_new):
    method_original = fj.constants.DIR_METHOD
    fj.constants.DIR_METHOD = method_new
    try:
        yield
    except:
        fj.constants.DIR_METHOD = method_original
