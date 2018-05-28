import os
class NumpyArrayPlaceholder(object):
    pass


def npy_load_placeholder(fname):
    pass

def npy_save_placeholder(fname):
    pass

try:
    if os.environ["DEACTIVATE_NUMPY"] == "1":
        raise ImportError("Numpy is expclitly deactivated!")
    from numpy import load, save, ndarray
    npy_load = load
    npy_save = save
    npy_array = ndarray
    has_numpy = True
except ImportError, KeyError:
    npy_save = npy_save_placeholder
    npy_load = npy_load_placeholder
    npy_array = NumpyArrayPlaceholder
    has_numpy = False
