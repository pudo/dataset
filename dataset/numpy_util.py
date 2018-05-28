import os


def npy_load_placeholder(fname):
    pass


def npy_save_placeholder(fname):
    pass


try:
    if "DEACTIVATE_NUMPY" in os.environ.keys():
        if os.environ["DEACTIVATE_NUMPY"] == "1":
            raise ImportError("Numpy is expclitly deactivated!")
    from numpy import load, save, ndarray
    npy_load = load
    npy_save = save
    npy_array = ndarray
    has_numpy = True
except ImportError:
    npy_save = npy_save_placeholder
    npy_load = npy_load_placeholder
    has_numpy = False


def is_numpy_array(obj):
    if has_numpy:
        return isinstance(obj, ndarray)
    return False
