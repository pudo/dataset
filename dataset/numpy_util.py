

def npy_load_placeholder(fname):
    pass


def npy_save_placeholder(fname):
    pass


try:
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
