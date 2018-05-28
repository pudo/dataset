class NumpyArrayPlaceholder(object):
    pass


def npy_load_placeholder(fname):
    pass

def npy_save_placeholder(fname):
    pass

try:
    from numpy import load, save, ndarray
    npy_load = load
    npy_save = save
    npy_array = ndarray
except ImportError:
    npy_save = npy_save_placeholder
    npy_load = npy_load_placeholder
    npy_array = NumpyArrayPlaceholder
