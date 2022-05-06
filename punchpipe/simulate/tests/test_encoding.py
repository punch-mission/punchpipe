import numpy as np
import pytest
from punchpipe.simulate.encoding import Encoding


def test_encoding():
    arr_dim = 2048
    arr = np.random.random([arr_dim, arr_dim]) * (2**16)

    encoded_arr = Encoding.encode(arr, frombits=16, tobits=10)

    assert encoded_arr.shape == arr.shape
    assert encoded_arr.max() <= 2**10


def test_decoding():
    arr_dim = 2048
    arr = np.random.random([arr_dim, arr_dim]) * (2 ** 10)

    decoded_arr = Encoding.decode(arr, frombits=10, tobits=16)

    assert decoded_arr.shape == arr.shape
    assert decoded_arr.max() <= 2**16
