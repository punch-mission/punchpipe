import os
import struct
from collections.abc import Callable

import numpy as np
from ndcube import NDCube
from punchbowl.data import load_ndcube_from_fits
from punchbowl.levelq.pca import find_bodies_in_image

from punchpipe.control.cache_layer import manager
from punchpipe.control.cache_layer.loader_base_class import LoaderABC


class NFIL1Loader(LoaderABC):
    def __init__(self, path: str):
        self.path = path

    def load(self, into: np.ndarray) -> tuple[float, float, list | np.ndarray]:
        with manager.try_read_from_key(self.gen_key()) as buffer:
            if buffer is None:
                cube = self.load_from_disk()
                bodies_in_quarter = find_bodies_in_image(cube)
                mean = cube.meta['DATAAVG'].value
                median = cube.meta['DATAMDN'].value
                data = cube.data
                self.try_caching((mean, median, data, bodies_in_quarter))
            else:
                mean, median, data, bodies_in_quarter = self.from_bytes(buffer.data)
            into[:] = data
            del data
        return mean, median, bodies_in_quarter

    def gen_key(self) -> str:
        return f"nfi_l1-{os.path.basename(self.path)}-{os.path.getmtime(self.path)}"

    def src_repr(self) -> str:
        return self.path

    def load_from_disk(self) -> NDCube:
        cube = load_ndcube_from_fits(self.path, include_uncertainty=False, include_provenance=False)
        return cube

    def to_bytes(self, data: tuple) -> bytes:
        mean, median, data, bodies_in_quarter = data
        mean = struct.pack('f', mean)
        median = struct.pack('f', median)
        bodies = np.packbits(np.array(bodies_in_quarter)).tobytes()
        data_array = data.tobytes()
        return mean + median + bodies + data_array

    def from_bytes(self, bytes: bytes) -> tuple[float, float, np.ndarray, np.ndarray]:
        mean = struct.unpack('f', bytes[0:4])[0]
        median = struct.unpack('f', bytes[4:8])[0]
        packed_bodies = np.frombuffer(bytes[8:11], dtype=np.uint8)
        bodies_in_quarter = np.unpackbits(packed_bodies, count=6*4).reshape((6, 4))
        return mean, median, np.frombuffer(bytes[11:], dtype=np.float64).reshape((2048, 2048)), bodies_in_quarter

    def __repr__(self):
        return f"FitsFileLoader({self.path})"


def wrap_if_appropriate(file_path: str) -> str | Callable:
    if manager.caching_is_enabled():
        return NFIL1Loader(file_path).load
    return file_path
