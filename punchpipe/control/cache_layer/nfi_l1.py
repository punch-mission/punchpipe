import os
from collections.abc import Callable

from ndcube import NDCube
import numpy as np
from punchbowl.data import load_ndcube_from_fits

from punchpipe.control.cache_layer import manager
from punchpipe.control.cache_layer.loader_base_class import LoaderABC


class NFIL1Loader(LoaderABC[NDCube]):
    def __init__(self, path: str):
        self.path = path

    def gen_key(self) -> str:
        return f"fits-{os.path.basename(self.path)}-{os.path.getmtime(self.path)}"

    def src_repr(self) -> str:
        return self.path

    def load_from_disk(self) -> NDCube:
        return load_ndcube_from_fits(self.path, include_uncertainty=False, include_provenance=False).data

    def to_bytes(self, object: np.ndarray) -> bytes:
        return object.tobytes()

    def from_bytes(self, bytes: bytes) -> np.ndarray:
        return np.frombuffer(bytes, dtype=np.float64).reshape((2048, 2048))

    def __repr__(self):
        return f"FitsFileLoader({self.path})"


def wrap_if_appropriate(file_path: str) -> str | Callable:
    if manager.caching_is_enabled():
        return NFIL1Loader(file_path).load
    return file_path
