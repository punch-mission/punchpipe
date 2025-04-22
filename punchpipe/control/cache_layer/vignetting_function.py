from collections.abc import Callable
from ndcube import NDCube
import pickle
import os

from punchbowl.data import load_ndcube_from_fits
from punchpipe.control.cache_layer import manager

class VignettingLoader:
    def __init__(self, path: str):
        self.path = path

    def gen_key(self) -> str:
        return f"vignetting-{os.path.basename(self.path)}-{os.path.getmtime(self.path)}"

    def load(self) -> tuple[NDCube, str]:
        with manager.try_read_from_key(self.gen_key()) as buffer:
            if buffer is None:
                result = self.load_from_disk()
                self.try_caching(result)
            else:
                result = pickle.loads(buffer.data)
        return result, self.path

    def load_from_disk(self) -> NDCube:
        return load_ndcube_from_fits(self.path, include_provenance=False)

    def try_caching(self, cube: NDCube) -> None:
        data = pickle.dumps(cube)
        manager.try_write_to_key(self.gen_key(), data)

    def __repr__(self):
        return f"VignettingLoader({self.path})"

    def __str__(self):
        return self.__repr__()


def wrap_if_appropriate(vignetting_path: str) -> str | Callable:
    if manager.caching_is_enabled():
        return VignettingLoader(vignetting_path).load
    return vignetting_path
