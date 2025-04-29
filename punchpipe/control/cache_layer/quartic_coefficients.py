import os
import pickle
from collections.abc import Callable

from ndcube import NDCube
from punchbowl.data import load_ndcube_from_fits

from punchpipe.control.cache_layer import manager


class QuarticLoader:
    def __init__(self, path: str):
        self.path = path

    def gen_key(self) -> str:
        return f"quartic-{os.path.basename(self.path)}-{os.path.getmtime(self.path)}"

    def load(self) -> tuple[NDCube, str]:
        with manager.try_read_from_key(self.gen_key()) as buffer:
            if buffer is None:
                result = self.load_from_disk()
                self.try_caching(result)
            else:
                result = pickle.loads(buffer.data)
        return result, self.path

    def load_from_disk(self) -> NDCube:
        return load_ndcube_from_fits(self.path)

    def try_caching(self, cube: NDCube) -> None:
        data = pickle.dumps(cube)
        manager.try_write_to_key(self.gen_key(), data)

    def __repr__(self):
        return f"QuarticLoader({self.path})"

    def __str__(self):
        return self.__repr__()


def wrap_if_appropriate(quartic_path: str) -> str | Callable:
    if manager.caching_is_enabled():
        return QuarticLoader(quartic_path).load
    return quartic_path
