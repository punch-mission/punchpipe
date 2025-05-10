import os
from collections.abc import Callable

from ndcube import NDCube
from punchbowl.data import load_ndcube_from_fits

from punchpipe.control.cache_layer import manager
from punchpipe.control.cache_layer.loader_base_class import LoaderABC


class QuarticLoader(LoaderABC[NDCube]):
    def __init__(self, path: str):
        self.path = path

    def gen_key(self) -> str:
        return f"quartic-{os.path.basename(self.path)}-{os.path.getmtime(self.path)}"

    def src_repr(self) -> str:
        return self.path

    def load_from_disk(self) -> NDCube:
        return load_ndcube_from_fits(self.path)

    def __repr__(self):
        return f"QuarticLoader({self.path})"


def wrap_if_appropriate(quartic_path: str) -> str | Callable:
    if manager.caching_is_enabled():
        return QuarticLoader(quartic_path).load
    return quartic_path
