import os
from collections.abc import Callable

from regularizepsf import ArrayPSFTransform

from punchpipe.control.cache_layer import manager
from punchpipe.control.cache_layer.loader_base_class import LoaderABC


class PSFLoader(LoaderABC[ArrayPSFTransform]):
    def __init__(self, path: str):
        self.path = path

    def gen_key(self) -> str:
        return f"psf-{os.path.basename(self.path)}-{os.path.getmtime(self.path)}"

    def src_repr(self) -> str:
        return self.path

    def load_from_disk(self) -> ArrayPSFTransform:
        return ArrayPSFTransform.load(self.path)

    def __repr__(self):
        return f"PSFLoader({self.path})"


def wrap_if_appropriate(psf_path: str) -> str | Callable:
    if manager.caching_is_enabled():
        return PSFLoader(psf_path).load
    return psf_path
