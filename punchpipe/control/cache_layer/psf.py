from collections.abc import Callable
import pickle
import os

from regularizepsf import ArrayPSFTransform

from punchpipe.control.cache_layer import manager

class PSFLoader:
    def __init__(self, path: str):
        self.path = path

    def gen_key(self) -> str:
        return f"cached-psf-{os.path.basename(self.path)}-{os.path.getmtime(self.path)}"

    def load(self) -> tuple[ArrayPSFTransform, str]:
        with manager.try_read_from_key(self.gen_key()) as buffer:
            if buffer is None:
                result = self.load_from_disk()
                self.try_caching(result)
            else:
                result = pickle.loads(buffer)
        return result, self.path

    def load_from_disk(self) -> ArrayPSFTransform:
        return ArrayPSFTransform.load(self.path)

    def try_caching(self, psf: ArrayPSFTransform) -> None:
        data = pickle.dumps(psf)
        manager.try_write_to_key(self.gen_key(), data)

    def __repr__(self):
        return f"PSFLoader({self.path})"

    def __str__(self):
        return self.__repr__()


def wrap_if_appropriate(psf_path: str) -> str | Callable:
    if manager.caching_is_enabled():
        return PSFLoader(psf_path).load
    return psf_path
