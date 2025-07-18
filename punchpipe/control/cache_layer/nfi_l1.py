import os

import numpy as np
from ndcube import NDCube
from punchbowl.data import load_ndcube_from_fits
from punchbowl.util import DataLoader
from punchbowl.levelq.pca import find_bodies_in_image

from punchpipe.control.cache_layer import manager
from punchpipe.control.cache_layer.loader_base_class import LoaderABC


class NFIL1Loader(LoaderABC):
    def __init__(self, path: str):
        self.path = path

    def load(self) -> tuple[float, float, list | np.ndarray]:
        with manager.try_read_from_key(self.gen_key()) as buffer:
            if buffer is None:
                cube = self.load_from_disk()
                bodies_in_quarter = find_bodies_in_image(cube)
                meta = cube.meta
                data = cube.data
                self.try_caching((data, meta, bodies_in_quarter))
            else:
                data, meta, bodies_in_quarter = self.from_bytes(buffer.data)
        return data, meta, bodies_in_quarter

    def gen_key(self) -> str:
        return f"nfi_l1-{os.path.basename(self.path)}-{os.path.getmtime(self.path)}"

    def src_repr(self) -> str:
        return self.path

    def load_from_disk(self) -> NDCube:
        cube = load_ndcube_from_fits(self.path, include_uncertainty=False, include_provenance=False)
        return cube

    def __repr__(self):
        return f"FitsFileLoader({self.path})"


def wrap_if_appropriate(file_path: str) -> str | DataLoader:
    if manager.caching_is_enabled():
        return NFIL1Loader(file_path)
    return file_path
