import abc
import pickle
from typing import Generic, TypeVar

from punchpipe.control.cache_layer import manager

T = TypeVar("T")


class LoaderABC(abc.ABC, Generic[T]):
    @abc.abstractmethod
    def gen_key(self) -> str:
        """Generate a cache key"""

    @abc.abstractmethod
    def src_repr(self) -> str:
        """Return a string representation of the source data (e.g. a file path)"""

    @abc.abstractmethod
    def load_from_disk(self) -> T:
        """Load the object"""

    @abc.abstractmethod
    def __repr__(self):
        """Return a string representation of this loader"""

    def load(self) -> tuple[T, str]:
        with manager.try_read_from_key(self.gen_key()) as buffer:
            if buffer is None:
                result = self.load_from_disk()
                self.try_caching(result)
            else:
                result = self.from_bytes(buffer.data)
        return result, self.src_repr()

    def try_caching(self, object: T) -> None:
        data = self.to_bytes(object)
        manager.try_write_to_key(self.gen_key(), data)

    def to_bytes(self, object: T) -> bytes:
        return pickle.dumps(object)

    def from_bytes(self, bytes: bytes) -> T:
        return pickle.loads(bytes)

    def __str__(self):
        return self.__repr__()
