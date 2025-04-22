import abc
import pickle

from punchpipe.control.cache_layer import manager

class LoaderABC[T](abc.ABC):
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
                result = pickle.loads(buffer.data)
        return result, self.src_repr()

    def try_caching(self, object: T) -> None:
        data = pickle.dumps(object)
        manager.try_write_to_key(self.gen_key(), data)

    def __str__(self):
        return self.__repr__()
