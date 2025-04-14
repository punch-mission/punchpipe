import contextlib
from multiprocessing.shared_memory import SharedMemory

from prefect import get_run_logger
from prefect.variables import Variable


def caching_is_enabled() -> bool:
    return Variable.get("use_shm_cache", False)


@contextlib.contextmanager
def try_read_from_key(key) -> memoryview | None:
    shm = None
    try:
        shm = SharedMemory(key, track=False)
        if shm.buf[0] == 1:
            yield shm.buf[1:]
        else:
            yield None
    except FileNotFoundError:
        yield None
    finally:
        pass
        # This triggers `BufferError: cannot close exported pointers exist`
        # if shm is not None:
        #     shm.close()


def try_write_to_key(key, data):
    shm = None
    try:
        shm = SharedMemory(key, create=True, size=len(data) + 1, track=False)
        # buf[0] will be a sentinel value to indicate that the rest of the data is in place, in case another process
        # opens this shared memory while we're still copying
        shm.buf[0] = 0
        shm.buf[1:] = data
        shm.buf[0] = 1
        get_run_logger().info(f"Saved to cache key {key}")
    except FileExistsError:
        pass
    finally:
        if shm is not None:
            shm.close()
