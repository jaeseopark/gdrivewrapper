from functools import wraps
from multiprocessing import Lock
from typing import Callable

LOCK = Lock()


class MockLogger:
    def __init__(self):
        pass

    def debug(self, msg):
        pass


def single_instance(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = kwargs.pop("logger") if "logger" in kwargs else MockLogger()
        logger.debug("Acquiring gdw lock...")
        LOCK.acquire()
        logger.debug("Acquired gdw lock")
        try:
            return func(*args, **kwargs)
        finally:
            LOCK.release()
            logger.debug("Released gdw lock")

    return wrapper


def prevent_concurrent_calls(obj):
    def is_method(attr_name: str) -> bool:
        return callable(getattr(obj, attr_name))

    names = [name for name in dir(obj) if is_method(name)]
    for name in names:
        method = getattr(obj, name)
        setattr(obj, name, single_instance(method))
