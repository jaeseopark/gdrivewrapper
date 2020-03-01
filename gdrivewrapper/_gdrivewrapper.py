from multiprocessing import Lock

from gdrivewrapper.core import *

_lock = Lock()


def _lock_and_call(func, *args, **kwargs):
    class MockLogger:
        def __init__(self):
            pass

        def debug(self, msg):
            pass

    logger = kwargs.pop("logger") if "logger" in kwargs else MockLogger()
    logger.debug("Acquiring gdw lock...")
    _lock.acquire()
    logger.debug("Acquired gdw lock")
    try:
        return func(*args, **kwargs)
    finally:
        _lock.release()
        logger.debug("Released gdw lock")


class GDriveWrapper:
    def __init__(self, scopes: str, creds_path: str):
        self.svc = get_service_object(scopes, creds_path)

    def upload(self, *args, **kwargs):
        return _lock_and_call(upload, self.svc, *args, **kwargs)

    def download_bytes(self, *args, **kwargs):
        return _lock_and_call(download_bytes, self.svc, *args, **kwargs)

    def create_folder(self, *args, **kwargs):
        return _lock_and_call(create_folder, self.svc, *args, **kwargs)

    def create_comment(self, *args, **kwargs):
        return _lock_and_call(create_comment, self.svc, *args, **kwargs)
