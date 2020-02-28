from multiprocessing import Lock

from gdrivewrapper._core import *

_lock = Lock()


def lock_and_call(func, *args, **kwargs):
    _lock.acquire()
    try:
        return func(*args, **kwargs)
    finally:
        _lock.release()


class GDriveWrapper:
    def __init__(self, scopes: str, creds_path: str):
        self.svc = get_service_object(scopes, creds_path)

    def upload(self, *args, **kwargs):
        return lock_and_call(upload, self.svc, *args, **kwargs)

    def download_bytes(self, *args, **kwargs):
        return lock_and_call(download_bytes, self.svc, *args, **kwargs)

    def create_folder(self, *args, **kwargs):
        return lock_and_call(create_folder, self.svc, *args, **kwargs)

    def create_comment(self, *args, **kwargs):
        return lock_and_call(create_comment, self.svc, *args, **kwargs)
