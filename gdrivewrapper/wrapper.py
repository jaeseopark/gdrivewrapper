import io
import logging
import ssl
import time
from typing import Union, List
from typing.io import BinaryIO, IO

from googleapiclient.http import MediaIoBaseDownload

from gdrivewrapper.decorator.single import prevent_concurrent_calls
from gdrivewrapper.service import get_service_object

logging.getLogger("googleapiclient").setLevel(logging.FATAL)

DEFAULT_UPLOAD_RETRY_COUNT = 5


def _download(service, key, fp: Union[IO, BinaryIO], max_bytes_per_second: int = None):
    request = service.files().get_media(fileId=key)
    downloader = MediaIoBaseDownload(fp, request)

    done = False
    prev_time = time.perf_counter()
    prev_bytes = 0

    while not done:
        status, done = downloader.next_chunk()

        if max_bytes_per_second:
            current_time = time.perf_counter()
            bytes_since_last_checked = status.total_size - prev_bytes
            actual_speed = bytes_since_last_checked / (current_time - prev_time)

            excess_ratio = actual_speed / max_bytes_per_second - 1
            if excess_ratio > 0:
                time.sleep(excess_ratio * max_bytes_per_second)

            prev_time = current_time
            prev_bytes = status.total_size


class GDriveWrapper:
    def __init__(self, scopes: Union[str, List[str]], creds_path: str, allow_concurrent_calls=True):
        self.svc = get_service_object(scopes, creds_path)
        if not allow_concurrent_calls:
            prevent_concurrent_calls(self)

    def upload(self, media, key=None, folder_id=None, thumbnail=None, retry_count=DEFAULT_UPLOAD_RETRY_COUNT, **kwargs):
        """
        Uploads the given data to google drive. This function can create a new file or update an existing file.
        :param media: Data to upload
        :param key: (update-only) FileId of the file to update
        :param folder_id: (Optional) FileId of the containing folder
        :param thumbnail: (Optional) bytearray for the thumbnail image, b64-encoded.
        :param retry_count: number of times to retry upon common errors such as SSLError/BrokenPipeError
        :param kwargs: keyword args
        :return:
        """
        if folder_id:
            kwargs["parents"] = [folder_id]

        if thumbnail:
            content_hints = kwargs.get("contentHints", dict())
            content_hints.update({
                "thumbnail": {
                    "image": thumbnail,
                    "mimeType": "image/png"
                }
            })
            kwargs["contentHints"] = content_hints

        last_exception_msg = None
        for i in range(retry_count):
            try:
                if key:
                    return self.svc.files().update(fileId=key, body=kwargs, media_body=media).execute()
                else:
                    return self.svc.files().create(body=kwargs, media_body=media).execute()
            except (ssl.SSLError, BrokenPipeError) as e:
                last_exception_msg = str(e)
                time.sleep(1)
                continue

        # Stacktrace is lost at this point in time. The next best thing is to create a new exception
        raise RuntimeError(last_exception_msg)

    def download_bytes(self, key: str, max_bytes_per_second: int = None) -> bytes:
        """
        Downloads a file as bytearray
        :param key: FileId of the file to download
        :param max_bytes_per_second: the maximum speed the function can download the file at.
        :return: bytes
        """
        with io.BytesIO() as bytesio:
            _download(self.svc, key, fp=bytesio, max_bytes_per_second=max_bytes_per_second)
            return bytesio.getvalue()

    def download_file(self, key, local_path, max_bytes_per_second: int = None):
        """
        Downloads a file as bytearray
        :param key: FileId of the file to download
        :param local_path: Destination path in the local filesystem
        :param max_bytes_per_second: the maximum speed the function can download the file at.
        """
        with open(local_path, "wb") as fp:
            _download(self.svc, key, fp, max_bytes_per_second=max_bytes_per_second)

    def create_folder(self, name, folder_id=None, **kwargs):
        """
        Creates a folder and returns the FileId
        :param name: name of the folder
        :param folder_id: (Optional) FileId of the containing folder
        :return: folder object
        """
        kwargs["name"] = name
        kwargs["mimeType"] = "application/vnd.google-apps.folder"
        if folder_id:
            kwargs["parents"] = [folder_id]
        return self.svc.files().create(body=kwargs).execute()

    def create_comment(self, key, comment):
        """
        Posts a comment to an existing file
        :param key: FileId of the file to post comment to
        :param comment: string
        :return: comment id
        """
        return self.svc.comments().create(fileId=key, body={'content': comment}, fields="id").execute()
