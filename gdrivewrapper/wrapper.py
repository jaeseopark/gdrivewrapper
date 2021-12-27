import base64
import io
import logging
import ssl
import time
from typing import Union, List
from typing.io import BinaryIO, IO

from commmons import with_retry
from googleapiclient.http import MediaIoBaseDownload, MediaUpload

from gdrivewrapper.service import get_service_object

logging.getLogger("googleapiclient").setLevel(logging.FATAL)

DEFAULT_UPLOAD_ATTEMPTS = 3


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


def _get_upload_body(name: str = None, folder_id: str = None, thumbnail: bytes = None) -> dict:
    body = dict()

    if name:
        body["name"] = name

    if folder_id:
        body["parents"] = [folder_id]

    if thumbnail:
        body["contentHints"] = {
            "thumbnail": {
                "image": base64.encodebytes(thumbnail).decode("utf-8"),
                "mimeType": "image/png"
            }
        }

    return body


class GDriveWrapper:
    def __init__(self, scopes: Union[str, List[str]], creds_path: str):
        self.svc = get_service_object(scopes, creds_path)

    def upload(self, media: MediaUpload, key: str = None, name: str = None, folder_id: str = None,
               thumbnail: bytes = None, max_upload_attempts=DEFAULT_UPLOAD_ATTEMPTS):
        """
        Uploads the given data to google drive. This function can create a new file or update an existing file.
        :param media: Data to upload
        :param key: (update-only) FileId of the file to update
        :param name: Display name of the file
        :param folder_id: (Optional) FileId of the containing folder
        :param thumbnail: (Optional) bytes for the thumbnail
        :param max_upload_attempts: Total number of attempts to perform a successful upload
        :return:
        """

        func = self.svc.files().create
        kwargs = {"body": _get_upload_body(name=name, folder_id=folder_id, thumbnail=thumbnail), "media_body": media}

        if key:
            func = self.svc.files().update
            kwargs.update({"fileId": key})

        func_with_retry = with_retry(func, max_upload_attempts, acceptable_exceptions=[ssl.SSLError, BrokenPipeError])
        return func_with_retry(**kwargs)

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

    def download_file(self, key: str, local_path: str, max_bytes_per_second: int = None):
        """
        Downloads a file as bytearray
        :param key: FileId of the file to download
        :param local_path: Destination path in the local filesystem
        :param max_bytes_per_second: the maximum speed the function can download the file at.
        """
        with open(local_path, "wb") as fp:
            _download(self.svc, key, fp, max_bytes_per_second=max_bytes_per_second)

    def create_folder(self, name: str, folder_id: str = None, **kwargs):
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

    def create_comment(self, key: str, comment: str):
        """
        Posts a comment to an existing file
        :param key: FileId of the file to post comment to
        :param comment: string
        :return: comment id
        """
        return self.svc.comments().create(fileId=key, body={'content': comment}, fields="id").execute()
