import io
import logging
import os
import ssl
import time

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from httplib2 import Http
from oauth2client import file, client, tools

logging.getLogger("googleapiclient").setLevel(logging.FATAL)


def get_service_object(scopes, creds_path, api_name="drive", api_version="v3"):
    """
    Creates a Service object
    :param scopes: scope of the service (ex. "https://www.googleapis.com/auth/drive.file")
    :param creds_path: local path to the credentials file
    :param api_name: name of the api (ex. "gdrive")
    :param api_version:  version of the api (ex. "v3")
    :return: A Service object
    """

    assert os.path.exists(creds_path)

    creds_parent = os.path.split(creds_path)[0]
    creds_filename = os.path.split(creds_path)[1]
    creds_basename = os.path.splitext(creds_filename)[0]

    token_path = os.path.join(creds_parent, f"{creds_basename}_store.json")
    store = file.Storage(token_path)
    creds = store.get()

    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(creds_path, scopes)
        creds = tools.run_flow(flow, store)

    return build(api_name, api_version, http=creds.authorize(Http()))


def upload(service, media, key=None, folder_id=None, thumbnail=None, retry_count=5, **kwargs):
    """
    Uploads the given data to google drive. This function can create a new file or update an existing file.
    :param service: Service object
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
                return service.files().update(fileId=key, body=kwargs, media_body=media).execute()
            else:
                return service.files().create(body=kwargs, media_body=media).execute()
        except (ssl.SSLError, BrokenPipeError) as e:
            last_exception_msg = str(e)
            time.sleep(1)
            continue

    # Stacktrace is lost at this point in time. The next best thing is to create a new exception
    raise RuntimeError(last_exception_msg)


def download_bytes(service, key):
    """
    Downloads a file as bytearray
    :param service: Service ojbect
    :param key: FileId of the file to download
    :return: bytearray
    """
    with io.BytesIO() as bytesio:
        _download(service, key, bytesio)
        return bytesio.getvalue()


def download_file(service, key, local_path):
    """
    Downloads a file as bytearray
    :param service: Service ojbect
    :param key: FileId of the file to download
    :param local_path: Destination path in the local filesystem
    """
    with open(local_path, "wb") as fp:
        _download(service, key, fp)


def _download(service, key, fp):
    request = service.files().get_media(fileId=key)
    downloader = MediaIoBaseDownload(fp, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()


def create_folder(service, name, folder_id=None, **kwargs):
    """
    Creates a folder and returns the FileId
    :param service: Service object
    :param name: name of the folder
    :param folder_id: (Optional) FileId of the containing folder
    :return: folder object
    """
    kwargs["name"] = name
    kwargs["mimeType"] = "application/vnd.google-apps.folder"
    if folder_id:
        kwargs["parents"] = [folder_id]
    return service.files().create(body=kwargs).execute()


def create_comment(service, key, comment):
    """
    Posts a comment to an existing file
    :param service: Service object
    :param key: FileId of the file to post comment to
    :param comment: string
    :return: comment id
    """
    return service.comments().create(fileId=key, body={'content': comment}, fields="id").execute()
