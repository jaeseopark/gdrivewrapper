import io
import os

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from httplib2 import Http
from oauth2client import file, client, tools


def get_service_object(scopes, creds_path, api_name="drive", api_version="v3"):
    """
    Creates a Service object
    :param scopes: scope of the service (ex. "https://www.googleapis.com/auth/drive.file")
    :param creds_path: local path to the credentials file
    :param api_name: name of the api (ex. "gdrive")
    :param api_version:  version of the api (ex. "v3")
    :return: A Service object
    """

    creds_parent = os.path.split(creds_path)[0]
    creds_filename = os.path.split(creds_path)[1]
    creds_basename = os.path.splitext(creds_filename)[0]

    store = file.Storage(f"{creds_parent}/{creds_basename}_store.json")
    creds = store.get()

    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(creds_path, scopes)
        creds = tools.run_flow(flow, store)

    return build(api_name, api_version, http=creds.authorize(Http()))


def upload(service, media, key=None, **kwargs):
    """
    Uploads the given data to google drive. This function can create a new file or update an existing file.
    :param service: Service object
    :param media: Data to upload
    :param key: (update-only) FileId of the file to update
    :param kwargs: keyword args
    :return:
    """
    if key:
        r = service.files().update(fileId=key, media_body=media).execute()
    else:
        r = service.files().create(body=kwargs, media_body=media).execute()

    return r


def download_bytes(service, key):
    """
    Downloads a file as bytearray
    :param service: Service ojbect
    :param key: FileId of the file to download
    :return: bytearray
    """
    request = service.files().get_media(fileId=key)
    with io.BytesIO() as bytesio:
        downloader = MediaIoBaseDownload(bytesio, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        return bytesio.getvalue()


def create_folder(service, name, parent_id=None):
    """
    Creates a folder and returns the FileId
    :param service: Service object
    :param name: name of the folder
    :param parent_id: (Optional) FileId of the containing folder
    :return: folder object
    """
    body = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder"
    }
    if parent_id:
        body["parents"] = [parent_id]
    return service.files().create(body=body).execute()


def create_comment(service, key, comment):
    """
    Posts a comment to an existing file
    :param service: Service object
    :param key: FileId of the file to post comment to
    :param comment: string
    :return: comment id
    """
    return service.comments().create(fileId=key, body={'content': comment}, fields="id").execute()


class GDriveWrapper:
    def __init__(self, scopes: str, creds_path: str):
        self.svc = get_service_object(scopes, creds_path)

    def upload(self, *args, **kwargs):
        return upload(self.svc, *args, **kwargs)

    def download_bytes(self, *args, **kwargs):
        return download_bytes(self.svc, *args, **kwargs)

    def create_folder(self, *args, **kwargs):
        return create_folder(self.svc, *args, **kwargs)

    def create_comment(self, *args, **kwargs):
        return create_comment(self.svc, *args, **kwargs)