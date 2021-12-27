import logging
import os
from typing import Union, List

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

logging.getLogger("googleapiclient").setLevel(logging.FATAL)

DEFAULT_UPLOAD_RETRY_COUNT = 5


def get_service_object(scopes: Union[str, List[str]], creds_path: str, api_name="drive", api_version="v3"):
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

    token_path = os.path.join(creds_parent, f"{creds_basename}_store.json")
    store = file.Storage(token_path)
    creds = store.get()

    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(creds_path, scopes)
        creds = tools.run_flow(flow, store)

    return build(api_name, api_version, http=creds.authorize(Http()))
