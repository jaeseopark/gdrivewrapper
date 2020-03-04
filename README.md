# gdrivewrapper

A wrapper around Google Drive SDK. Covers basic operations like uploading a file or creating a folder.

## Usage

#### Initiate the wrapper class

```python
from gdrivewrapper import GDriveWrapper

api_scope = "https://www.googleapis.com/auth/drive.file"
creds_path = "./drive_v3_creds.json"

gdw = GDriveWrapper(api_scope, creds_path)
```

#### Upload a local file

```python
from googleapiclient.http import MediaFileUpload

local_path = "/tmp/resources/hello.txt"
media = MediaFileUpload(local_path)
gdw.upload(media)
```

#### Upload a string

```python
from googleapiclient.http import MediaInMemoryUpload

my_bytearray = "my string :)".encode('utf-8')
media = MediaInMemoryUpload(my_bytearray)
gdw.upload(media)
```

#### Upload with a filename

```python
media = ...
gdw.upload(media, name="mytextfile.txt")
```

#### Upload with a thumbnail

```python
import base64

media = ...

with open("image_path.png") as fp:
    image_bytes = fp.read() 

image_bytes_b64 = base64.urlsafe_b64encode(image_bytes).decode('utf-8')
gdw.upload(media, thumbnail=image_bytes_b64)
```

#### Upload to a folder

```python
response = gdw.create_folder("myfolder1")

media = ...
gdw.upload(media, folder_id=response["id"])
```

#### Add a comment to an existing file

```python
media = ...
response = gdw.upload(media)
gdw.create_comment(key=response["id"], comment="this file is great!")
```
