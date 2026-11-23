"""Connection details for the local emulators started by the workflow.

Not part of any reporter's code. Imported by the scripts in this directory.
"""

import json
import urllib.error
import urllib.request

S3_HOST = "127.0.0.1:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_REGION = "us-east-1"

GCS_HOST = "127.0.0.1:9001"


def s3fs():
    """A pyarrow S3FileSystem pointed at the local MinIO."""
    from pyarrow import fs
    return fs.S3FileSystem(
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        endpoint_override=S3_HOST,
        scheme="http",
        region=S3_REGION,
    )


def gcsfs_arrow():
    """A pyarrow GcsFileSystem pointed at the local storage testbench."""
    from pyarrow.fs import GcsFileSystem
    return GcsFileSystem(endpoint_override=GCS_HOST, scheme="http", anonymous=True)


def gcs_create_bucket(name):
    """Create a bucket on the testbench over its JSON API (works for any pyarrow version)."""
    req = urllib.request.Request(
        f"http://{GCS_HOST}/storage/v1/b?project=test-project",
        data=json.dumps({"name": name}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req).read()
    except urllib.error.HTTPError as exc:
        if exc.code != 409:  # 409: already exists
            raise


def gcs_upload(bucket, name, data=b""):
    """Upload an object (zero bytes by default) over the testbench's JSON API."""
    from urllib.parse import quote
    req = urllib.request.Request(
        f"http://{GCS_HOST}/upload/storage/v1/b/{bucket}/o?uploadType=media&name={quote(name, safe='')}",
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        method="POST",
    )
    urllib.request.urlopen(req).read()
