import glob
import io
import logging
import math
import os
import socket
import time
from collections import namedtuple
from pathlib import Path
from typing import Optional
from urllib.request import urlopen
from urllib.error import URLError

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.credentials import Credentials
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.service_resource import Bucket

from brus_backend_common.config import CONFIG

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

FileInfo = namedtuple("FileInfo", ["full_file", "file"])


def is_aws() -> bool:
    """Check if an instance is running on AWS, by way of getting a 200 response from the EC2 metadata service"""
    result = False
    meta = "http://169.254.169.254/latest/meta-data/"
    try:
        result = urlopen(meta, timeout=2).status == 200
    except (ConnectionError, TimeoutError, URLError, socket.timeout):
        return result
    return result


def get_storage_options() -> dict:
    """DeltaLake library doesn't use boto3 and doesn't pull the aws creds the same way."""
    storage_options = {
        "endpoint": f"http://{CONFIG.AWS_S3_ENDPOINT}" if CONFIG.IS_LOCAL else f"https://{CONFIG.AWS_S3_ENDPOINT}",
        "aws_sts_endpoint": (
            f"http://{CONFIG.AWS_STS_ENDPOINT}" if CONFIG.IS_LOCAL else f"https://{CONFIG.AWS_STS_ENDPOINT}"
        ),
        "region": CONFIG.AWS_REGION,
        "allow_http": "true",
        "aws_conditional_put": "etag",
    }
    aws_creds = get_aws_credentials()
    # these values should still be provided locally to prevent DeltaLake looking for metadata in EC2s
    storage_options.update(
        {
            "access_key_id": aws_creds.access_key if aws_creds else CONFIG.MINIO_ROOT_USER.get_secret_value(),
            "secret_access_key": aws_creds.secret_key if aws_creds else CONFIG.MINIO_ROOT_PASSWORD.get_secret_value(),
            "token": aws_creds.token if aws_creds and aws_creds.token is not None else "",
        }
    )
    return storage_options


def get_aws_credentials(
    access_key: str | None = None,
    secret_key: str | None = None,
    profile: str | None = None,
) -> Credentials | None:
    """Use boto3.Session(...) to derive credentials from any of given values or other credential providers that boto3
    uses

    For example, if no args are provided, but the AWS_PROFILE environment variable is populated, it will use the
    credentials from that profile.
    """
    if profile == "":
        profile = None

    aws_creds = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        profile_name=profile,
    ).get_credentials()
    return aws_creds


def get_prefixed_file_list(
    file_path: str | os.PathLike,
    prefix: str,
    local: bool = False,
    bucket_name: str | None = None,
    file_extension: str = "csv",
) -> list[FileInfo]:
    """Get a list of files starting with the given prefix

    Args:
        file_path: path to where files are stored
        prefix: prefix to filter which files to pull from AWS
        local: whether the path is local or remote
        bucket_name: name of the bucket from which to pull the files
        file_extension: the extension of the files to look for

    Returns:
        A list of tuples containing information about existing
    """
    if file_path is None:
        raise ValueError("file_path not provided")
    if local:
        logger.info("Loading local files")
        # get list of prefixed files in the specified local directory
        found_files = glob.glob(os.path.join(file_path, f"{prefix}*.{file_extension}"))
        file_list = [FileInfo(file_info, os.path.basename(file_info)) for file_info in found_files]
    else:
        logger.info("Loading Files")
        # get list of prefixed files in the config bucket on S3
        s3_client = _get_boto3("client", "s3")
        if not bucket_name:
            raise ValueError("bucket_name must be provided if non-local")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        file_list = []
        for obj in response.get("Contents", []):
            file_url = s3_client.generate_presigned_url(
                "get_object", {"Bucket": bucket_name, "Key": obj["Key"]}, ExpiresIn=600
            )
            file_list.append(FileInfo(file_url, obj["Key"]))
    return file_list


def _get_boto3(method_name: str, *args, region_name=CONFIG.AWS_REGION, **kwargs):
    """
    A wrapper for attributes of boto3 that creates a session to support Minio when running in a local dev
    environment. For non-local environments this will function similarly to a normal call to boto3.
    For example:
        - OLD: boto3.client('s3')  # This would require handling of the session for local development
        - NEW: _get_boto3("client", "s3")
    """
    attr = getattr(boto3, method_name)
    kwargs.update({"region_name": region_name})

    if callable(attr):
        if CONFIG.IS_LOCAL:
            session = boto3.Session(
                region_name=region_name,
                aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
                aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
            )
            attr = getattr(session, method_name)
            kwargs.update({"endpoint_url": f"http://{CONFIG.AWS_S3_ENDPOINT}"})
        else:
            kwargs.update({"endpoint_url": f"https://{CONFIG.AWS_S3_ENDPOINT}"})
        return attr(*args, **kwargs)
    return attr


def get_s3_bucket(bucket_name: str, region_name: str | None = CONFIG.AWS_REGION) -> Bucket:
    s3 = _get_boto3("resource", "s3", region_name=region_name)
    return s3.Bucket(bucket_name)


def retrieve_s3_bucket_object_list(bucket_name: str, key_prefix: Optional[str] = None):
    try:
        bucket = get_s3_bucket(bucket_name=bucket_name)
        bucket_objects = list(bucket.objects.filter(Prefix=key_prefix) if key_prefix else bucket.objects.all())
    except Exception as e:
        message = (
            f"Problem accessing S3 bucket '{bucket_name}' for deleted records.  Most likely the "
            f"AWS region or bucket name is configured incorrectly."
        )
        logger.exception(message)
        raise RuntimeError(message) from e
    return bucket_objects


def access_s3_object(bucket_name: str, obj) -> io.BytesIO:
    """Return the Bytes of an S3 object"""
    bucket = get_s3_bucket(bucket_name=bucket_name)
    data = io.BytesIO()
    bucket.download_fileobj(obj.key, data)
    data.seek(0)  # Like rewinding a VCR cassette
    return data


def multipart_upload(
    bucketname: str, regionname: str, source_path: str, keyname: str, sub_dir: Optional[os.PathLike] = None
):
    s3_client = _get_boto3("client", "s3", region_name=regionname)
    source_size = Path(source_path).stat().st_size
    # Sets the chunksize at minimum ~5MB to sqrt(5MB) * sqrt(source size)
    bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)), 5242880)
    config = TransferConfig(multipart_chunksize=bytes_per_chunk)
    # Type checkers don't like this line due to boto3 typing
    transfer = S3Transfer(s3_client, config=config)  # type: ignore
    file_name = Path(keyname).name
    if sub_dir is not None:
        file_name = f"{sub_dir}/{file_name}"
    transfer.upload_file(source_path, bucketname, file_name, extra_args={"ACL": "bucket-owner-full-control"})


def download_s3_object(
    bucket_name: str,
    key: str,
    file_path: str,
    s3_client: S3Client | None = None,
    retry_count: int = 3,
    retry_cooldown: int = 30,
    region_name: str = CONFIG.AWS_REGION,
) -> None:
    """Download an S3 object to a file.
    Args:
        bucket_name: The name of the bucket where the key is located.
        key: The name of the key to download from.
        file_path: The path to the file to download to.
        max_retries: The number of times to retry the download.
        retry_delay: The amount of time in seconds to wait after a failure before retrying.
        region_name: AWS region
    """
    if not s3_client:
        s3_client = _get_boto3("client", "s3", region_name=region_name)
    for attempt in range(retry_count + 1):
        try:
            logger.info(f"Retrieving file from S3. Bucket: {bucket_name} Key: {key}")
            s3_client.download_file(bucket_name, key, file_path)
            logger.info(f"Saving {key} to: {file_path}")
            return
        except ClientError as e:
            logger.info(
                f"Attempt {attempt + 1} of {retry_count + 1} failed to download {key} from bucket {bucket_name}. Error: {e}"
            )
            if attempt < retry_count:
                time.sleep(retry_cooldown)
            else:
                logger.error(f"Failed to download {key} from bucket {bucket_name} after {retry_count + 1} attempts.")
                raise e


def delete_s3_object(bucket_name: str, key: str, region_name: str = CONFIG.AWS_REGION) -> None:
    """Delete an S3 object
    Args:
        bucket_name: The name of the bucket where the key is located.
        key: The name of the key to delete
    """
    s3 = _get_boto3("client", "s3", region_name=region_name)
    s3.delete_object(Bucket=bucket_name, Key=key)


def delete_s3_objects(
    bucket_name: str,
    *,
    key_list: Optional[list[str]] = None,
    key_prefix: Optional[str] = None,
    region_name: Optional[str] = CONFIG.AWS_REGION,
) -> int:
    """Deletes all objects based on a list of keys
    Args:
        bucket_name: The name of the bucket where the objects are located
        key_list: A list of keys representing objects in the bucket to delete
        key_prefix: A prefix in the bucket used to generate a list of objects to delete
        region_name: AWS region to use; defaults to the settings provided region

    Returns:
        Number of objects delete
    """
    object_list = []

    if key_prefix:
        bucket = get_s3_bucket(bucket_name, region_name)
        objects = bucket.objects.filter(Prefix=key_prefix)
        object_list.extend([{"Key": obj.key} for obj in objects])

    if key_list:
        object_list.extend([{"Key": key} for key in key_list])

    s3_client = _get_boto3("client", "s3", region_name=region_name)
    resp = s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": object_list})

    return len(resp.get("Deleted", []))


def rename_s3_object(bucket_name: str, old_key: str, new_key: str, region_name: str = CONFIG.AWS_REGION) -> None:
    """Rename an existing S3 object by:
        1) Copying the file (old_key) to a new file with the new name (new_key)
        2) If the copy was successful, delete the old file (old_key)
    Args:
        bucket_name: The name of the bucket where the current object is located.
        old_key: The current name of the key to be renamed.
        new_key: The new name of the key.
        region_name: AWS region to use; defaults to the settings provided region.
    """

    s3 = _get_boto3("client", "s3", region_name=region_name)
    response = s3.copy_object(Bucket=bucket_name, CopySource=f"{bucket_name}/{old_key}", Key=new_key)

    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        s3.delete_object(Bucket=bucket_name, Key=old_key)
