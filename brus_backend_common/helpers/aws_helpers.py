import boto3
import glob
import logging
import os
import socket
from collections import namedtuple
from urllib.request import urlopen
from urllib.error import URLError

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


def is_aws():
    """Check if an instance is running on AWS, by way of getting a 200 response from the EC2 metadata service"""
    result = False
    meta = "http://169.254.169.254/latest/meta-data/"
    try:
        result = urlopen(meta, timeout=2).status == 200
    except (ConnectionError, TimeoutError, URLError, socket.timeout):
        return result
    return result


def get_aws_credentials(
    access_key: str = None,
    secret_key: str = None,
    profile: str = None,
) -> "botocore.credential.Credential":  # noqa, quoting to defer boto3+botocore import until totally needed
    """Use boto3.Session(...) to derive credentials from any of given values or other credential providers that boto3
    uses

    For example, if no args are provided, but the AWS_PROFILE environment variable is populated, it will use the
    credentials from that profile.
    """
    # Deferring import of external boto3 library until necessary.
    # Some configurations may run in a remote Spark cluster, and this reduces dependency on 3rd-party python
    # libraries, unless needed
    import boto3

    if profile == "":
        profile = None

    aws_creds = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        profile_name=profile,
    ).get_credentials()
    return aws_creds


def get_prefixed_file_list(
    file_path,
    prefix,
    local=False,
    aws_region="us-gov-west-1",
    bucket_name="sf_133_bucket",
    file_extension="csv",
):
    """Get a list of files starting with the given prefix

    Args:
        file_path: path to where files are stored
        prefix: prefix to filter which files to pull from AWS
        local: whether the path is local or remote
        aws_region: name of the region from which to pull the files
        bucket_name: name of the bucket from which to pull the files
        file_extension: the extension of the files to look for

    Returns:
        A list of tuples containing information about existing
    """
    FileInfo = namedtuple("FileInfo", ["full_file", "file"])
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
        s3_client = boto3.client("s3", region_name=aws_region)
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        file_list = []
        for obj in response.get("Contents", []):
            file_url = s3_client.generate_presigned_url(
                "get_object", {"Bucket": bucket_name, "Key": obj["Key"]}, ExpiresIn=600
            )
            file_list.append(FileInfo(file_url, obj["Key"]))
    return file_list
