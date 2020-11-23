#!/usr/bin/env python3
# _*_ coding: utf-8 _*_
"""Load from and Save to a project S3 bucket

:author: Shay Hill
:created: 11/13/2020
"""

import hashlib
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Set, Union

import boto3

s3 = boto3.resource("s3")
s3client = boto3.client("s3")


def _recursive_listdir(directory: Path) -> Set[str]:
    """
    Recursively list files from directory.

    :param directory: start of search
    :return: relative path to every file in the directory
    """
    files = {x for x in directory.glob("**/*") if x.is_file()}
    return {str(x.relative_to(directory).as_posix()) for x in files}


def _create_s3_bucket(bucket_name: str) -> None:
    """
    Create a bucket on S3, skip if bucket exists

    :param bucket_name: name of new bucket
    """
    location = {"LocationConstraint": "us-east-2"}
    try:
        s3client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        print(f"creating {bucket_name} bucket.")
    except s3client.exceptions.BucketAlreadyOwnedByYou:
        print(f"checking for {bucket_name} bucket. bucket exists")


def _hash_local_file(path: str):
    """
    Hash file at path

    :param path: full path to a file
    :return: md5 hash of file

    This will create a hash that can be compared to the S3 object 'ETag' value.

    'ETag': '"da86e4696de39679cdc5c2c1fd8dd79c"'

    Only works with files under 5 gigabytes.
    """
    with open(path, "rb") as file:
        buf = file.read()
    return hashlib.md5(buf).hexdigest()


def _list_s3_bucket_items(bucket_name: str) -> List[Dict[str, Any]]:
    """
    List all files in an S3 bucket

    :param bucket_name: bucket name on S3 account
    :return: a list of "objects" (dictionaries) in bucket

    Returned object (dictionary from S3) example:
    {'Key': 'black_widow.png',
     'LastModified': datetime.datetime(2020, 11, 13, 14, 46, 32, tzinfo=tzutc()),
     'ETag': '"da86e4696de39679cdc5c2c1fd8dd79c"',
     'Size': 1775690,
     'StorageClass': 'STANDARD'}
    """
    return s3client.list_objects_v2(Bucket=bucket_name).get("Contents", [])


def _upload_file_to_s3(bucket_name: str, local_dir: Path, file: str) -> None:
    """
    Upload a file to S3

    :param bucket_name: bucket name on s3
    :param local_dir: local directory where file exists
    :param file: file (relative path from local_dir)
    """
    data = open(local_dir / file, "rb")
    s3.Bucket(bucket_name).put_object(Key=file, Body=data)
    print(f"uploaded {file} to {bucket_name}")


def _compare_remote_to_local(bucket_name: str, local_dir: Path) -> Dict[str, Set[str]]:
    """
    Compare S3 bucket contents to a local folder contents.

    :param bucket_name: S3 bucket name
    :param local_dir: full path to a local directory
    :return: a dictionary of file states:
        * 'remote only': files present only in the S3 bucket
        * 'local only': files present only in the local directory
        * 'hash different': name on remote and local, hash different
        * 'hash same': name on remote and local, hash same
    """
    state2names = {}
    name2remote = {x["Key"]: x for x in _list_s3_bucket_items(bucket_name)}
    local = _recursive_listdir(local_dir)

    state2names["remote only"] = set(name2remote) - local
    state2names["local only"] = local - set(name2remote)
    state2names["hash different"] = set()
    state2names["hash same"] = set()

    for name in local & set(name2remote):
        local_hash = _hash_local_file(local_dir / name)
        remote_hash = name2remote[name]["ETag"][1:-1]
        if local_hash == remote_hash:
            state2names["hash same"].add(name)
        else:
            state2names["hash different"].add(name)
    return state2names


def _get_next_revision(filename: Union[Path, str], prefix: str) -> str:
    """
    Add or update a numbered prefix to a filename.

    :param filename: filename with or without a [prefix\\d+] prefix
    :param prefix: string to identify revision source
    :return:

        # create prefix
        >>> str(_get_next_revision('filename.png', 'loc'))
        str(Path('[loc0]filename.png'))

        # update prefix
        >>> str(_get_next_revision('[rem0]filename.png', 'rem'))
        str(Path('[rem1]filename.png'))

        # create with path
        >>> str(_get_next_revision(Path('folder/filename.png'), 'loc'))
        str(Path('folder/[loc0]filename.png'))

        # update with path
        >>> str(_get_next_revision(Path('folder/[loc99]filename.png'), 'loc'))
        str(Path('folder/[loc100]filename.png'))

    """
    filename = Path(filename)
    pattern = rf"\[{prefix}(?P<rev>\d+)\](?P<name>.+)"
    rev_number = re.match(pattern, filename.name)
    if rev_number:
        rev = int(rev_number["rev"]) + 1
        name = rev_number["name"]
    else:
        rev = 0
        name = filename.name
    return filename.parent / f"[{prefix}{rev}]{name}"


def _push_s3_bucket(bucket_name: str, local_dir: str, safe: bool = True) -> None:
    """
    Overwrite S3 bucket with contents of local_dir.

    :param bucket_name: bucket name on S3 account
    :param local_dir: full path to directory on machine
    """
    state2names = _compare_remote_to_local(bucket_name, local_dir)

    for name in state2names["hash different"] | state2names["remote only"]:
        if safe:
            old_path = bucket_name + "/" + name
            new_path = str(_get_next_revision(name, "rem").as_posix())
            s3.Object(bucket_name, str(new_path)).copy_from(CopySource=str(old_path))
        s3.Object(bucket_name, name).delete()

    for name in state2names["local only"] | state2names["hash different"]:
        _upload_file_to_s3(bucket_name, local_dir, name)

    print(f"pushed content from {local_dir} to S3 {bucket_name}")


def _pull_s3_bucket(bucket_name: str, local_dir: Path, safe: bool = True) -> None:
    """
    Overwrite contents of local_dir with S3 bucket.

    :param bucket_name: bucket name on S3 account
    :param local_dir: full path to directory on machine
    """
    state2names = _compare_remote_to_local(bucket_name, local_dir)

    for name in state2names["hash different"] | state2names["local only"]:
        old_path = local_dir / name
        if safe:
            new_path = local_dir / _get_next_revision(name, "loc")
            shutil.copy2(old_path, new_path)
        old_path.unlink()

    for name in state2names["remote only"] | state2names["hash different"]:
        local_filename = Path(local_dir) / name
        local_filename.parent.mkdir(parents=True, exist_ok=True)
        s3client.download_file(bucket_name, name, str(local_filename))

    print(f"pulled content from S3 {bucket_name} to {local_dir}")


class RemoteBucket:
    """An S3 bucket synced to a local file."""

    def __init__(self, bucket_name: str, local_dir: Union[Path, str]) -> None:
        self.bucket_name = bucket_name
        self.local_dir = Path(local_dir)
        _create_s3_bucket(bucket_name)
        self.local_dir.mkdir(parents=True, exist_ok=True)

    def push(self, safe: bool = True) -> None:
        """Push all files from the local folder to the S3 bucket."""
        _push_s3_bucket(self.bucket_name, self.local_dir, safe)

    def pull(self, safe: bool = True) -> None:
        """Pull all files from the S3 bucket to the local folder."""
        _pull_s3_bucket(self.bucket_name, self.local_dir, safe)

    @property
    def local_filenames(self) -> Set[str]:
        """Get relative paths from self.local_dir"""
        return _recursive_listdir(self.local_dir)

    @property
    def remote_items(self) -> Set[Dict[str, Any]]:
        """Item dictionaries in bucket"""
        return set(_list_s3_bucket_items(self.bucket_name))

    @property
    def remote_filenames(self) -> Set[str]:
        """Get relative paths from bucket"""
        return {x["Key"] for x in self.remote_items}


if __name__ == "__main__":
    import doctest

    doctest.testmod()
