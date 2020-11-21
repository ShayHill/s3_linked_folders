#!/usr/bin/env python3
# _*_ coding: utf-8 _*_
"""Load from and Save to a project S3 bucket

:author: Shay Hill
:created: 11/13/2020
"""

import hashlib
import os
import re
import shutil
import warnings
from typing import Any, Dict, List, Set

import boto3

s3 = boto3.resource("s3")
s3client = boto3.client("s3")


def _create_s3_bucket(bucket_name: str) -> None:
    """
    Create a bucket on S3

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


def _upload_file_to_s3(bucket_name: str, complete_file_path: str):
    """
    Upload a file to S3

    :param complete_file_path:
    :return:
    """
    data = open(os.path.normpath(complete_file_path), "rb")
    file_basename = os.path.basename(complete_file_path)
    s3.Bucket(bucket_name).put_object(Key=file_basename, Body=data)
    print(f"uploaded {file_basename} to {bucket_name}")


def _compare_remote_to_local(bucket_name: str, local_dir: str) -> Dict[str, Set[str]]:
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
    local = set(os.listdir(local_dir))
    state2names["remote only"] = set(name2remote) - local
    state2names["local only"] = local - set(name2remote)
    state2names["hash different"] = set()
    state2names["hash same"] = set()
    for name in local & set(name2remote):
        local_hash = _hash_local_file(os.path.join(local_dir, name))
        remote_hash = name2remote[name]["ETag"][1:-1]
        if local_hash == remote_hash:
            state2names["hash same"].add(name)
        else:
            state2names["hash different"].add(name)
    return state2names


def _get_next_revision(filename: str, prefix: str) -> str:
    """
    Add or update a numbered prefix to a filename.

    :param filename: filename with or without a [prefix\\d+] prefix
    :param prefix: string to identify revision source
    :return:

        >>> _get_next_revision('filename.png', 'pre')
        '[rev0]filename.png'

        >>> _get_next_revision('[rev0]filename.png', 'pre')
        '[rev1]filename.png'
    """
    pattern = rf"\[{prefix}(?P<rev>\d+)\](?P<name>.+)"
    rev_number = re.match(pattern, filename)
    if rev_number:
        rev = int(rev_number["rev"]) + 1
        name = rev_number["name"]
    else:
        rev = 0
        name = filename
    return f"[{prefix}{rev}]{name}"


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
            new_name = _get_next_revision(name, "rem")
            s3.Object(bucket_name, new_name).copy_from(CopySource=old_path)
        s3.Object(bucket_name, name).delete()

    for name in state2names["local only"] | state2names["hash different"]:
        path = os.path.join(local_dir, name)
        _upload_file_to_s3(bucket_name, path)

    # if do_delete_unmatched:
    #
    #     def remove(name_):
    #         s3.Object(bucket_name, name_).delete()
    #
    # else:
    #
    #     def remove(name_):
    #         warnings.warn(
    #             f"{name_} exists in bucket {bucket_name} "
    #             f"but not in local folder {local_dir}"
    #         )
    #
    # for name in state2names["remote only"]:
    #     remove(name)
    print(f"pushed content from {local_dir} to S3 {bucket_name}")


def _pull_s3_bucket(bucket_name: str, local_dir: str, safe: bool = True) -> None:
    """
    Overwrite contents of local_dir with S3 bucket.

    :param bucket_name: bucket name on S3 account
    :param local_dir: full path to directory on machine
    """
    state2names = _compare_remote_to_local(bucket_name, local_dir)

    for name in state2names["hash different"] | state2names["local only"]:
        old_path = os.path.join(local_dir, name)
        if safe:
            new_path = os.path.join(local_dir, _get_next_revision(name, "loc"))
            shutil.copy2(old_path, new_path)
        os.remove(old_path)

    for name in state2names["remote only"] | state2names["hash different"]:
        path = os.path.join(local_dir, name)
        s3client.download_file(bucket_name, name, path)

    # if do_delete_unmatched:
    #
    #     def remove(name_):
    #         os.remove(os.path.join(local_dir, name_))
    #
    # else:
    #
    #     def remove(name_):
    #         warnings.warn(
    #             f"{name_} exists in local folder {local_dir} "
    #             f"but not in s3 {bucket_name}"
    #         )
    #
    # for name in state2names["local only"]:
    #     remove(name)
    print(f"pulled content from S3 {bucket_name} to {local_dir}")


class RemoteBucket:
    """An S3 bucket synced to a local file."""

    def __init__(self, bucket_name: str, local_dir: str) -> None:
        self.bucket_name = bucket_name
        self.local_dir = local_dir
        if not os.path.exists(local_dir):
            os.mkdir(local_dir)
        _create_s3_bucket(bucket_name)

    def push(self, safe: bool = True) -> None:
        """Push all files from the local folder to the S3 bucket."""
        _push_s3_bucket(self.bucket_name, self.local_dir, safe)

    def pull(self, safe: bool = True) -> None:
        """Pull all files from the S3 bucket to the local folder."""
        _pull_s3_bucket(self.bucket_name, self.local_dir, safe)
