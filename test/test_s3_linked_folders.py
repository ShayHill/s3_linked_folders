#!/usr/bin/env python3
# _*_ coding: utf-8 _*_
"""Test all functions in s3_linked_folders

:author: Shay Hill
:created: 11/20/2020
"""

# raise RuntimeError('read the README then comment out this line to run the tests.')

import os
from contextlib import suppress
from pathlib import Path
from typing import Iterable

import pytest

from ..s3_linked_folders import (
    RemoteBucket,
    _compare_remote_to_local,
    _get_next_revision,
    _hash_local_file,
    _recursive_listdir,
    s3,
    s3client,
)

TEMP_LOCAL_DIR = Path(__file__, "..", "temp_test_dir").resolve()
TEMP_S3_BUCKET = "temp-test-linked"


def _create_local_files(files: Iterable[str]) -> None:
    """
    Create text files in the TEMP_LOCAL_DIR

    :param files: filenames
    :effects: write files to local filesystem
    """
    for filename in files:
        subfolders, _ = os.path.split(filename)
        with suppress(FileExistsError):
            os.makedirs(TEMP_LOCAL_DIR / subfolders)
        with open(TEMP_LOCAL_DIR / filename, "w") as file:
            file.write(filename)


def _alter_local_files(files: Iterable[str]) -> None:
    """
    Append extra text to local files.

    :param files: names of files in TEMP_LOCAL_DIR
    :effects: alters files on local filesystem

    This will alter the hash of a file.
    """
    for filename in files:
        with open(TEMP_LOCAL_DIR / filename, "a") as file:
            file.write(filename)


def _delete_temp_bucket() -> None:
    """
    Delete s3 bucket

    S3 buckets appear to have subfolders, but they're actually flat. Delete files
    inside, then delete bucket.
    """
    for obj in s3client.list_objects(Bucket=TEMP_S3_BUCKET).get("Contents", []):
        s3client.delete_object(Bucket=TEMP_S3_BUCKET, Key=obj["Key"])
    s3client.delete_bucket(Bucket=TEMP_S3_BUCKET)


def _delete_temp_local_dir(directory=TEMP_LOCAL_DIR) -> None:
    """
    Recursively delete files then empty directories

    :param directory: path to directory or file
    :effects: removes TEMP_LOCAL_DIR
    """
    if os.path.isfile(directory):
        os.remove(directory)
        return
    for file in os.listdir(directory):
        _delete_temp_local_dir(directory / file)
    os.rmdir(directory)


def _get_remote_filenames():
    """Set of remote filenames"""
    remote_files = set()
    for obj in s3client.list_objects(Bucket=TEMP_S3_BUCKET).get("Contents", []):
        remote_files.add(obj["Key"])
    return remote_files


def _get_local_filenames():
    """Set of local filenames"""
    return _recursive_listdir(TEMP_LOCAL_DIR)


@pytest.fixture(scope="function")
def matching_state() -> RemoteBucket:
    """
    Link a remote bucket and local folder, put four files in each.
    :yield: the linked RemoteBucket object
    """
    with suppress(Exception):
        _delete_temp_bucket()
    with suppress(Exception):
        _delete_temp_local_dir()
    linked = RemoteBucket(TEMP_S3_BUCKET, TEMP_LOCAL_DIR)
    files = {"hash_same", "hash_different", "remote_only", "local_only"}
    _create_local_files(files)
    linked.push()
    assert _get_local_filenames() == _get_remote_filenames() == files
    for name in _get_local_filenames():
        local_hash = _hash_local_file(TEMP_LOCAL_DIR / name)
        remote_hash = s3.Object(TEMP_S3_BUCKET, name).get()["ETag"][1:-1]
        assert local_hash == remote_hash
    yield linked
    _delete_temp_bucket()
    _delete_temp_local_dir()


@pytest.fixture(scope="function")
def unmatched_state(matching_state) -> RemoteBucket:
    _alter_local_files({"hash_different"})
    os.remove(TEMP_LOCAL_DIR / "remote_only")
    s3.Object(TEMP_S3_BUCKET, "local_only").delete()
    assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
        "hash same": {"hash_same"},
        "hash different": {"hash_different"},
        "remote only": {"remote_only"},
        "local only": {"local_only"},
    }
    return matching_state


class TestGetNextRevision:
    def test_new_rev(self) -> None:
        """Add rev prefix if none exists"""
        assert _get_next_revision("filename.ext", "rev") == "[rev0]filename.ext"

    def test_update_rev(self) -> None:
        """Update rev number is previous rev found"""
        assert _get_next_revision("[rev9]filename.ext", "rev") == "[rev10]filename.ext"

    def test_subfolder_rev(self) -> None:
        """Update rev of file, not subfolder"""
        assert (
            _get_next_revision("subfolder/[rev9]filename.ext", "rev")
            == "subfolder/[rev10]filename.ext"
        )


class TestRemoteBucket:
    def test_push_safe(self, unmatched_state) -> None:
        """Rename conflicts on remote

        file.one -> file on both. names and hatch match -> no change
        file.two -> file on both. names match hatch not -> keep both. rename remote
        file.thr -> file on remote only                 -> move local to remote
        file.fou -> file on local only                  -> keep and rename
        """
        unmatched_state.push()
        assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
            "hash same": {"hash_same", "hash_different", "local_only"},
            "hash different": set(),
            "remote only": {"[rem0]hash_different", "[rem0]remote_only"},
            "local only": set(),
        }

    def test_push_unsafe(self, unmatched_state) -> None:
        """Discard conflicts on local

        file.one -> file on both. names and hatch match -> no change
        file.two -> file on both. names match hatch not -> keep both. rename remote
        file.thr -> file on remote only                 -> move local to remote
        file.fou -> file on local only                  -> keep and rename
        """
        unmatched_state.push(safe=False)
        assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
            "hash same": {"hash_same", "hash_different", "local_only"},
            "hash different": set(),
            "remote only": set(),
            "local only": set(),
        }

    def test_pull_safe(self, unmatched_state) -> None:
        """Rename conflicts on local

        file.one -> file on both. names and hatch match -> no change
        file.two -> file on both. names match hatch not -> keep both. rename local
        file.thr -> file on remote only                 -> move remote to local
        file.fou -> file on local only                  -> keep and rename
        """
        unmatched_state.pull()
        assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
            "hash same": {"hash_same", "hash_different", "remote_only"},
            "hash different": set(),
            "remote only": set(),
            "local only": {"[loc0]hash_different", "[loc0]local_only"},
        }

    def test_pull_unsafe(self, unmatched_state) -> None:
        """Discard conflicts on local

        file.one -> file on both. names and hatch match -> no change
        file.two -> file on both. names match hatch not -> delete local, keep remote
        file.thr -> file on remote only                 -> move remote to local
        file.fou -> file on local only                  -> delete
        """
        unmatched_state.pull(safe=False)
        assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
            "hash same": {"hash_same", "hash_different", "remote_only"},
            "hash different": set(),
            "remote only": set(),
            "local only": set(),
        }

    def test_subfolders(self, matching_state) -> None:
        """Subfolders write to s3 as subfolder/filename, any depth"""
        files = {"sub1/1deep.file", "sub1/sub2/2deep.file"}
        _create_local_files(files)
        matching_state.push()
        matching_state.pull()
        assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
            "remote only": set(),
            "local only": set(),
            "hash different": set(),
            "hash same": {
                "remote_only",
                "sub1/sub2/2deep.file",
                "hash_different",
                "sub1/1deep.file",
                "hash_same",
                "local_only",
            },
        }

    def test_pull_subfolders(self, matching_state) -> None:
        """Subfolders write to s3 as subfolder/filename, any depth"""
        files = {"sub1/1deep.file", "sub1/sub2/2deep.file"}
        _create_local_files(files)
        matching_state.push()
        os.remove(TEMP_LOCAL_DIR / "sub1/1deep.file")
        matching_state.pull()
        breakpoint()
        assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
            "remote only": set(),
            "local only": set(),
            "hash different": set(),
            "hash same": {
                "remote_only",
                "sub1/sub2/2deep.file",
                "hash_different",
                "sub1/1deep.file",
                "hash_same",
                "local_only",
            },
        }
