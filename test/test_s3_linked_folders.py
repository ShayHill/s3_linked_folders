#!/usr/bin/env python3
# _*_ coding: utf-8 _*_
"""Test all functions in s3_linked_folders

:author: Shay Hill
:created: 11/20/2020
"""

raise RuntimeError('read the README then comment out this line to run the tests.')

import os
from pathlib import Path
from typing import Iterable

import pytest

from s3_linked_folders.s3_linked_folders import (
    RemoteBucket,
    _compare_remote_to_local,
    _get_next_revision,
    _hash_local_file,
    s3,
    s3client,
)

TEMP_LOCAL_DIR = Path(__file__, "../..", "temp_test_dir").resolve()
TEMP_S3_BUCKET = "temp-test-linked"


def _create_local_files(files: Iterable[str]) -> None:
    """
    Create text files in the TEMP_LOCAL_DIR

    :param files: filenames
    :effects: write files to local filesystem
    """
    for filename in files:
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


def _empty_temp_bucket() -> None:
    """
    Prepare s3 bucket for deletion by removing all files
    """
    for obj in s3client.list_objects(Bucket=TEMP_S3_BUCKET).get("Contents", []):
        s3client.delete_object(Bucket=TEMP_S3_BUCKET, Key=obj["Key"])


def _delete_temp_bucket() -> None:
    """
    Delete s3 bucket
    """
    _empty_temp_bucket()
    s3client.delete_bucket(Bucket=TEMP_S3_BUCKET)


def _empty_temp_local_dir() -> None:
    """
    Remove all files from TEMP_LOCAL_DIR
    """
    for file in os.listdir(TEMP_LOCAL_DIR):
        os.remove(TEMP_LOCAL_DIR / file)


def _delete_temp_local_dir() -> None:
    """
    Remove TEMP_LOCAL_DIR
    """
    _empty_temp_local_dir()
    os.rmdir(TEMP_LOCAL_DIR)


def _get_remote_filenames():
    """Set of remote filenames"""
    remote_files = set()
    for obj in s3client.list_objects(Bucket=TEMP_S3_BUCKET).get("Contents", []):
        remote_files.add(obj["Key"])
    return remote_files


def _get_local_filenames():
    """Set of local filenames"""
    return {x for x in os.listdir(TEMP_LOCAL_DIR)}


@pytest.fixture(scope="function")
def matching_state() -> RemoteBucket:
    """
    Link a remote bucket and local folder, put four files in each.
    :yield: the linked RemoteBucket object
    """
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

    # def test_pull(self, matching_state) -> None:
    #     """Pulling into an empty local directory creates match to remote directory"""
    #     _empty_temp_local_dir()
    #     matching_state.pull()
    #     assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
    #         "remote only": set(),
    #         "local only": set(),
    #         "hash different": set(),
    #         "hash same": {"file.one", "file.two"},
    #     }
    #
    # def test_push_altered(self, matching_state) -> None:
    #     """Create [rem0] files if names match but hatches differ"""
    #     _alter_local_files({"file.one"})
    #     matching_state.push()
    #     assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
    #         "remote only": {"[rem0]file.one"},
    #         "local only": set(),
    #         "hash different": set(),
    #         "hash same": {"file.two", "file.one"},
    #     }
    #
    # def test_pull_merge(self, matching_state) -> None:
    #     """Create [loc0] files if names match but hashes differ"""
    #     _alter_local_files({"file.one"})
    #     os.remove(TEMP_LOCAL_DIR / "file.three")
    #     matching_state.pull()
    #     assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
    #         "remote only": set(),
    #         "local only": {"[loc0]file.one"},
    #         "hash different": set(),
    #         "hash same": {"file.two", "file.one"},
    #     }
    #
    # def test_push_warn_missing(self, matching_state) -> None:
    #     """By default, warn about extra remote files when pushing."""
    #     os.remove(TEMP_LOCAL_DIR / "file.one")
    #     with pytest.warns(UserWarning) as record:
    #         matching_state.push()
    #     assert "file.one" in record[0].message.args[0]
    #     # nothing deleted
    #     assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
    #         "remote only": {"file.one"},
    #         "local only": set(),
    #         "hash different": set(),
    #         "hash same": {"file.two"},
    #     }
    #
    # def test_pull_warn_missing(self, matching_state) -> None:
    #     """By default, warn about extra local files when pulling."""
    #     s3.Object(TEMP_S3_BUCKET, "file.one").delete()
    #     with pytest.warns(UserWarning) as record:
    #         matching_state.pull()
    #     assert "file.one" in record[0].message.args[0]
    #     assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
    #         "remote only": set(),
    #         "local only": {"file.one"},
    #         "hash different": set(),
    #         "hash same": {"file.two"},
    #     }
    #
    # def test_push_remove_missing(self, matching_state) -> None:
    #     """Remove remote files that aren't on local."""
    #     os.remove(TEMP_LOCAL_DIR / "file.one")
    #     matching_state.push(do_delete_unmatched=True)
    #     assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
    #         "remote only": set(),
    #         "local only": set(),
    #         "hash different": set(),
    #         "hash same": {"file.two"},
    #     }
    #
    # def test_pull_remove_missing(self, matching_state) -> None:
    #     """Remove remote files that aren't on remote."""
    #     s3.Object(TEMP_S3_BUCKET, "file.one").delete()
    #     matching_state.pull(do_delete_unmatched=True)
    #     assert _compare_remote_to_local(TEMP_S3_BUCKET, TEMP_LOCAL_DIR) == {
    #         "remote only": set(),
    #         "local only": set(),
    #         "hash different": set(),
    #         "hash same": {"file.two"},
    #     }
