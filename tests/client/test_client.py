from pathlib import PurePath
import pytest
from .. import immufsClient 
from .. import ImmuFSClient
from io import BytesIO
from immufs.client import ErrorCode

def test_mount_unmount_test(immufsClient: ImmuFSClient):
    pytest.fail("Not implemented")

def test_list_directories(immufsClient: ImmuFSClient):
    pytest.fail("Not implemented")

def test_create_file(immufsClient: ImmuFSClient):
    assert immufsClient.createFile("/test", BytesIO(b"blabla")) == (True, None)
    assert immufsClient.createFile("/test/setup", BytesIO(b"blabla")) == (False, ErrorCode.DIRECTORY_NOT_EXISTS)

    alls = immufsClient.list_directory("/")
    assert alls == [PurePath("/test")]
    assert immufsClient.createFile("/test2", BytesIO(b"blabla")) == (True, None)
    assert immufsClient.createFile("/test3", BytesIO(b"blabla")) == (True, None)

    alls = immufsClient.list_directory("/")
    assert alls == [PurePath("/test"), PurePath("/test2"), PurePath("/test3")]

def test_create_directory(immufsClient: ImmuFSClient):
    assert immufsClient.createDirectory("/dir1") == (True, None)
    alls = immufsClient.list_directory("/")
    assert alls == [PurePath("/dir1/")]
    assert immufsClient.createDirectory("/dir1/dir2") == (True, None)
    alls = immufsClient.list_directory("/dir1/")
    assert alls == [PurePath("/dir1/dir2/")]
    alls = immufsClient.list_directory("/")
    assert alls == [PurePath("/dir1/")]
    assert immufsClient.createFile("/dir1/temp", BytesIO(b"blabla")) == (True, None)
    alls = immufsClient.list_directory("/")
    assert alls == [PurePath("/dir1/")]

    alls = immufsClient.list_directory("/dir1/")
    assert alls == [PurePath("/dir1/temp"), PurePath("/dir1/dir2/")]
    assert immufsClient.createDirectory("/dir1/dir2/dir4/") == (True, None)

    alls = immufsClient.list_directory("/dir1/dir2/")
    assert alls == [PurePath("/dir1/dir2/dir4")]

    assert immufsClient.createFile("/dir1/dir2/dir4/aaa", BytesIO(b"blabla")) == (True, None)

    alls = immufsClient.list_directory("/dir1/dir2/dir4/")
    assert alls == [PurePath("/dir1/dir2/dir4/aaa")]

    assert immufsClient.createDirectory("/dir1/dir2/ddd") == (True, None)

    alls = immufsClient.list_directory("/dir1/dir2/ddd/")
    assert alls == []


def test_move_file(immufsClient: ImmuFSClient):
    pytest.fail("Not implemented")

def test_remove_file(immufsClient: ImmuFSClient):
    pytest.fail("Not implemented")

def test_append_to_file(immufsClient: ImmuFSClient):
    pytest.fail("Not implemented")

def test_override_file(immufsClient: ImmuFSClient):
    pytest.fail("Not implemented")