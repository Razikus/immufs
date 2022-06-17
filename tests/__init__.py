import pytest
from immufs.client import ImmuFSClient
from immudb.client import ImmudbClient



@pytest.fixture
def immufsClient():
    return ImmuFSClient("localhost", 3322)


