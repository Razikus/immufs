import pytest
from pytest_docker.plugin import get_docker_services
import requests
from immufs.client import ImmuFSClient
from immudb.client import ImmudbClient






def is_responsive(url):
    try:
        response = requests.get(url, timeout=0.05)
        if response.status_code == 200:
            return True
    except:
        return False

@pytest.fixture(scope="function")
def docker_services_each(docker_compose_file, docker_compose_project_name, docker_cleanup):
    with get_docker_services(
        docker_compose_file, docker_compose_project_name, docker_cleanup
    ) as docker_service:
        yield docker_service

@pytest.fixture(scope="function")
def immufsClient(docker_ip, docker_services_each):
    clientPort = docker_services_each.port_for("immudb", 3322)
    port = docker_services_each.port_for("immudb", 8080)
    url = "http://{}:{}".format(docker_ip, port)
    docker_services_each.wait_until_responsive(
        timeout=30.0, pause=0.2, check=lambda: is_responsive(url)
    )
    return ImmuFSClient(docker_ip, int(clientPort), "immudb", "immudb", "defaultdb")