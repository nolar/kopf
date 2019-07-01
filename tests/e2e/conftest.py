import glob
import os.path
import pathlib
import subprocess

import pytest

from kopf.clients.auth import login

root_dir = os.path.relpath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
examples = sorted(glob.glob(os.path.join(root_dir, 'examples/*/')))
assert examples  # if empty, it is just the detection failed


@pytest.fixture(params=examples)
def exampledir(request):
    return pathlib.Path(request.param)


@pytest.fixture(scope='session', autouse=True)
def autologin():
    if os.environ.get('E2E'):
        login()  # or anything like that; it is not a unit-under-test


@pytest.fixture()
def with_crd():
    subprocess.run("kubectl apply -f examples/crd.yaml", shell=True, check=True)


@pytest.fixture()
def with_peering():
    subprocess.run("kubectl apply -f peering.yaml", shell=True, check=True)


@pytest.fixture()
def no_crd():
    subprocess.run("kubectl delete customresourcedefinition kopfexamples.zalando.org", shell=True, check=True)


@pytest.fixture()
def no_peering():
    subprocess.run("kubectl delete customresourcedefinition kopfpeerings.zalando.org", shell=True, check=True)


@pytest.fixture(autouse=True)
def _skip_if_not_explicitly_enabled():
    # Minikube tests are heavy and require a cluster. Skip them by default,
    # so that the contributors can run pytest without initial tweaks.
    if not os.environ.get('E2E'):
        pytest.skip('e2e tests are not explicitly enabled; set E2E env var to enable.')


def pytest_collection_modifyitems(config, items):
    # Put the e2e tests to the end always, since they are a bit lengthy.
    etc = [item for item in items if '/e2e/' not in item.nodeid]
    e2e = [item for item in items if '/e2e/' in item.nodeid]
    items[:] = etc + e2e
