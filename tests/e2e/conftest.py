import glob
import os.path
import pathlib
import subprocess

import pytest

import kopf

root_dir = os.path.relpath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
examples = sorted(glob.glob(os.path.join(root_dir, 'examples/*/')))
assert examples  # if empty, it is just the detection failed


@pytest.fixture(params=examples)
def exampledir(request):
    return pathlib.Path(request.param)


@pytest.fixture(scope='session', autouse=True)
def autologin():
    kopf.config.login()  # or anything like that; it is not a unit-under-test


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
