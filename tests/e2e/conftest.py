import glob
import os.path
import pathlib
import subprocess

import pytest

root_dir = os.path.relpath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
examples = sorted(glob.glob(os.path.join(root_dir, 'examples/*/')))
assert examples  # if empty, it is just the detection failed
examples = [path for path in examples if not glob.glob((os.path.join(path, 'test*.py')))]


@pytest.fixture(params=examples, ids=[os.path.basename(path.rstrip('/')) for path in examples])
def exampledir(request):
    return pathlib.Path(request.param)


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
