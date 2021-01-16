import glob
import os.path
import pathlib
import subprocess

import pytest

from kopf.reactor.registries import SmartOperatorRegistry

root_dir = os.path.relpath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
examples = sorted(glob.glob(os.path.join(root_dir, 'examples/*/')))
assert examples  # if empty, it is just the detection failed
examples = [path for path in examples if not glob.glob((os.path.join(path, 'test*.py')))]


@pytest.fixture
def registry_factory():
    # Authentication is needed for the real e2e tests.
    return SmartOperatorRegistry


@pytest.fixture(params=examples, ids=[os.path.basename(path.rstrip('/')) for path in examples])
def exampledir(request):
    return pathlib.Path(request.param)


@pytest.fixture(scope='session')
def peering_yaml():
    crd_api = os.environ.get('CRDAPI') or 'v1'
    crd_file = 'peering.yaml' if crd_api == 'v1' else f'peering-{crd_api}.yaml'
    return f'{crd_file}'


@pytest.fixture(scope='session')
def crd_yaml():
    crd_api = os.environ.get('CRDAPI') or 'v1'
    crd_file = 'crd.yaml' if crd_api == 'v1' else f'crd-{crd_api}.yaml'
    return f'examples/{crd_file}'


@pytest.fixture()
def with_crd(crd_yaml):
    # Our best guess on which Kubernetes version we are running on.
    subprocess.run(f"kubectl apply -f {crd_yaml}",
                   shell=True, check=True, timeout=10, capture_output=True)


@pytest.fixture()
def with_peering(peering_yaml):
    subprocess.run(f"kubectl apply -f {peering_yaml}",
                   shell=True, check=True, timeout=10, capture_output=True)


@pytest.fixture()
def no_crd():
    subprocess.run("kubectl delete customresourcedefinition kopfexamples.kopf.dev",
                   shell=True, check=True, timeout=10, capture_output=True)


@pytest.fixture()
def no_peering():
    subprocess.run("kubectl delete customresourcedefinition kopfpeerings.kopf.dev",
                   shell=True, check=True, timeout=10, capture_output=True)
