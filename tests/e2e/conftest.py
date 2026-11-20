import glob
import os
import os.path
import pathlib
import shutil
import subprocess

import kubernetes.config.kube_config
import pytest

from kopf._core.intents.registries import SmartOperatorRegistry

root_dir = os.path.relpath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
examples = sorted(glob.glob(os.path.join(root_dir, 'examples/*/')))
assert examples  # if empty, it is just the detection failed
examples = [path for path in examples if not glob.glob(os.path.join(path, 'test*.py'))]


@pytest.fixture
def registry_factory():
    # Authentication is needed for the real e2e tests.
    return SmartOperatorRegistry


@pytest.fixture(params=examples, ids=[os.path.basename(path.rstrip('/')) for path in examples])
def exampledir(request):
    return pathlib.Path(request.param)


@pytest.fixture(scope='module')
def k3d_cluster(tmp_path_factory):
    """
    A throw-away K3s cluster, created for the module and deleted after it.

    The cluster is isolated from every other kube context on the machine:
    its kubeconfig is written to a temporary file, the default kubeconfig is not
    touched, and ``KUBECONFIG`` points to the temporary file only for this module.
    Thus, no ``kubectl`` call in the tests can reach any other cluster.

    Requires ``k3d`` and ``kubectl`` on the PATH and a running Docker daemon; skips otherwise.
    """
    if shutil.which('k3d') is None:
        pytest.skip("k3d is not installed.")
    if shutil.which('kubectl') is None:
        pytest.skip("kubectl is not installed.")
    if subprocess.run(['docker', 'info'], capture_output=True).returncode != 0:
        pytest.skip("Docker daemon is not running.")

    name = f'kopf-e2e-{os.getpid()}'
    kubeconfig = tmp_path_factory.mktemp('k3d') / 'kubeconfig'
    created_ok = False
    try:
        created = subprocess.run(
            ['k3d', 'cluster', 'create', name,
             '--kubeconfig-update-default=false',
             '--kubeconfig-switch-context=false',
             '--k3s-arg', '--disable=metrics-server@server:*',
             '--wait', '--timeout', '300s'],
            timeout=420, capture_output=True, text=True,
        )
        if created.returncode != 0:
            pytest.fail(f"Cannot create the k3d cluster:\n{created.stdout}\n{created.stderr}")
        created_ok = True
        result = subprocess.run(
            ['k3d', 'kubeconfig', 'get', name],
            check=True, timeout=30, capture_output=True, text=True,
        )
        kubeconfig.write_text(result.stdout)
        kubeconfig.chmod(0o600)

        with pytest.MonkeyPatch.context() as mp:
            mp.setenv('KUBECONFIG', str(kubeconfig))
            # The kubernetes client reads its default kubeconfig path once, at import time,
            # so the environment variable alone does not reach the operator's own login.
            mp.setattr(kubernetes.config.kube_config, 'KUBE_CONFIG_DEFAULT_LOCATION',
                       str(kubeconfig))
            yield name
    finally:
        deleted = subprocess.run(['k3d', 'cluster', 'delete', name],
                                 timeout=300, capture_output=True, text=True)
        if created_ok and deleted.returncode != 0:
            pytest.fail(f"Cannot delete the k3d cluster:\n{deleted.stdout}\n{deleted.stderr}")


@pytest.fixture()
def with_crd():
    # Our best guess on which Kubernetes version we are running on.
    subprocess.run(f"kubectl apply -f examples/crd.yaml",
                   shell=True, check=True, timeout=10, capture_output=True)


@pytest.fixture()
def with_peering():
    subprocess.run(f"kubectl apply -f peering.yaml",
                   shell=True, check=True, timeout=10, capture_output=True)


@pytest.fixture()
def no_crd():
    subprocess.run("kubectl delete customresourcedefinition kopfexamples.kopf.dev",
                   shell=True, check=True, timeout=10, capture_output=True)


@pytest.fixture()
def no_peering():
    subprocess.run("kubectl delete customresourcedefinition kopfpeerings.kopf.dev",
                   shell=True, check=True, timeout=10, capture_output=True)
