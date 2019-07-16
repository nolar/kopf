import os.path
import subprocess
import time

import pytest

import kopf.testing

crd_yaml = os.path.relpath(os.path.join(os.path.dirname(__file__), '..', 'crd.yaml'))
obj_yaml = os.path.relpath(os.path.join(os.path.dirname(__file__), '..', 'obj.yaml'))
example_py = os.path.relpath(os.path.join(os.path.dirname(__file__), 'example.py'))


@pytest.fixture(autouse=True)
def crd_exists():
    subprocess.run(f"kubectl apply -f {crd_yaml}",
                   check=True, timeout=10, capture_output=True, shell=True)


@pytest.fixture(autouse=True)
def obj_absent():
    # Operator is not running in fixtures, so we need a force-delete (or this patch).
    subprocess.run(['kubectl', 'patch', '-f', obj_yaml,
                    '-p', '{"metadata":{"finalizers":[]}}',
                    '--type', 'merge'],
                   check=False, timeout=10, capture_output=True)
    subprocess.run(f"kubectl delete -f {obj_yaml}",
                   check=False, timeout=10, capture_output=True, shell=True)


def test_resource_lifecycle(mocker):

    # To prevent lengthy threads in the loop executor when the process exits.
    mocker.patch('kopf.config.WatchersConfig.default_stream_timeout', 10)

    # Run an operator and simulate some activity with the operated resource.
    with kopf.testing.KopfRunner(['run', '--verbose', '--standalone', example_py], timeout=60) as runner:
        subprocess.run(f"kubectl create -f {obj_yaml}", shell=True, check=True)
        time.sleep(5)  # give it some time to react
        subprocess.run(f"kubectl delete -f {obj_yaml}", shell=True, check=True)
        time.sleep(1)  # give it some time to react

    # Ensure that the operator did not die on start, or during the operation.
    assert runner.exception is None
    assert runner.exit_code == 0

    # There are usually more than these messages, but we only check for the certain ones.
    assert '[default/kopf-example-1] Creation event:' in runner.stdout
    assert '[default/kopf-example-1] Something was logged here.' in runner.stdout
