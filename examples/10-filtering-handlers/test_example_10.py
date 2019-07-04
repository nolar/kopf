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
    subprocess.run(f"kubectl apply -f {crd_yaml}", shell=True, check=True)


@pytest.fixture(autouse=True)
def obj_absent():
    subprocess.run(f"kubectl delete -f {obj_yaml}", shell=True, check=False)


def test_handler_filtering(mocker):

    # To prevent lengthy threads in the loop executor when the process exits.
    mocker.patch('kopf.clients.watching.DEFAULT_STREAM_TIMEOUT', 10)

    # Run an operator and simulate some activity with the operated resource.
    with kopf.testing.KopfRunner(['run', '--verbose', '--standalone', example_py]) as runner:
        subprocess.run(f"kubectl create -f {obj_yaml}", shell=True, check=True)
        time.sleep(5)  # give it some time to react
        subprocess.run(f"kubectl delete -f {obj_yaml}", shell=True, check=True)
        time.sleep(1)  # give it some time to react

    # Ensure that the operator did not die on start, or during the operation.
    assert runner.exception is None
    assert runner.exit_code == 0

    # Check for correct log lines (to indicate correct handlers were executed).
    assert '[default/kopf-example-1] Label satisfied.' in runner.stdout
    assert '[default/kopf-example-1] Label exists.' in runner.stdout
    assert '[default/kopf-example-1] Label not satisfied.' not in runner.stdout
    assert '[default/kopf-example-1] Annotation satisfied.' in runner.stdout
    assert '[default/kopf-example-1] Annotation exists.' in runner.stdout
    assert '[default/kopf-example-1] Annotation not satisfied.' not in runner.stdout
