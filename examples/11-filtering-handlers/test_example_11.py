import os.path
import subprocess
import time

import kopf.testing
import pytest

obj_yaml = os.path.relpath(os.path.join(os.path.dirname(__file__), '..', 'obj.yaml'))
example_py = os.path.relpath(os.path.join(os.path.dirname(__file__), 'example.py'))


@pytest.fixture(scope='session')
def crd_yaml():
    crd_api = os.environ.get('CRDAPI') or 'v1'
    crd_file = 'crd.yaml' if crd_api == 'v1' else f'crd-{crd_api}.yaml'
    return os.path.relpath(os.path.join(os.path.dirname(__file__), '..', crd_file))


@pytest.fixture(autouse=True)
def crd_exists(crd_yaml):
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


def test_handler_filtering():

    # To prevent lengthy threads in the loop executor when the process exits.
    settings = kopf.OperatorSettings()
    settings.watching.server_timeout = 10

    # Run an operator and simulate some activity with the operated resource.
    with kopf.testing.KopfRunner(
        ['run', '--all-namespaces', '--verbose', '--standalone', example_py],
        settings=settings,
    ) as runner:

        subprocess.run(f"kubectl create -f {obj_yaml}",
                       shell=True, check=True, timeout=10, capture_output=True)
        time.sleep(5)  # give it some time to react
        subprocess.run(f"kubectl patch -f {obj_yaml} --type merge -p '" '{"spec":{"field":"changed"}}' "'",
                       shell=True, check=True, timeout=10, capture_output=True)
        time.sleep(2)  # give it some time to react
        subprocess.run(f"kubectl delete -f {obj_yaml}",
                       shell=True, check=True, timeout=10, capture_output=True)
        time.sleep(1)  # give it some time to react

    # Ensure that the operator did not die on start, or during the operation.
    assert runner.exception is None
    assert runner.exit_code == 0

    # Check for correct log lines (to indicate correct handlers were executed).
    assert '[default/kopf-example-1] Label is matching.' in runner.stdout
    assert '[default/kopf-example-1] Label is present.' in runner.stdout
    assert '[default/kopf-example-1] Label is absent.' in runner.stdout
    assert '[default/kopf-example-1] Label callback matching.' in runner.stdout
    assert '[default/kopf-example-1] Annotation is matching.' in runner.stdout
    assert '[default/kopf-example-1] Annotation is present.' in runner.stdout
    assert '[default/kopf-example-1] Annotation is absent.' in runner.stdout
    assert '[default/kopf-example-1] Annotation callback mismatch.' not in runner.stdout
    assert '[default/kopf-example-1] Filter satisfied.' in runner.stdout
    assert '[default/kopf-example-1] Filter not satisfied.' not in runner.stdout
    assert '[default/kopf-example-1] Field value is satisfied.' in runner.stdout
    assert '[default/kopf-example-1] Field value is not satisfied.' not in runner.stdout
    assert '[default/kopf-example-1] Field presence is satisfied.' in runner.stdout
    assert '[default/kopf-example-1] Field presence is not satisfied.' not in runner.stdout
    assert '[default/kopf-example-1] Field change is satisfied.' in runner.stdout
    assert '[default/kopf-example-1] Field daemon is satisfied.' in runner.stdout
