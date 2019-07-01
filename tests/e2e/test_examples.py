import re
import shlex
import subprocess
import time

from kopf.testing import KopfRunner


def test_all_examples_are_runnable(mocker, with_crd, with_peering, exampledir):

    # If the example has its own opinion on the timing, try to respect it.
    # See e.g. /examples/99-all-at-once/example.py.
    example_py = exampledir / 'example.py'
    m = re.search(r'^E2E_CREATE_TIME\s*=\s*(\d+)$', example_py.read_text(), re.M)
    e2e_create_time = eval(m.group(1)) if m else None
    m = re.search(r'^E2E_DELETE_TIME\s*=\s*(\d+)$', example_py.read_text(), re.M)
    e2e_delete_time = eval(m.group(1)) if m else None
    m = re.search(r'^E2E_TRACEBACKS\s*=\s*(\w+)$', example_py.read_text(), re.M)
    e2e_tracebacks = eval(m.group(1)) if m else None

    # To prevent lengthy sleeps on the simulated retries.
    mocker.patch('kopf.reactor.handling.DEFAULT_RETRY_DELAY', 1)

    # To prevent lengthy threads in the loop executor when the process exits.
    mocker.patch('kopf.clients.watching.DEFAULT_STREAM_TIMEOUT', 10)

    # Run an operator and simulate some activity with the operated resource.
    with KopfRunner(['run', '--verbose', str(example_py)]) as runner:
        subprocess.run("kubectl apply -f examples/obj.yaml", shell=True, check=True)
        time.sleep(e2e_create_time or 1)  # give it some time to react and to sleep and to retry
        subprocess.run("kubectl delete -f examples/obj.yaml", shell=True, check=True)
        time.sleep(e2e_delete_time or 1)  # give it some time to react

    # Ensure that the operator did not die on start, or during the operation.
    assert runner.exception is None
    assert runner.exit_code == 0

    # There are usually more than these messages, but we only check for the certain ones.
    # This just shows us that the operator is doing something, it is alive.
    assert '[default/kopf-example-1] First appearance:' in runner.stdout
    assert '[default/kopf-example-1] Creation event:' in runner.stdout
    assert '[default/kopf-example-1] Deletion event:' in runner.stdout
    assert '[default/kopf-example-1] Deleted, really deleted' in runner.stdout
    if not e2e_tracebacks:
        assert 'Traceback (most recent call last):' not in runner.stdout
