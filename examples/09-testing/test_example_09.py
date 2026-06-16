import os.path
import subprocess
import textwrap

import kopf.testing
import pytest

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


def test_resource_lifecycle(tmp_path):
    # Prevent lengthy threads in the loop executor when the process exits.
    injected_py = tmp_path / 'injected.py'
    injected_py.write_text(textwrap.dedent("""
        import kopf

        @kopf.on.startup()
        def test_config(settings: kopf.OperatorSettings, **_: Any) -> None:
            settings.watching.server_timeout = 10
    """))

    # Run an operator and simulate some activity with the operated resource.
    # NB: not cluster-wide, since we do not want to block unrelated system pods from deletion.
    with kopf.testing.KopfCLI(
        ['run', '--verbose', '--standalone', '--namespace', 'default', example_py, str(injected_py)],
    ) as runner:
        runner.wait_for('watch-stream for kopfexamples.v1.kopf.dev', timeout=5)

        subprocess.run(f"kubectl create -f {obj_yaml}",
                       shell=True, check=True, timeout=10, capture_output=True)
        runner.wait_for('Creation is processed', timeout=5)

        subprocess.run(f"kubectl delete -f {obj_yaml}",
                       shell=True, check=True, timeout=10, capture_output=True)
        runner.wait_for('Deleted, really deleted', timeout=2)

    # There are usually more than these messages, but we only check for the certain ones.
    assert '[default/kopf-example-1] Creation is in progress:' in runner.output
    assert '[default/kopf-example-1] Something was logged here.' in runner.output
