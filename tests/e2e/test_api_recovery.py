"""
Real-cluster tests: the operator survives a temporary failure of the real API server.

The tests run a real operator process against the throw-away k3d cluster and break the API
the way it breaks in production: a custom resource definition is removed while the operator
watches it (the API answers 404), the API server restarts (the API is unreachable),
and the peering object is absent or deleted and created again.
The operator must keep running and must handle the objects created after the API is back.
"""
import json
import re
import subprocess
import textwrap
import time

import pytest

from kopf.testing import KopfRunner

pytestmark = pytest.mark.timeout(900)  # includes k3d cluster creation on the first test

OPERATOR = textwrap.dedent('''
    import kopf

    @kopf.on.create('kopfexamples', id='created')
    def created(name, logger, **_):
        logger.info(f"Handled {name}.")  # the standard message does not name the object
''')


def kubectl(command: str, *, timeout: float = 60, stdin: str | None = None) -> str:
    return subprocess.run(f"kubectl {command}", shell=True, check=True, timeout=timeout,
                          input=stdin, capture_output=True, text=True).stdout


def create_object(name: str) -> None:
    kubectl("apply -f -", stdin=textwrap.dedent(f"""
        apiVersion: kopf.dev/v1
        kind: KopfExample
        metadata:
          name: {name}
    """))


def create_cluster_peering() -> None:
    kubectl("apply -f -", stdin=textwrap.dedent("""
        apiVersion: kopf.dev/v1
        kind: ClusterKopfPeering
        metadata:
          name: default
    """))


def cluster_peering_has_records() -> bool:
    body = json.loads(kubectl("get clusterkopfpeering default -o json"))
    return bool(body.get('status'))


def count_messages(caplog, pattern: str) -> int:
    return len([msg for msg in list(caplog.messages) if re.search(pattern, msg)])


def wait_until(condition, *, timeout: float, what: str) -> None:
    started = time.perf_counter()
    while not condition():
        if time.perf_counter() - started > timeout:
            raise TimeoutError(f"Timed out waiting for: {what}")
        time.sleep(0.2)


def wait_for_crd() -> None:
    kubectl("wait --for=condition=established crd/kopfexamples.kopf.dev --timeout=60s")


def wait_for_api(*, timeout: float = 180) -> None:
    def ready() -> bool:
        result = subprocess.run("kubectl get --raw /readyz", shell=True, timeout=30,
                                capture_output=True, text=True)
        return result.returncode == 0
    wait_until(ready, timeout=timeout, what="the API server to become ready")


@pytest.fixture()
def operator_file(tmp_path):
    path = tmp_path / 'operator.py'
    path.write_text(OPERATOR)
    return path


@pytest.fixture(autouse=True)
def no_leftover_objects(k3d_cluster, with_crd):
    # An object left by another test would be "resumed", not "created", by the next operator.
    wait_for_crd()
    kubectl("delete kopfexamples --all --ignore-not-found --wait=true")


@pytest.fixture()
def fast_recovery_settings(settings):
    # The retries inside one request would otherwise hide a short outage for about a minute.
    settings.networking.error_backoffs = [0.2, 0.5]
    settings.watching.reconnect_backoff = 0.5
    # To prevent lengthy threads in the loop executor when the process exits.
    settings.watching.server_timeout = 10
    # A failing operator must not SIGKILL the test process.
    settings.process.ultimate_exiting_timeout = None
    return settings


def test_operator_survives_removal_and_return_of_the_custom_resource_definition(
        k3d_cluster, with_crd, fast_recovery_settings, operator_file, caplog):
    settings = fast_recovery_settings
    # The operator's own watching of definitions would stop the watcher gracefully, but only
    # eventually. Without it, the watch really hits the missing definition (HTTP 404).
    settings.scanning.disabled = True
    wait_for_crd()

    with KopfRunner(
        ['run', '--all-namespaces', '--standalone', '--verbose', str(operator_file)],
        timeout=300, settings=settings,
    ) as runner:
        wait_until(lambda: count_messages(caplog, 'Client is configured') > 0,
                   timeout=60, what="the operator to start")

        create_object("before-removal")
        wait_until(lambda: count_messages(caplog, r"Handled before-removal\.") == 1,
                   timeout=60, what="the handler for the first object")

        kubectl("delete -f examples/crd.yaml --wait=true")
        wait_until(lambda: count_messages(caplog, r"Watch-stream .* failed; reconnecting") > 0,
                   timeout=60, what="the watcher to notice the missing definition")

        kubectl("apply -f examples/crd.yaml")
        wait_for_crd()
        create_object("after-return")
        wait_until(lambda: count_messages(caplog, r"Handled after-return\.") == 1,
                   timeout=120, what="the handler for the object created after the return")

    assert runner.exception is None
    assert runner.exit_code == 0
    assert count_messages(caplog, r"Handled before-\w+\.") == 1  # no repeated handling


def test_operator_survives_a_restart_of_the_api_server(
        k3d_cluster, with_crd, fast_recovery_settings, operator_file, caplog):
    wait_for_crd()

    with KopfRunner(
        ['run', '--all-namespaces', '--standalone', '--verbose', str(operator_file)],
        timeout=400, settings=fast_recovery_settings,
    ) as runner:
        wait_until(lambda: count_messages(caplog, 'Client is configured') > 0,
                   timeout=60, what="the operator to start")

        create_object("before-restart")
        wait_until(lambda: count_messages(caplog, r"Handled before-restart\.") == 1,
                   timeout=60, what="the handler for the first object")

        # The restart takes at least the container's stop time: the API is down for tens of seconds.
        subprocess.run(f"docker restart k3d-{k3d_cluster}-server-0", shell=True, check=True,
                       timeout=180, capture_output=True)
        wait_until(lambda: count_messages(caplog, r"Watch-stream .* failed; reconnecting") > 0,
                   timeout=60, what="the watcher to notice the outage")
        wait_for_api()

        create_object("after-restart")
        wait_until(lambda: count_messages(caplog, r"Handled after-restart\.") == 1,
                   timeout=120, what="the handler for the object created after the restart")

    assert runner.exception is None
    assert runner.exit_code == 0
    assert count_messages(caplog, r"Handled before-\w+\.") == 1  # no repeated handling


def test_mandatory_peering_pauses_until_its_object_returns(
        k3d_cluster, with_crd, with_peering, fast_recovery_settings, operator_file, caplog):
    settings = fast_recovery_settings
    settings.peering.lifetime = 4
    kubectl("delete clusterkopfpeering default --ignore-not-found --wait=true")

    with KopfRunner(
        ['run', '--all-namespaces', '--peering=default', '--verbose', str(operator_file)],
        timeout=300, settings=settings,
    ) as runner:
        wait_until(lambda: count_messages(caplog, 'Client is configured') > 0,
                   timeout=60, what="the mandatory-peering operator to start")
        wait_until(lambda: count_messages(
            caplog, r"Pausing the watch-stream for kopfexamples") > 0,
            timeout=30, what="event handling to pause before the first peering record")

        create_object("blocked-at-start")
        time.sleep(2)
        assert count_messages(caplog, r"Handled blocked-at-start\.") == 0

        create_cluster_peering()
        wait_until(lambda: count_messages(caplog, r"Handled blocked-at-start\.") == 1,
                   timeout=60, what="processing to resume after creating the peering object")

        pauses_before = count_messages(caplog, r"Pausing the watch-stream for kopfexamples")
        kubectl("delete clusterkopfpeering default --wait=true")
        wait_until(lambda: count_messages(
            caplog, r"Pausing the watch-stream for kopfexamples") > pauses_before,
            timeout=30, what="event handling to pause after deleting the peering object")

        create_object("blocked-after-deletion")
        time.sleep(2)
        assert count_messages(caplog, r"Handled blocked-after-deletion\.") == 0

        create_cluster_peering()
        wait_until(lambda: count_messages(caplog, r"Handled blocked-after-deletion\.") == 1,
                   timeout=60, what="processing to resume after restoring the peering object")

    assert runner.exception is None
    assert runner.exit_code == 0
    assert count_messages(caplog, r"Handled blocked-(at-start|after-deletion)\.") == 2


def test_optional_peering_republishes_its_record_when_the_object_appears(
        k3d_cluster, with_crd, with_peering, fast_recovery_settings, operator_file, caplog):
    settings = fast_recovery_settings
    settings.peering.lifetime = 60  # the periodic retry would otherwise happen after 30-36s
    kubectl("delete clusterkopfpeering default --ignore-not-found --wait=true")

    with KopfRunner(
        ['run', '--all-namespaces', '--verbose', str(operator_file)],
        timeout=300, settings=settings,
    ) as runner:
        wait_until(lambda: count_messages(caplog, r"Keep-alive .* not found") > 0,
                   timeout=60, what="optional peering to fall back to standalone")

        create_cluster_peering()
        wait_until(cluster_peering_has_records, timeout=10,
                   what="the optional operator to publish its record immediately")

    assert runner.exception is None
    assert runner.exit_code == 0
