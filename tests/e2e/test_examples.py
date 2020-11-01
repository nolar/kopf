import collections
import re
import subprocess
import time
from typing import Any, Optional, Sequence

import pytest

from kopf.testing import KopfRunner


def test_all_examples_are_runnable(mocker, settings, with_crd, exampledir, caplog):

    # If the example has its own opinion on the timing, try to respect it.
    # See e.g. /examples/99-all-at-once/example.py.
    example_py = exampledir / 'example.py'
    e2e_startup_time_limit = _parse_e2e_value(str(example_py), 'E2E_STARTUP_TIME_LIMIT')
    e2e_startup_stop_words = _parse_e2e_value(str(example_py), 'E2E_STARTUP_STOP_WORDS')
    e2e_cleanup_time_limit = _parse_e2e_value(str(example_py), 'E2E_CLEANUP_TIME_LIMIT')
    e2e_cleanup_stop_words = _parse_e2e_value(str(example_py), 'E2E_CLEANUP_STOP_WORDS')
    e2e_creation_time_limit = _parse_e2e_value(str(example_py), 'E2E_CREATION_TIME_LIMIT')
    e2e_creation_stop_words = _parse_e2e_value(str(example_py), 'E2E_CREATION_STOP_WORDS')
    e2e_deletion_time_limit = _parse_e2e_value(str(example_py), 'E2E_DELETION_TIME_LIMIT')
    e2e_deletion_stop_words = _parse_e2e_value(str(example_py), 'E2E_DELETION_STOP_WORDS')
    e2e_tracebacks = _parse_e2e_value(str(example_py), 'E2E_TRACEBACKS')
    e2e_success_counts = _parse_e2e_value(str(example_py), 'E2E_SUCCESS_COUNTS')
    e2e_failure_counts = _parse_e2e_value(str(example_py), 'E2E_FAILURE_COUNTS')
    e2e_test_creation = _parse_e2e_presence(str(example_py), r'@kopf.on.create\(')
    e2e_test_highlevel = _parse_e2e_presence(str(example_py), r'@kopf.on.(create|update|delete)\(')

    # check whether there are mandatory deletion handlers or not
    m = re.search(r'@kopf\.on\.delete\((\s|.*)?(optional=(\w+))?\)', example_py.read_text(), re.M)
    requires_finalizer = False
    if m:
        requires_finalizer = True
        if m.group(2):
            requires_finalizer = not eval(m.group(3))

    # To prevent lengthy sleeps on the simulated retries.
    mocker.patch('kopf.reactor.handling.DEFAULT_RETRY_DELAY', 1)

    # To prevent lengthy threads in the loop executor when the process exits.
    settings.watching.server_timeout = 10

    # Run an operator and simulate some activity with the operated resource.
    with KopfRunner(
        ['run', '--all-namespaces', '--standalone', '--verbose', str(example_py)],
        timeout=60,
    ) as runner:

        # Give it some time to start.
        _sleep_till_stopword(caplog=caplog,
                             delay=e2e_startup_time_limit,
                             patterns=e2e_startup_stop_words or ['Client is configured'])

        # Trigger the reaction. Give it some time to react and to sleep and to retry.
        subprocess.run("kubectl apply -f examples/obj.yaml",
                       shell=True, check=True, timeout=10, capture_output=True)
        _sleep_till_stopword(caplog=caplog,
                             delay=e2e_creation_time_limit,
                             patterns=e2e_creation_stop_words)

        # Trigger the reaction. Give it some time to react.
        subprocess.run("kubectl delete -f examples/obj.yaml",
                       shell=True, check=True, timeout=10, capture_output=True)
        _sleep_till_stopword(caplog=caplog,
                             delay=e2e_deletion_time_limit,
                             patterns=e2e_deletion_stop_words)

    # Give it some time to finish.
    _sleep_till_stopword(caplog=caplog,
                         delay=e2e_cleanup_time_limit,
                         patterns=e2e_cleanup_stop_words or ['Hung tasks', 'Root tasks'])

    # Verify that the operator did not die on start, or during the operation.
    assert runner.exception is None
    assert runner.exit_code == 0

    # There are usually more than these messages, but we only check for the certain ones.
    # This just shows us that the operator is doing something, it is alive.
    if requires_finalizer:
        assert '[default/kopf-example-1] Adding the finalizer' in runner.stdout
    if e2e_test_creation:
        assert '[default/kopf-example-1] Creation is in progress:' in runner.stdout
    if requires_finalizer:
        assert '[default/kopf-example-1] Deletion is in progress:' in runner.stdout
    if e2e_test_highlevel:
        assert '[default/kopf-example-1] Deleted, really deleted' in runner.stdout
    if not e2e_tracebacks:
        assert 'Traceback (most recent call last):' not in runner.stdout

    # Verify that once a handler succeeds, it is never re-executed again.
    handler_names = re.findall(r"'(.+?)' succeeded", runner.stdout)
    if e2e_success_counts is not None:
        checked_names = [name for name in handler_names if name in e2e_success_counts]
        name_counts = collections.Counter(checked_names)
        assert name_counts == e2e_success_counts
    else:
        name_counts = collections.Counter(handler_names)
        assert set(name_counts.values()) == {1}

    # Verify that once a handler fails, it is never re-executed again.
    handler_names = re.findall(r"'(.+?)' failed (?:permanently|with an exception. Will stop.)", runner.stdout)
    if e2e_failure_counts is not None:
        checked_names = [name for name in handler_names if name in e2e_failure_counts]
        name_counts = collections.Counter(checked_names)
        assert name_counts == e2e_failure_counts
    else:
        name_counts = collections.Counter(handler_names)
        assert not name_counts


def _parse_e2e_value(path: str, name: str) -> Any:
    with open(path, 'rt', encoding='utf-8') as f:
        name = re.escape(name)
        text = f.read()
        m = re.search(fr'^{name}\s*=\s*(.+)$', text, re.M)
        return eval(m.group(1)) if m else None


def _parse_e2e_presence(path: str, pattern: str) -> bool:
    with open(path, 'rt', encoding='utf-8') as f:
        text = f.read()
        m = re.search(pattern, text, re.M)
        return bool(m)


def _sleep_till_stopword(
        caplog,
        delay: float,
        patterns: Sequence[str] = (),
        *,
        interval: Optional[float] = None,
) -> bool:
    patterns = list(patterns or [])
    delay = delay or (10.0 if patterns else 1.0)
    interval = interval or min(1.0, max(0.1, delay / 10.))
    started = time.perf_counter()
    found = False
    while not found and time.perf_counter() - started < delay:
        for message in list(caplog.messages):
            if any(re.search(pattern, message) for pattern in patterns):
                found = True
                break
        else:
            time.sleep(interval)
    return found
