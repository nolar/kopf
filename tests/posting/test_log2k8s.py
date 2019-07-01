import asyncio
import io
import logging

import pytest

import kopf
from kopf.engines.logging import ObjectLogger
from kopf.engines.logging import ObjectPrefixingFormatter

OBJ1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}}
REF1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}


@pytest.fixture()
def logstream(caplog):
    """ Prefixing is done at the final output. We have to intercept it. """

    logger = logging.getLogger()
    handlers = list(logger.handlers)

    kopf.configure(verbose=True)

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    formatter = ObjectPrefixingFormatter('prefix %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    try:
        with caplog.at_level(logging.DEBUG):
            yield stream
    finally:
        logger.removeHandler(handler)
        logger.handlers[:] = handlers  # undo `kopf.configure()`


@pytest.mark.parametrize('logfn, event_type', [
    ['info', "Normal"],
    ['warning', "Warning"],
    ['error', "Error"],
    ['critical', "Fatal"],
])
def test_posting_normal_levels(caplog, logstream, logfn, event_type):
    queue = asyncio.Queue()
    logger = ObjectLogger(body=OBJ1, event_queue=queue)

    getattr(logger, logfn)("hello %s", "world")

    assert queue.qsize() == 1
    event1 = queue.get_nowait()
    assert event1.ref == REF1
    assert event1.type == event_type
    assert event1.reason == "Logging"
    assert event1.message == "hello world"
    assert caplog.messages == ["hello world"]


@pytest.mark.parametrize('logfn', [
    'debug',
])
def test_skipping_debug_level(caplog, logstream, logfn):
    queue = asyncio.Queue()
    logger = ObjectLogger(body=OBJ1, event_queue=queue)

    getattr(logger, logfn)("hello %s", "world")

    assert queue.empty()
    assert caplog.messages == ["hello world"]


@pytest.mark.parametrize('logfn', [
    'debug',
    'info',
    'warning',
    'error',
    'critical',
])
def test_skipping_when_local_with_all_level(caplog, logstream, logfn):
    queue = asyncio.Queue()
    logger = ObjectLogger(body=OBJ1, event_queue=queue)

    getattr(logger, logfn)("hello %s", "world", local=True)

    assert queue.empty()
    assert caplog.messages == ["hello world"]
