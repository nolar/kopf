import pytest

from kopf.engines.logging import ObjectLogger

OBJ1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}}
REF1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}


@pytest.mark.parametrize('logfn, event_type', [
    ['info', "Normal"],
    ['warning', "Warning"],
    ['error', "Error"],
    ['critical', "Fatal"],
])
async def test_posting_normal_levels(caplog, logstream, logfn, event_type, event_queue, event_queue_loop):
    logger = ObjectLogger(body=OBJ1)
    logger_fn = getattr(logger, logfn)

    logger_fn("hello %s", "world")

    assert event_queue.qsize() == 1
    event1 = event_queue.get_nowait()
    assert event1.ref == REF1
    assert event1.type == event_type
    assert event1.reason == "Logging"
    assert event1.message == "hello world"
    assert caplog.messages == ["hello world"]


@pytest.mark.parametrize('logfn', [
    'debug',
])
async def test_skipping_hidden_levels(caplog, logstream, logfn, event_queue, event_queue_loop):
    logger = ObjectLogger(body=OBJ1)
    logger_fn = getattr(logger, logfn)

    logger_fn("hello %s", "world")
    logger.info("must be here")

    assert event_queue.qsize() == 1  # not 2!
    assert caplog.messages == ["hello world", "must be here"]


@pytest.mark.parametrize('logfn', [
    'debug',
    'info',
    'warning',
    'error',
    'critical',
])
async def test_skipping_when_local_with_all_levels(caplog, logstream, logfn, event_queue, event_queue_loop):
    logger = ObjectLogger(body=OBJ1)
    logger_fn = getattr(logger, logfn)

    logger_fn("hello %s", "world", local=True)
    logger.info("must be here")

    assert event_queue.qsize() == 1  # not 2!
    assert caplog.messages == ["hello world", "must be here"]
