import logging

import pytest

from kopf.engines.loggers import LocalObjectLogger, ObjectLogger

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
async def test_posting_normal_levels(settings, caplog, logstream, logfn, event_type,
                                     event_queue, event_queue_loop):

    logger = ObjectLogger(body=OBJ1, settings=settings)
    logger_fn = getattr(logger, logfn)

    logger_fn("hello %s", "world")

    assert event_queue.qsize() == 1
    event1 = event_queue.get_nowait()
    assert event1.ref == REF1
    assert event1.type == event_type
    assert event1.reason == "Logging"
    assert event1.message == "hello world"
    assert caplog.messages == ["hello world"]


@pytest.mark.parametrize('logfn, event_type, min_levelno', [
    ['debug', "Debug", logging.DEBUG],
    ['info', "Normal", logging.INFO],
    ['warning', "Warning", logging.WARNING],
    ['error', "Error", logging.ERROR],
    ['critical', "Fatal", logging.CRITICAL],
])
async def test_posting_above_config(settings, caplog, logstream, logfn, event_type, min_levelno,
                                    event_queue, event_queue_loop, mocker):
    logger = ObjectLogger(body=OBJ1, settings=settings)
    logger_fn = getattr(logger, logfn)

    settings.posting.level = min_levelno
    logger_fn("hello %s", "world")
    settings.posting.level = min_levelno + 1
    logger_fn("must not be posted")

    assert event_queue.qsize() == 1
    event1 = event_queue.get_nowait()
    assert event1.ref == REF1
    assert event1.type == event_type
    assert event1.reason == "Logging"
    assert event1.message == "hello world"
    assert caplog.messages == ["hello world", "must not be posted"]


@pytest.mark.parametrize('logfn', [
    'debug',
])
async def test_skipping_hidden_levels(settings, caplog, logstream, logfn,
                                      event_queue, event_queue_loop):

    logger = ObjectLogger(body=OBJ1, settings=settings)
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
async def test_skipping_below_config(settings, caplog, logstream, logfn,
                                     event_queue, event_queue_loop, mocker):

    logger = ObjectLogger(body=OBJ1, settings=settings)
    logger_fn = getattr(logger, logfn)

    settings.posting.level = 666
    logger_fn("hello %s", "world")
    settings.posting.level = 0
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
async def test_skipping_when_disabled(settings, caplog, logstream, logfn,
                                      event_queue, event_queue_loop):

    logger = LocalObjectLogger(body=OBJ1, settings=settings)
    logger_fn = getattr(logger, logfn)

    settings.posting.enabled = False
    settings.posting.level = 0
    logger_fn("hello %s", "world")

    assert event_queue.qsize() == 0
    assert caplog.messages == ["hello world"]


@pytest.mark.parametrize('logfn', [
    'debug',
    'info',
    'warning',
    'error',
    'critical',
])
async def test_skipping_when_local_with_all_levels(settings, caplog, logstream, logfn,
                                                   event_queue, event_queue_loop):

    logger = LocalObjectLogger(body=OBJ1, settings=settings)
    logger_fn = getattr(logger, logfn)

    logger_fn("hello %s", "world")

    assert event_queue.qsize() == 0
    assert caplog.messages == ["hello world"]
