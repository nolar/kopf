import functools
import logging

import pytest

from kopf._core.actions.loggers import ObjectLogger
from kopf._core.engines.posting import event, exception, info, warn

OBJ1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}}
REF1 = {'apiVersion': 'group1/version1', 'kind': 'Kind1',
        'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1'}


@pytest.fixture(autouse=True)
def _settings_via_contextvar(settings_via_contextvar):
    pass


async def test_loggers_setting_default_is_true(settings):
    assert settings.posting.loggers  # for backwards compatibility


async def test_loggers_setting_true_posts_loggers(settings, event_queue, event_queue_loop, caplog):
    settings.posting.loggers = True
    logger = ObjectLogger(body=OBJ1, settings=settings)
    with caplog.at_level(logging.INFO):
        logger.info("hello world")

    assert event_queue.qsize() == 1
    event1 = event_queue.get_nowait()
    assert event1.message == "hello world"


async def test_loggers_setting_false_skips_loggers(settings, event_queue, event_queue_loop, caplog):
    settings.posting.loggers = False
    logger = ObjectLogger(body=OBJ1, settings=settings)
    with caplog.at_level(logging.INFO):
        logger.info("this log should be skipped")

    assert event_queue.qsize() == 0


@pytest.mark.parametrize('routine', [
    functools.partial(event, type='irrelevant'),
    info,
    warn,
    exception,
])
@pytest.mark.parametrize('flag', [True, False], ids=['on', 'off'])
async def test_explicit_events_posted_regardless_of_settings(settings, event_queue, event_queue_loop, flag, routine):
    settings.posting.loggers = flag
    routine(OBJ1, reason="ExplicitReason", message="this should be posted")

    assert event_queue.qsize() == 1
    event1 = event_queue.get_nowait()
    assert event1.reason == "ExplicitReason"
    assert event1.message == "this should be posted"
