import asyncio
import logging
from unittest.mock import call

import pytest

from kopf.reactor.effects import throttled
from kopf.structs.containers import Throttler


@pytest.fixture(autouse=True)
def clock(mocker):
    return mocker.patch('time.monotonic', return_value=0)


@pytest.fixture(autouse=True)
def sleep(mocker):
    return mocker.patch('kopf.structs.primitives.sleep_or_wait', return_value=None)


async def test_remains_inactive_on_success():
    logger = logging.getLogger()
    throttler = Throttler()
    async with throttled(throttler=throttler, logger=logger, delays=[123]):
        pass
    assert throttler.source_of_delays is None
    assert throttler.last_used_delay is None


@pytest.mark.parametrize('exc_cls, kwargs', [
    pytest.param(BaseException, dict(), id='none'),
    pytest.param(BaseException, dict(errors=BaseException), id='base'),
    pytest.param(Exception, dict(errors=ValueError), id='child'),
    pytest.param(RuntimeError, dict(errors=ValueError), id='sibling'),
    pytest.param(RuntimeError, dict(errors=(ValueError, TypeError)), id='tuple'),
    pytest.param(asyncio.CancelledError, dict(), id='cancelled'),
])
async def test_escalates_unexpected_errors(exc_cls, kwargs):
    logger = logging.getLogger()
    throttler = Throttler()
    with pytest.raises(exc_cls):
        async with throttled(throttler=throttler, logger=logger, delays=[123], **kwargs):
            raise exc_cls()


@pytest.mark.parametrize('exc_cls, kwargs', [
    pytest.param(Exception, dict(), id='none'),
    pytest.param(RuntimeError, dict(errors=Exception), id='parent'),
    pytest.param(RuntimeError, dict(errors=(RuntimeError, EnvironmentError)), id='tuple'),
])
async def test_activates_on_expected_errors(exc_cls, kwargs):
    logger = logging.getLogger()
    throttler = Throttler()
    async with throttled(throttler=throttler, logger=logger, delays=[123], **kwargs):
        raise exc_cls()
    assert throttler.source_of_delays is not None
    assert throttler.last_used_delay is not None


async def test_sleeps_for_the_first_delay_when_inactive(sleep):
    logger = logging.getLogger()
    throttler = Throttler()

    async with throttled(throttler=throttler, logger=logger, delays=[123, 234]):
        raise Exception()

    assert throttler.last_used_delay == 123
    assert throttler.source_of_delays is not None
    assert next(throttler.source_of_delays) == 234

    assert throttler.active_until is None  # means: no sleep time left
    assert sleep.mock_calls == [call(123, wakeup=None)]


async def test_sleeps_for_the_next_delay_when_active(sleep):
    logger = logging.getLogger()
    throttler = Throttler()

    async with throttled(throttler=throttler, logger=logger, delays=[123, 234]):
        raise Exception()

    sleep.reset_mock()
    async with throttled(throttler=throttler, logger=logger, delays=[...]):
        raise Exception()

    assert throttler.last_used_delay == 234
    assert throttler.source_of_delays is not None
    assert next(throttler.source_of_delays, 999) == 999

    assert throttler.active_until is None  # means: no sleep time left
    assert sleep.mock_calls == [call(234, wakeup=None)]


async def test_sleeps_for_the_last_known_delay_when_depleted(sleep):
    logger = logging.getLogger()
    throttler = Throttler()

    async with throttled(throttler=throttler, logger=logger, delays=[123, 234]):
        raise Exception()

    async with throttled(throttler=throttler, logger=logger, delays=[...]):
        raise Exception()

    sleep.reset_mock()
    async with throttled(throttler=throttler, logger=logger, delays=[...]):
        raise Exception()

    assert throttler.last_used_delay == 234
    assert throttler.source_of_delays is not None
    assert next(throttler.source_of_delays, 999) == 999

    assert throttler.active_until is None  # means: no sleep time left
    assert sleep.mock_calls == [call(234, wakeup=None)]


async def test_resets_on_success(sleep):
    logger = logging.getLogger()
    throttler = Throttler()

    async with throttled(throttler=throttler, logger=logger, delays=[123]):
        raise Exception()

    sleep.reset_mock()
    async with throttled(throttler=throttler, logger=logger, delays=[...]):
        pass

    assert throttler.last_used_delay is None
    assert throttler.source_of_delays is None
    assert throttler.active_until is None
    assert sleep.mock_calls == []


async def test_skips_on_no_delays(sleep):
    logger = logging.getLogger()
    throttler = Throttler()

    async with throttled(throttler=throttler, logger=logger, delays=[]):
        raise Exception()

    assert throttler.last_used_delay is None
    assert throttler.source_of_delays is not None
    assert next(throttler.source_of_delays, 999) == 999

    assert throttler.active_until is None  # means: no sleep time left
    assert sleep.mock_calls == []


async def test_renews_on_repeated_failure(sleep):
    logger = logging.getLogger()
    throttler = Throttler()

    async with throttled(throttler=throttler, logger=logger, delays=[123]):
        raise Exception()

    async with throttled(throttler=throttler, logger=logger, delays=[...]):
        pass

    sleep.reset_mock()
    async with throttled(throttler=throttler, logger=logger, delays=[234]):
        raise Exception()

    assert throttler.last_used_delay is 234
    assert throttler.source_of_delays is not None
    assert throttler.active_until is None
    assert sleep.mock_calls == [call(234, wakeup=None)]


async def test_interruption(clock, sleep):
    wakeup = asyncio.Event()
    logger = logging.getLogger()
    throttler = Throttler()

    clock.return_value = 1000  # simulated "now"
    sleep.return_value = 55  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[123, 234], wakeup=wakeup):
        raise Exception()

    assert throttler.last_used_delay == 123
    assert throttler.source_of_delays is not None
    assert throttler.active_until == 1123  # means: some sleep time is left
    assert sleep.mock_calls == [call(123, wakeup=wakeup)]


async def test_continuation_with_success(clock, sleep):
    wakeup = asyncio.Event()
    logger = logging.getLogger()
    throttler = Throttler()

    clock.return_value = 1000  # simulated "now"
    sleep.return_value = 55  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[123, 234], wakeup=wakeup):
        raise Exception()

    sleep.reset_mock()
    clock.return_value = 1077  # simulated "now"
    sleep.return_value = None  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[...], wakeup=wakeup):
        pass

    assert throttler.last_used_delay is None
    assert throttler.source_of_delays is None
    assert throttler.active_until is None  # means: no sleep time is left
    assert sleep.mock_calls == [call(123 - 77, wakeup=wakeup)]


async def test_continuation_with_error(clock, sleep):
    wakeup = asyncio.Event()
    logger = logging.getLogger()
    throttler = Throttler()

    clock.return_value = 1000  # simulated "now"
    sleep.return_value = 55  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[123, 234], wakeup=wakeup):
        raise Exception()

    sleep.reset_mock()
    clock.return_value = 1077  # simulated "now"
    sleep.return_value = None  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[...], wakeup=wakeup):
        raise Exception()

    assert throttler.last_used_delay == 234
    assert throttler.source_of_delays is not None
    assert throttler.active_until is None  # means: no sleep time is left
    assert sleep.mock_calls == [call(123 - 77, wakeup=wakeup), call(234, wakeup=wakeup)]


async def test_continuation_when_overdue(clock, sleep):
    wakeup = asyncio.Event()
    logger = logging.getLogger()
    throttler = Throttler()

    clock.return_value = 1000  # simulated "now"
    sleep.return_value = 55  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[123, 234], wakeup=wakeup):
        raise Exception()

    sleep.reset_mock()
    clock.return_value = 2000  # simulated "now"
    sleep.return_value = None  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[...], wakeup=wakeup):
        raise Exception()

    assert throttler.last_used_delay == 234
    assert throttler.source_of_delays is not None
    assert throttler.active_until is None  # means: no sleep time is left
    assert sleep.mock_calls == [call(123 - 1000, wakeup=wakeup), call(234, wakeup=wakeup)]


async def test_recommends_running_initially():
    logger = logging.getLogger()
    throttler = Throttler()
    async with throttled(throttler=throttler, logger=logger, delays=[123]) as should_run:
        remembered_should_run = should_run
    assert remembered_should_run is True


async def test_recommends_skipping_immediately_after_interrupted_error(sleep):
    logger = logging.getLogger()
    throttler = Throttler()

    sleep.return_value = 33  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[123]):
        raise Exception()

    sleep.return_value = 33  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[...]) as should_run:
        remembered_should_run = should_run

    assert remembered_should_run is False


async def test_recommends_running_immediately_after_continued(sleep):
    logger = logging.getLogger()
    throttler = Throttler()

    sleep.return_value = 33  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[123]):
        raise Exception()

    sleep.return_value = None  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[...]) as should_run:
        remembered_should_run = should_run

    assert remembered_should_run is True


async def test_logging_when_deactivates_immediately(caplog):
    caplog.set_level(0)
    logger = logging.getLogger()
    throttler = Throttler()

    async with throttled(throttler=throttler, logger=logger, delays=[123]):
        raise Exception("boo!")

    assert caplog.messages == [
        "Throttling for 123 seconds due to an unexpected error: Exception('boo!')",
        "Throttling is over. Switching back to normal operations.",
    ]


async def test_logging_when_deactivates_on_reentry(sleep, caplog):
    caplog.set_level(0)
    logger = logging.getLogger()
    throttler = Throttler()

    sleep.return_value = 55  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[123]):
        raise Exception("boo!")

    sleep.return_value = None  # simulated sleep time left
    async with throttled(throttler=throttler, logger=logger, delays=[...]):
        pass

    assert caplog.messages == [
        "Throttling for 123 seconds due to an unexpected error: Exception('boo!')",
        "Throttling is over. Switching back to normal operations.",
    ]
