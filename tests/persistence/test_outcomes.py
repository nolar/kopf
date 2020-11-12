from unittest.mock import Mock

import pytest

from kopf.storage.states import HandlerOutcome
from kopf.structs.callbacks import Result


@pytest.fixture()
def handler():
    return Mock(id='id', spec_set=['id'])


def test_creation_for_ignored_handlers(handler):
    outcome = HandlerOutcome(final=True, handler=handler)
    assert outcome.final
    assert outcome.delay is None
    assert outcome.result is None
    assert outcome.exception is None
    assert not outcome.subrefs


def test_creation_for_results(handler):
    result = Result(object())
    outcome = HandlerOutcome(final=True, handler=handler, result=result)
    assert outcome.final
    assert outcome.delay is None
    assert outcome.result is result
    assert outcome.exception is None
    assert not outcome.subrefs


def test_creation_for_permanent_errors(handler):
    error = Exception()
    outcome = HandlerOutcome(final=True, handler=handler, exception=error)
    assert outcome.final
    assert outcome.delay is None
    assert outcome.result is None
    assert outcome.exception is error
    assert not outcome.subrefs


def test_creation_for_temporary_errors(handler):
    error = Exception()
    outcome = HandlerOutcome(final=False, handler=handler, exception=error, delay=123)
    assert not outcome.final
    assert outcome.delay == 123
    assert outcome.result is None
    assert outcome.exception is error
    assert not outcome.subrefs


def test_creation_with_subrefs(handler):
    outcome = HandlerOutcome(final=True, handler=handler, subrefs=['sub1', 'sub2'])
    assert outcome.subrefs == ['sub1', 'sub2']
