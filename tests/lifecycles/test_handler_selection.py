import pytest

import kopf
from kopf.structs.status import set_retry_time, get_retry_count


@pytest.mark.parametrize('lifecycle', [
    kopf.lifecycles.all_at_once,
    kopf.lifecycles.one_by_one,
    kopf.lifecycles.randomized,
    kopf.lifecycles.shuffled,
    kopf.lifecycles.asap,
])
def test_with_empty_input(lifecycle):
    handlers = []
    selected = lifecycle(handlers, body={})
    assert isinstance(selected, (tuple, list))
    assert len(selected) == 0


def test_all_at_once_respects_order():
    handler1 = object()
    handler2 = object()
    handler3 = object()

    handlers = [handler1, handler2, handler3]
    selected = kopf.lifecycles.all_at_once(handlers)
    assert isinstance(selected, (tuple, list))
    assert len(selected) == 3
    assert list(selected) == handlers

    handlers = [handler3, handler2, handler1]
    selected = kopf.lifecycles.all_at_once(handlers)
    assert isinstance(selected, (tuple, list))
    assert len(selected) == 3
    assert list(selected) == handlers


def test_one_by_one_respects_order():
    handler1 = object()
    handler2 = object()
    handler3 = object()

    handlers = [handler1, handler2, handler3]
    selected = kopf.lifecycles.one_by_one(handlers)
    assert isinstance(selected, (tuple, list))
    assert len(selected) == 1
    assert selected[0] is handler1

    handlers = [handler3, handler2, handler1]
    selected = kopf.lifecycles.one_by_one(handlers)
    assert len(selected) == 1
    assert selected[0] is handler3


def test_randomized_takes_only_one():
    handler1 = object()
    handler2 = object()
    handler3 = object()

    handlers = [handler1, handler2, handler3]
    selected = kopf.lifecycles.randomized(handlers)
    assert isinstance(selected, (tuple, list))
    assert len(selected) == 1
    assert selected[0] in {handler1, handler2, handler3}


def test_shuffled_takes_them_all():
    handler1 = object()
    handler2 = object()
    handler3 = object()

    handlers = [handler1, handler2, handler3]
    selected = kopf.lifecycles.shuffled(handlers)
    assert isinstance(selected, (tuple, list))
    assert len(selected) == 3
    assert set(selected) == {handler1, handler2, handler3}


def test_asap_takes_the_first_one_when_no_retries(mocker):
    body = {}
    handler1 = mocker.Mock(id='id1', spec_set=['id'])
    handler2 = mocker.Mock(id='id2', spec_set=['id'])
    handler3 = mocker.Mock(id='id3', spec_set=['id'])

    handlers = [handler1, handler2, handler3]
    selected = kopf.lifecycles.asap(handlers, body=body)
    assert isinstance(selected, (tuple, list))
    assert len(selected) == 1
    assert selected[0] is handler1


def test_asap_takes_the_least_retried(mocker):
    body = {}
    handler1 = mocker.Mock(id='id1', spec_set=['id'])
    handler2 = mocker.Mock(id='id2', spec_set=['id'])
    handler3 = mocker.Mock(id='id3', spec_set=['id'])

    # Set the pre-existing state, and verify that it was set properly.
    set_retry_time(body=body, patch=body, handler=handler1)
    set_retry_time(body=body, patch=body, handler=handler1)
    set_retry_time(body=body, patch=body, handler=handler3)
    assert get_retry_count(body=body, handler=handler1) == 2
    assert get_retry_count(body=body, handler=handler2) == 0
    assert get_retry_count(body=body, handler=handler3) == 1

    handlers = [handler1, handler2, handler3]
    selected = kopf.lifecycles.asap(handlers, body=body)
    assert isinstance(selected, (tuple, list))
    assert len(selected) == 1
    assert selected[0] is handler2
