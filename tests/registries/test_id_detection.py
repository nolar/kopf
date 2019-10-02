import functools

import pytest

from kopf import SimpleRegistry
from kopf.reactor.registries import get_callable_id


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn():
    pass


@pytest.fixture(params=[
    'some-field.sub-field',
    ['some-field', 'sub-field'],
    ('some-field', 'sub-field'),
], ids=['str', 'list', 'tuple'])
def field(request):
    return request.param


def test_id_of_simple_function():
    fn_id = get_callable_id(some_fn)
    assert fn_id == 'some_fn'


def test_id_of_single_partial():
    partial_fn = functools.partial(some_fn)

    fn_id = get_callable_id(partial_fn)
    assert fn_id == 'some_fn'


def test_id_of_double_partial():
    partial1_fn = functools.partial(some_fn)
    partial2_fn = functools.partial(partial1_fn)

    fn_id = get_callable_id(partial2_fn)
    assert fn_id == 'some_fn'


def test_id_of_single_wrapper():

    @functools.wraps(some_fn)
    def wrapped_fn():
        pass

    fn_id = get_callable_id(wrapped_fn)
    assert fn_id == 'some_fn'


def test_id_of_double_wrapper():

    @functools.wraps(some_fn)
    def wrapped1_fn():
        pass

    @functools.wraps(wrapped1_fn)
    def wrapped2_fn():
        pass

    fn_id = get_callable_id(wrapped2_fn)
    assert fn_id == 'some_fn'


def test_id_of_lambda():
    some_lambda = lambda: None

    fn_id = get_callable_id(some_lambda)
    assert fn_id.startswith(f'lambda:{__file__}:')


def test_with_no_hints(mocker):
    get_fn_id = mocker.patch('kopf.reactor.registries.get_callable_id', return_value='some-id')

    registry = SimpleRegistry()
    registry.register(some_fn)
    handlers = registry.get_cause_handlers(mocker.MagicMock())

    assert get_fn_id.called

    assert len(handlers) == 1
    assert handlers[0].fn is some_fn
    assert handlers[0].id == 'some-id'


def test_with_prefix(mocker):
    get_fn_id = mocker.patch('kopf.reactor.registries.get_callable_id', return_value='some-id')

    registry = SimpleRegistry(prefix='some-prefix')
    registry.register(some_fn)
    handlers = registry.get_cause_handlers(mocker.MagicMock())

    assert get_fn_id.called

    assert len(handlers) == 1
    assert handlers[0].fn is some_fn
    assert handlers[0].id == 'some-prefix/some-id'


def test_with_suffix(mocker, field):
    get_fn_id = mocker.patch('kopf.reactor.registries.get_callable_id', return_value='some-id')
    diff = [('add', ('some-field', 'sub-field'), 'old', 'new')]

    registry = SimpleRegistry()
    registry.register(some_fn, field=field)
    handlers = registry.get_cause_handlers(mocker.MagicMock(diff=diff))

    assert get_fn_id.called

    assert len(handlers) == 1
    assert handlers[0].fn is some_fn
    assert handlers[0].id == 'some-id/some-field.sub-field'


def test_with_prefix_and_suffix(mocker, field):
    get_fn_id = mocker.patch('kopf.reactor.registries.get_callable_id', return_value='some-id')
    diff = [('add', ('some-field', 'sub-field'), 'old', 'new')]

    registry = SimpleRegistry(prefix='some-prefix')
    registry.register(some_fn, field=field)
    handlers = registry.get_cause_handlers(mocker.MagicMock(diff=diff))

    assert get_fn_id.called

    assert len(handlers) == 1
    assert handlers[0].fn is some_fn
    assert handlers[0].id == 'some-prefix/some-id/some-field.sub-field'


def test_with_explicit_id_and_prefix_and_suffix(mocker, field):
    get_fn_id = mocker.patch('kopf.reactor.registries.get_callable_id', return_value='some-id')
    diff = [('add', ('some-field', 'sub-field'), 'old', 'new')]

    registry = SimpleRegistry(prefix='some-prefix')
    registry.register(some_fn, id='explicit-id', field=field)
    handlers = registry.get_cause_handlers(mocker.MagicMock(diff=diff))

    assert not get_fn_id.called

    assert len(handlers) == 1
    assert handlers[0].fn is some_fn
    assert handlers[0].id == 'some-prefix/explicit-id/some-field.sub-field'
