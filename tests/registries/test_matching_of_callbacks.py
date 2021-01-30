import dataclasses
from unittest.mock import Mock

import pytest

from kopf.reactor.causation import ResourceWatchingCause
from kopf.reactor.registries import match, prematch
from kopf.structs.bodies import Body
from kopf.structs.dicts import parse_field
from kopf.structs.handlers import ResourceWatchingHandler
from kopf.structs.references import Resource


# Used in the tests. Must be global-scoped, or its qualname will be affected.
def some_fn(x=None):
    pass


@pytest.fixture()
def callback():
    mock = Mock()
    mock.return_value = True
    return mock


@pytest.fixture(params=['annotations', 'labels', 'value', 'when'])
def handler(request, callback, selector):
    handler = ResourceWatchingHandler(
        selector=selector,
        annotations={'known': 'value'},
        labels={'known': 'value'},
        field=parse_field('spec.field'),
        value='value',
        when=None,
        fn=some_fn, id='a', param=None, errors=None, timeout=None, retries=None, backoff=None,
    )
    if request.param in ['annotations', 'labels']:
        handler = dataclasses.replace(handler, **{request.param: {'known': callback}})
    else:
        handler = dataclasses.replace(handler, **{request.param: callback})
    return handler


@pytest.fixture()
def cause(cause_factory, callback):
    return cause_factory(
        cls=ResourceWatchingCause,
        body=Body(dict(
            metadata=dict(
                labels={'known': 'value'},
                annotations={'known': 'value'},
            ),
            spec=dict(
                field='value',
            ),
        )))


@pytest.mark.parametrize('match_fn', [match, prematch])
def test_callback_is_called_with_matching_resource(
        match_fn, callback, handler, cause,
):
    result = match_fn(handler=handler, cause=cause)
    assert result
    assert callback.called


@pytest.mark.parametrize('match_fn', [match, prematch])
def test_callback_is_not_called_with_mismatching_resource(
        match_fn, callback, handler, cause,
):
    cause = dataclasses.replace(cause, resource=Resource(group='x', version='y', plural='z'))
    result = match_fn(handler=handler, cause=cause)
    assert not result
    assert not callback.called
