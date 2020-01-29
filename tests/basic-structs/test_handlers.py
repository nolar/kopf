import pytest

from kopf.reactor.handlers import ActivityHandler, ResourceHandler


@pytest.mark.parametrize('cls', [ActivityHandler, ResourceHandler])
def test_handler_with_no_args(cls):
    with pytest.raises(TypeError):
        cls()


def test_activity_handler_with_all_args(mocker):
    fn = mocker.Mock()
    id = mocker.Mock()
    errors = mocker.Mock()
    timeout = mocker.Mock()
    retries = mocker.Mock()
    backoff = mocker.Mock()
    activity = mocker.Mock()
    handler = ActivityHandler(
        fn=fn,
        id=id,
        errors=errors,
        timeout=timeout,
        retries=retries,
        backoff=backoff,
        cooldown=None,  # deprecated, but still required
        activity=activity,
    )
    assert handler.fn is fn
    assert handler.id is id
    assert handler.errors is errors
    assert handler.timeout is timeout
    assert handler.retries is retries
    assert handler.backoff is backoff
    assert handler.cooldown is backoff  # deprecated alias
    assert handler.activity is activity


def test_resource_handler_with_all_args(mocker):
    fn = mocker.Mock()
    id = mocker.Mock()
    reason = mocker.Mock()
    field = mocker.Mock()
    errors = mocker.Mock()
    timeout = mocker.Mock()
    retries = mocker.Mock()
    backoff = mocker.Mock()
    initial = mocker.Mock()
    labels = mocker.Mock()
    annotations = mocker.Mock()
    requires_finalizer = mocker.Mock()
    handler = ResourceHandler(
        fn=fn,
        id=id,
        reason=reason,
        field=field,
        errors=errors,
        timeout=timeout,
        retries=retries,
        backoff=backoff,
        cooldown=None,  # deprecated, but still required
        initial=initial,
        labels=labels,
        annotations=annotations,
        requires_finalizer=requires_finalizer,
    )
    assert handler.fn is fn
    assert handler.id is id
    assert handler.reason is reason
    assert handler.event is reason  # deprecated
    assert handler.field is field
    assert handler.errors is errors
    assert handler.timeout is timeout
    assert handler.retries is retries
    assert handler.backoff is backoff
    assert handler.cooldown is backoff  # deprecated alias
    assert handler.initial is initial
    assert handler.labels is labels
    assert handler.annotations is annotations
    assert handler.requires_finalizer is requires_finalizer
