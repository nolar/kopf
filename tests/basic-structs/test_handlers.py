import pytest

from kopf.structs.handlers import ActivityHandler, ResourceChangingHandler


@pytest.mark.parametrize('cls', [ActivityHandler, ResourceChangingHandler])
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
    deleted = mocker.Mock()
    labels = mocker.Mock()
    annotations = mocker.Mock()
    when = mocker.Mock()
    requires_finalizer = mocker.Mock()
    handler = ResourceChangingHandler(
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
        deleted=deleted,
        labels=labels,
        annotations=annotations,
        when=when,
        requires_finalizer=requires_finalizer,
    )
    assert handler.fn is fn
    assert handler.id is id
    assert handler.reason is reason
    assert handler.field is field
    assert handler.errors is errors
    assert handler.timeout is timeout
    assert handler.retries is retries
    assert handler.backoff is backoff
    assert handler.initial is initial
    assert handler.deleted is deleted
    assert handler.labels is labels
    assert handler.annotations is annotations
    assert handler.when is when
    assert handler.requires_finalizer is requires_finalizer

    with pytest.deprecated_call(match=r"use handler.reason"):
        assert handler.event is reason

    with pytest.deprecated_call(match=r"use handler.backoff"):
        assert handler.cooldown is backoff
