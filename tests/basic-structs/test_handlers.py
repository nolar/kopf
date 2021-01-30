import pytest

from kopf.structs.handlers import ActivityHandler, ResourceChangingHandler


@pytest.mark.parametrize('cls', [ActivityHandler, ResourceChangingHandler])
def test_handler_with_no_args(cls):
    with pytest.raises(TypeError):
        cls()


def test_activity_handler_with_all_args(mocker):
    fn = mocker.Mock()
    id = mocker.Mock()
    param = mocker.Mock()
    errors = mocker.Mock()
    timeout = mocker.Mock()
    retries = mocker.Mock()
    backoff = mocker.Mock()
    activity = mocker.Mock()
    handler = ActivityHandler(
        fn=fn,
        id=id,
        param=param,
        errors=errors,
        timeout=timeout,
        retries=retries,
        backoff=backoff,
        activity=activity,
    )
    assert handler.fn is fn
    assert handler.id is id
    assert handler.param is param
    assert handler.errors is errors
    assert handler.timeout is timeout
    assert handler.retries is retries
    assert handler.backoff is backoff
    assert handler.activity is activity


def test_resource_handler_with_all_args(mocker):
    fn = mocker.Mock()
    id = mocker.Mock()
    param = mocker.Mock()
    selector = mocker.Mock()
    reason = mocker.Mock()
    errors = mocker.Mock()
    timeout = mocker.Mock()
    retries = mocker.Mock()
    backoff = mocker.Mock()
    initial = mocker.Mock()
    deleted = mocker.Mock()
    labels = mocker.Mock()
    annotations = mocker.Mock()
    when = mocker.Mock()
    field = mocker.Mock()
    value = mocker.Mock()
    old = mocker.Mock()
    new = mocker.Mock()
    field_needs_change = mocker.Mock()
    requires_finalizer = mocker.Mock()
    handler = ResourceChangingHandler(
        fn=fn,
        id=id,
        param=param,
        selector=selector,
        reason=reason,
        errors=errors,
        timeout=timeout,
        retries=retries,
        backoff=backoff,
        initial=initial,
        deleted=deleted,
        labels=labels,
        annotations=annotations,
        when=when,
        field=field,
        value=value,
        old=old,
        new=new,
        field_needs_change=field_needs_change,
        requires_finalizer=requires_finalizer,
    )
    assert handler.fn is fn
    assert handler.id is id
    assert handler.param is param
    assert handler.selector is selector
    assert handler.reason is reason
    assert handler.errors is errors
    assert handler.timeout is timeout
    assert handler.retries is retries
    assert handler.backoff is backoff
    assert handler.initial is initial
    assert handler.deleted is deleted
    assert handler.labels is labels
    assert handler.annotations is annotations
    assert handler.when is when
    assert handler.field is field
    assert handler.value is value
    assert handler.old is old
    assert handler.new is new
    assert handler.field_needs_change is field_needs_change
    assert handler.requires_finalizer is requires_finalizer
