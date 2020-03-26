# Original test-file: tests/basic-structs/test_handlers.py
import pytest

from kopf.structs.handlers import ActivityHandler, ResourceChangingHandler


def test_activity_handler_with_deprecated_cooldown_instead_of_backoff(mocker):
    fn = mocker.Mock()
    id = mocker.Mock()
    errors = mocker.Mock()
    timeout = mocker.Mock()
    retries = mocker.Mock()
    backoff = mocker.Mock()
    activity = mocker.Mock()

    with pytest.deprecated_call(match=r"use backoff="):
        handler = ActivityHandler(
            fn=fn,
            id=id,
            errors=errors,
            timeout=timeout,
            retries=retries,
            backoff=None,
            cooldown=backoff,  # deprecated, but still required
            activity=activity,
        )

    assert handler.fn is fn
    assert handler.id is id
    assert handler.errors is errors
    assert handler.timeout is timeout
    assert handler.retries is retries
    assert handler.backoff is backoff
    assert handler.activity is activity

    with pytest.deprecated_call(match=r"use handler.backoff"):
        assert handler.cooldown is backoff


def test_resource_handler_with_deprecated_cooldown_instead_of_backoff(mocker):
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

    with pytest.deprecated_call(match=r"use backoff="):
        handler = ResourceChangingHandler(
            fn=fn,
            id=id,
            reason=reason,
            field=field,
            errors=errors,
            timeout=timeout,
            retries=retries,
            backoff=None,
            cooldown=backoff,  # deprecated, but still required
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
