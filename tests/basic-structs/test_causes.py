import pytest

from kopf._core.intents.causes import ActivityCause, ChangingCause, WatchingCause


@pytest.mark.parametrize('cls', [ActivityCause, WatchingCause, ChangingCause])
def test_cause_with_no_args(cls):
    with pytest.raises(TypeError):
        cls()


def test_activity_cause(mocker):
    memo = mocker.Mock()
    logger = mocker.Mock()
    indices = mocker.Mock()
    activity = mocker.Mock()
    settings = mocker.Mock()
    cause = ActivityCause(
        activity=activity,
        settings=settings,
        indices=indices,
        logger=logger,
        memo=memo,
    )
    assert cause.activity is activity
    assert cause.settings is settings
    assert cause.indices is indices
    assert cause.logger is logger
    assert cause.memo is memo


def test_watching_cause(mocker):
    logger = mocker.Mock()
    indices = mocker.Mock()
    resource = mocker.Mock()
    body = mocker.Mock()
    patch = mocker.Mock()
    memo = mocker.Mock()
    type = mocker.Mock()
    event = mocker.Mock()
    cause = WatchingCause(
        resource=resource,
        indices=indices,
        logger=logger,
        body=body,
        patch=patch,
        memo=memo,
        type=type,
        event=event,
    )
    assert cause.resource is resource
    assert cause.indices is indices
    assert cause.logger is logger
    assert cause.body is body
    assert cause.patch is patch
    assert cause.memo is memo
    assert cause.type is type
    assert cause.event is event


def test_changing_cause_with_all_args(mocker):
    logger = mocker.Mock()
    indices = mocker.Mock()
    resource = mocker.Mock()
    reason = mocker.Mock()
    initial = mocker.Mock()
    body = mocker.Mock()
    patch = mocker.Mock()
    memo = mocker.Mock()
    diff = mocker.Mock()
    old = mocker.Mock()
    new = mocker.Mock()
    cause = ChangingCause(
        resource=resource,
        indices=indices,
        logger=logger,
        reason=reason,
        initial=initial,
        body=body,
        patch=patch,
        memo=memo,
        diff=diff,
        old=old,
        new=new,
    )
    assert cause.resource is resource
    assert cause.indices is indices
    assert cause.logger is logger
    assert cause.reason is reason
    assert cause.initial is initial
    assert cause.body is body
    assert cause.patch is patch
    assert cause.memo is memo
    assert cause.diff is diff
    assert cause.old is old
    assert cause.new is new


def test_changing_cause_with_only_required_args(mocker):
    logger = mocker.Mock()
    indices = mocker.Mock()
    resource = mocker.Mock()
    reason = mocker.Mock()
    initial = mocker.Mock()
    body = mocker.Mock()
    patch = mocker.Mock()
    memo = mocker.Mock()
    cause = ChangingCause(
        resource=resource,
        indices=indices,
        logger=logger,
        reason=reason,
        initial=initial,
        body=body,
        patch=patch,
        memo=memo,
    )
    assert cause.resource is resource
    assert cause.indices is indices
    assert cause.logger is logger
    assert cause.reason is reason
    assert cause.initial is initial
    assert cause.body is body
    assert cause.patch is patch
    assert cause.memo is memo
    assert cause.diff is not None
    assert not cause.diff
    assert cause.old is None
    assert cause.new is None
