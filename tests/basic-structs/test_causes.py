import pytest

from kopf.reactor.causation import ActivityCause, ResourceChangingCause, ResourceWatchingCause


@pytest.mark.parametrize('cls', [ActivityCause, ResourceWatchingCause, ResourceChangingCause])
def test_cause_with_no_args(cls):
    with pytest.raises(TypeError):
        cls()


def test_activity_cause(mocker):
    logger = mocker.Mock()
    activity = mocker.Mock()
    settings = mocker.Mock()
    cause = ActivityCause(
        activity=activity,
        settings=settings,
        logger=logger,
    )
    assert cause.activity is activity
    assert cause.settings is settings
    assert cause.logger is logger


def test_resource_watching_cause(mocker):
    logger = mocker.Mock()
    resource = mocker.Mock()
    body = mocker.Mock()
    patch = mocker.Mock()
    memo = mocker.Mock()
    type = mocker.Mock()
    raw = mocker.Mock()
    cause = ResourceWatchingCause(
        resource=resource,
        logger=logger,
        body=body,
        patch=patch,
        memo=memo,
        type=type,
        raw=raw,
    )
    assert cause.resource is resource
    assert cause.logger is logger
    assert cause.body is body
    assert cause.patch is patch
    assert cause.memo is memo
    assert cause.type is type
    assert cause.raw is raw


def test_resource_changing_cause_with_all_args(mocker):
    logger = mocker.Mock()
    resource = mocker.Mock()
    reason = mocker.Mock()
    initial = mocker.Mock()
    body = mocker.Mock()
    patch = mocker.Mock()
    memo = mocker.Mock()
    diff = mocker.Mock()
    old = mocker.Mock()
    new = mocker.Mock()
    cause = ResourceChangingCause(
        resource=resource,
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
    assert cause.logger is logger
    assert cause.reason is reason
    assert cause.initial is initial
    assert cause.body is body
    assert cause.patch is patch
    assert cause.memo is memo
    assert cause.diff is diff
    assert cause.old is old
    assert cause.new is new


def test_resource_changing_cause_with_only_required_args(mocker):
    logger = mocker.Mock()
    resource = mocker.Mock()
    reason = mocker.Mock()
    initial = mocker.Mock()
    body = mocker.Mock()
    patch = mocker.Mock()
    memo = mocker.Mock()
    cause = ResourceChangingCause(
        resource=resource,
        logger=logger,
        reason=reason,
        initial=initial,
        body=body,
        patch=patch,
        memo=memo,
    )
    assert cause.resource is resource
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
