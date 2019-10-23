import pytest

from kopf.reactor.causation import ResourceChangingCause


def test_no_args():
    with pytest.raises(TypeError):
        ResourceChangingCause()


def test_all_args(mocker):
    logger = mocker.Mock()
    resource = mocker.Mock()
    reason = mocker.Mock()
    initial = mocker.Mock()
    body = mocker.Mock()
    patch = mocker.Mock()
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
        diff=diff,
        old=old,
        new=new,
    )
    assert cause.resource is resource
    assert cause.logger is logger
    assert cause.reason is reason
    assert cause.event is reason  # deprecated
    assert cause.initial is initial
    assert cause.body is body
    assert cause.patch is patch
    assert cause.diff is diff
    assert cause.old is old
    assert cause.new is new


def test_required_args(mocker):
    logger = mocker.Mock()
    resource = mocker.Mock()
    reason = mocker.Mock()
    initial = mocker.Mock()
    body = mocker.Mock()
    patch = mocker.Mock()
    cause = ResourceChangingCause(
        resource=resource,
        logger=logger,
        reason=reason,
        initial=initial,
        body=body,
        patch=patch,
    )
    assert cause.resource is resource
    assert cause.logger is logger
    assert cause.reason is reason
    assert cause.event is reason  # deprecated
    assert cause.initial is initial
    assert cause.body is body
    assert cause.patch is patch
    assert cause.diff is not None
    assert not cause.diff
    assert cause.old is None
    assert cause.new is None
