import pytest

from kopf.reactor.handling import Cause


def test_no_args():
    with pytest.raises(TypeError):
        Cause()


def test_all_args(mocker):
    logger = mocker.Mock()
    resource = mocker.Mock()
    event = mocker.Mock()
    body = mocker.Mock()
    patch = mocker.Mock()
    diff = mocker.Mock()
    old = mocker.Mock()
    new = mocker.Mock()
    cause = Cause(
        resource=resource,
        logger=logger,
        event=event,
        body=body,
        patch=patch,
        diff=diff,
        old=old,
        new=new,
    )
    assert cause.resource is resource
    assert cause.logger is logger
    assert cause.event is event
    assert cause.body is body
    assert cause.patch is patch
    assert cause.diff is diff
    assert cause.old is old
    assert cause.new is new


def test_required_args(mocker):
    logger = mocker.Mock()
    resource = mocker.Mock()
    event = mocker.Mock()
    body = mocker.Mock()
    patch = mocker.Mock()
    cause = Cause(
        resource=resource,
        logger=logger,
        event=event,
        body=body,
        patch=patch,
    )
    assert cause.resource is resource
    assert cause.logger is logger
    assert cause.event is event
    assert cause.body is body
    assert cause.patch is patch
    assert cause.diff is None
    assert cause.old is None
    assert cause.new is None
