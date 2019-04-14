import pytest

from kopf.reactor.registry import Handler


def test_no_args():
    with pytest.raises(TypeError):
        Handler()


def test_all_args(mocker):
    fn = mocker.Mock()
    id = mocker.Mock()
    event = mocker.Mock()
    field = mocker.Mock()
    timeout  = mocker.Mock()
    handler = Handler(
        fn=fn,
        id=id,
        event=event,
        field=field,
        timeout=timeout,
    )
    assert handler.fn is fn
    assert handler.id is id
    assert handler.event is event
    assert handler.field is field
    assert handler.timeout is timeout
