import pytest

from kopf.engines.peering import Peer, keepalive


class StopInfiniteCycleException(Exception):
    pass


async def test_background_task_runs(mocker, settings):
    touch_mock = mocker.patch('kopf.engines.peering.touch')

    sleep_mock = mocker.patch('asyncio.sleep')
    sleep_mock.side_effect = [None, None, StopInfiniteCycleException]

    settings.peering.lifetime = 33
    with pytest.raises(StopInfiniteCycleException):
        await keepalive(settings=settings, identity='id', namespace='namespace')

    assert sleep_mock.call_count == 3
    assert sleep_mock.call_args_list[0][0][0] == 33 - 10
    assert sleep_mock.call_args_list[1][0][0] == 33 - 10
    assert sleep_mock.call_args_list[2][0][0] == 33 - 10

    assert touch_mock.call_count == 4  # 3 updates + 1 clean-up
