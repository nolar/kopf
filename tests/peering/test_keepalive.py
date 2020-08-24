import pytest

from kopf.engines.peering import Peer, peers_keepalive


class StopInfiniteCycleException(Exception):
    pass


async def test_background_task_runs(mocker):
    ourselves = Peer(id='id', name='name', namespace='namespace', lifetime=33)
    keepalive_mock = mocker.patch.object(ourselves, 'keepalive')
    disappear_mock = mocker.patch.object(ourselves, 'disappear')

    sleep_mock = mocker.patch('asyncio.sleep')
    sleep_mock.side_effect = [None, None, StopInfiniteCycleException]

    with pytest.raises(StopInfiniteCycleException):
        await peers_keepalive(ourselves=ourselves)

    assert sleep_mock.call_count == 3
    assert sleep_mock.call_args_list[0][0][0] == 33 - 10
    assert sleep_mock.call_args_list[1][0][0] == 33 - 10
    assert sleep_mock.call_args_list[2][0][0] == 33 - 10

    assert keepalive_mock.call_count == 3
    assert disappear_mock.call_count == 1
