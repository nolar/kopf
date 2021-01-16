import pytest

from kopf.engines.peering import keepalive


class StopInfiniteCycleException(Exception):
    pass


async def test_background_task_runs(mocker, settings, namespaced_peering_resource):
    touch_mock = mocker.patch('kopf.engines.peering.touch')

    sleep_mock = mocker.patch('asyncio.sleep')
    sleep_mock.side_effect = [None, None, StopInfiniteCycleException]

    randint_mock = mocker.patch('random.randint')
    randint_mock.side_effect = [7, 5, 9]

    settings.peering.lifetime = 33
    with pytest.raises(StopInfiniteCycleException):
        await keepalive(settings=settings, identity='id',
                        resource=namespaced_peering_resource, namespace='namespace')

    assert randint_mock.call_count == 3  # only to be sure that we test the right thing
    assert sleep_mock.call_count == 3
    assert sleep_mock.call_args_list[0][0][0] == 33 - 7
    assert sleep_mock.call_args_list[1][0][0] == 33 - 5
    assert sleep_mock.call_args_list[2][0][0] == 33 - 9

    assert touch_mock.call_count == 4  # 3 updates + 1 clean-up
