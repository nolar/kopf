from kopf._core.reactor.running import operator


async def test_spawn_tasks_result_passed_to_run_tasks(mocker):
    sentinel = object()
    spawn = mocker.patch('kopf._core.reactor.running.spawn_tasks', return_value=sentinel)
    run = mocker.patch('kopf._core.reactor.running.run_tasks')

    await operator(clusterwide=True)

    assert spawn.await_count == 1
    assert run.await_count == 1
    assert run.call_args.args[0] is sentinel


async def test_existing_tasks_passed_as_ignored(mocker):
    existing = frozenset()
    mocker.patch('kopf._cogs.aiokits.aiotasks.all_tasks', return_value=existing)
    mocker.patch('kopf._core.reactor.running.spawn_tasks', return_value=[])
    run = mocker.patch('kopf._core.reactor.running.run_tasks')

    await operator(clusterwide=True)

    assert run.call_args.kwargs['ignored'] is existing


async def test_kwargs_forwarded_to_spawn_tasks(mocker):
    spawn = mocker.patch('kopf._core.reactor.running.spawn_tasks', return_value=[])
    mocker.patch('kopf._core.reactor.running.run_tasks')

    await operator(
        clusterwide=True,
        priority=100,
        peering_name='my-peering',
        standalone=True,
        liveness_endpoint="/health",
    )

    kwargs = spawn.call_args.kwargs
    assert kwargs['clusterwide'] is True
    assert kwargs['priority'] == 100
    assert kwargs['peering_name'] == 'my-peering'
    assert kwargs['standalone'] is True
    assert kwargs['liveness_endpoint'] == "/health"
