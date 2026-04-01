import asyncio

import kopf
from kopf._cogs.clients.errors import APIUnprocessableEntityError
from kopf._core.actions.execution import TemporaryError


async def test_daemon_patching_with_fns_only(
        settings, resource, dummy, k8s_mocked, simulate_cycle, looptime):
    executed = asyncio.Event()

    @kopf.daemon(*resource, id='fn')
    async def fn(patch, **kwargs):
        dummy.mock(**kwargs)
        patch.fns.append(lambda body: body.setdefault('status', {}).update({'json-patch': 'hello'}))
        executed.set()

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer], 'resourceVersion': '123'}}
    await simulate_cycle(event_object)
    await executed.wait()
    await dummy.wait_for_daemon_done()

    payloads = [call.kwargs['payload'] for call in k8s_mocked.patch.call_args_list]
    assert len(payloads) == 1
    assert payloads[0] == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '123'},
        {'op': 'add', 'path': '/status', 'value': {'json-patch': 'hello'}},
    ]


async def test_daemon_patching_with_merge_and_fns(
        settings, resource, dummy, k8s_mocked, simulate_cycle, looptime):
    executed = asyncio.Event()

    @kopf.daemon(*resource, id='fn')
    async def fn(patch, **kwargs):
        dummy.mock(**kwargs)
        patch.status['merge-patch'] = 'hello'
        patch.fns.append(lambda body: body.setdefault('status', {}).update({'json-patch': 'hello'}))
        executed.set()

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'finalizers': [finalizer], 'labels': {}}}
    await simulate_cycle(event_object)
    await executed.wait()
    await dummy.wait_for_daemon_done()

    payloads = [call.kwargs['payload'] for call in k8s_mocked.patch.call_args_list]
    assert len(payloads) == 2
    assert payloads[0] == {'status': {'merge-patch': 'hello'}}
    assert payloads[1] == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': None},
        {'op': 'add', 'path': '/status', 'value': {'json-patch': 'hello'}},
    ]


async def test_daemon_fns_cleared_between_iterations(
        settings, resource, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    """Each daemon iteration must carry only its own fns, not accumulated from prior iterations."""
    finished = asyncio.Event()

    @kopf.daemon(*resource, id='fn', backoff=1.0)
    async def fn(retry, patch: kopf.Patch, **kwargs):
        dummy.mock(**kwargs, retry=retry, patch=patch)
        n = dummy.mock.call_count
        patch.fns.append(lambda body, n=n: body.setdefault('status', {}).update({f'iter{n}': True}))
        if not retry:
            raise TemporaryError("retry me!", delay=1.0)
        finished.set()

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'resourceVersion': '123', 'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await finished.wait()
    await dummy.wait_for_daemon_done()

    assert dummy.mock.call_count == 2

    # If fns leak, the second retry's payload would contain operations from the first retry too.
    payloads = [call.kwargs['payload'] for call in k8s_mocked.patch.call_args_list]
    assert len(payloads) == 2
    assert payloads[0] == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '123'},
        {'op': 'add', 'path': '/status', 'value': {f'iter1': True}},
    ]
    assert payloads[1] == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '123'},
        {'op': 'add', 'path': '/status', 'value': {f'iter2': True}},
        # important: no "iter1" field here (as if when the fn would be preserved)!
    ]


async def test_daemon_fns_carried_forward_on_conflict(
        resource, settings, dummy, assert_logs, k8s_mocked, simulate_cycle, looptime):
    """Patch fns must be retried on the next retry after a 422 conflict."""
    finished = asyncio.Event()

    @kopf.daemon(*resource, id='fn', backoff=1.0)
    async def fn(retry, patch: kopf.Patch, **kwargs):
        dummy.mock(**kwargs, patch=patch)
        n = dummy.mock.call_count
        patch.fns.append(lambda body, n=n: body.setdefault('status', {}).update({f'iter{n}': True}))
        if not retry:
            raise TemporaryError("retry me!", delay=1.0)
        finished.set()

    # Fail the first JSON-patch call with 422 (version conflict), succeed on 2nd and so on.
    k8s_mocked.patch.side_effect = [
        APIUnprocessableEntityError(status=422, headers={}),  # 1st json-patch
        {'metadata': {'resourceVersion': '123'}},  # 2nd json-patch
    ]

    finalizer = settings.persistence.finalizer
    event_object = {'metadata': {'resourceVersion': '123', 'finalizers': [finalizer]}}
    await simulate_cycle(event_object)
    await finished.wait()
    await dummy.wait_for_daemon_done()

    assert dummy.mock.call_count == 2

    # The fn was added on retry 0, JSON-patch failed with 422, fn was carried forward.
    payloads = [call.kwargs['payload'] for call in k8s_mocked.patch.call_args_list]
    assert len(payloads) == 2
    assert payloads[0] == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '123'},
        {'op': 'add', 'path': '/status', 'value': {'iter1': True}},
    ]
    assert payloads[1] == [
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '123'},
        {'op': 'add', 'path': '/status', 'value': {'iter1': True, 'iter2': True}},
        # important: "iter1" is carried forward, despite only "iter2" is added normally.
    ]
