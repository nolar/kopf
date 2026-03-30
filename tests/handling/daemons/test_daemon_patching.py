import asyncio

import kopf


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
