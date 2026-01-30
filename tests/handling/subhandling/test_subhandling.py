import asyncio
from unittest.mock import Mock

import pytest

import kopf
from kopf._cogs.structs.ephemera import Memo
from kopf._core.engines.indexing import OperatorIndexers
from kopf._core.intents.causes import Reason
from kopf._core.reactor.inventory import ResourceMemories
from kopf._core.reactor.processing import process_resource_event

EVENT_TYPES_WHEN_EXISTS = [None, 'ADDED', 'MODIFIED']


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_1st_level(registry, settings, resource, cause_mock, event_type,
                         assert_logs, k8s_mocked, looptime):
    cause_mock.reason = Reason.CREATE

    fn_mock = Mock(return_value=None)
    sub1a_mock = Mock(return_value=None)
    sub1b_mock = Mock(return_value=None)

    # Only to justify the finalizer. See: cause_mock, which adds finalizers always.
    # TODO: get rid of mocks, test it normally.
    @kopf.on.delete('kopfexamples', id='del')
    async def _del(**_):
        pass

    @kopf.on.create('kopfexamples', id='fn')
    async def fn(**kwargs):
        fn_mock(**kwargs)
        @kopf.subhandler(id='sub1a')
        async def sub1a(**kwargs):
            sub1a_mock(**kwargs)
        @kopf.subhandler(id='sub1b')
        async def sub1b(**_):
            sub1b_mock(**kwargs)

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=event_queue,
    )

    assert fn_mock.call_count == 1
    assert sub1a_mock.call_count == 1
    assert sub1b_mock.call_count == 1

    assert looptime == 0
    assert k8s_mocked.patch.call_count == 1
    assert not event_queue.empty()

    assert_logs([
        "Creation is in progress:",
        "Handler 'fn' is invoked",
        "Handler 'fn/sub1a' is invoked",
        "Handler 'fn/sub1a' succeeded",
        "Handler 'fn/sub1b' is invoked",
        "Handler 'fn/sub1b' succeeded",
        "Handler 'fn' succeeded",
        "Creation is processed",
        "Patching with",
    ])


@pytest.mark.parametrize('event_type', EVENT_TYPES_WHEN_EXISTS)
async def test_2nd_level(registry, settings, resource, cause_mock, event_type,
                         assert_logs, k8s_mocked, looptime):
    cause_mock.reason = Reason.CREATE

    fn_mock = Mock(return_value=None)
    sub1a_mock = Mock(return_value=None)
    sub1b_mock = Mock(return_value=None)
    sub1a2a_mock = Mock(return_value=None)
    sub1a2b_mock = Mock(return_value=None)
    sub1b2a_mock = Mock(return_value=None)
    sub1b2b_mock = Mock(return_value=None)

    # Only to justify the finalizer. See: cause_mock, which adds finalizers always.
    # TODO: get rid of mocks, test it normally.
    @kopf.on.delete('kopfexamples', id='del')
    async def _del(**_):
        pass

    @kopf.on.create(*resource, id='fn')
    def fn(**kwargs):
        fn_mock(**kwargs)
        @kopf.subhandler(id='sub1a')
        def sub1a(**kwargs):
            sub1a_mock(**kwargs)
            @kopf.subhandler(id='sub1a2a')
            def sub1a2a(**kwargs):
                sub1a2a_mock(**kwargs)
            @kopf.subhandler(id='sub1a2b')
            def sub1a2b(**kwargs):
                sub1a2b_mock(**kwargs)
        @kopf.subhandler(id='sub1b')
        def sub1b(**kwargs):
            sub1b_mock(**kwargs)
            @kopf.subhandler(id='sub1b2a')
            def sub1b2a(**kwargs):
                sub1b2a_mock(**kwargs)
            @kopf.subhandler(id='sub1b2b')
            def sub1b2b(**kwargs):
                sub1b2b_mock(**kwargs)

    event_queue = asyncio.Queue()
    await process_resource_event(
        lifecycle=kopf.lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        resource=resource,
        indexers=OperatorIndexers(),
        memories=ResourceMemories(),
        memobase=Memo(),
        raw_event={'type': event_type, 'object': {}},
        event_queue=event_queue,
    )

    assert fn_mock.call_count == 1
    assert sub1a_mock.call_count == 1
    assert sub1b_mock.call_count == 1
    assert sub1a2a_mock.call_count == 1
    assert sub1a2b_mock.call_count == 1
    assert sub1b2a_mock.call_count == 1
    assert sub1b2b_mock.call_count == 1

    assert looptime == 0
    assert k8s_mocked.patch.call_count == 1
    assert not event_queue.empty()

    assert_logs([
        "Creation is in progress:",
        "Handler 'fn' is invoked",
        "Handler 'fn/sub1a' is invoked",
        "Handler 'fn/sub1a/sub1a2a' is invoked",
        "Handler 'fn/sub1a/sub1a2a' succeeded",
        "Handler 'fn/sub1a/sub1a2b' is invoked",
        "Handler 'fn/sub1a/sub1a2b' succeeded",
        "Handler 'fn/sub1a' succeeded",
        "Handler 'fn/sub1b' is invoked",
        "Handler 'fn/sub1b/sub1b2a' is invoked",
        "Handler 'fn/sub1b/sub1b2a' succeeded",
        "Handler 'fn/sub1b/sub1b2b' is invoked",
        "Handler 'fn/sub1b/sub1b2b' succeeded",
        "Handler 'fn/sub1b' succeeded",
        "Handler 'fn' succeeded",
        "Creation is processed",
        "Patching with",
    ])
