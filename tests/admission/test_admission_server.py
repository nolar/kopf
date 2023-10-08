import contextlib
from unittest.mock import Mock

import pytest

import kopf
from kopf._cogs.aiokits.aiovalues import Container
from kopf._core.engines.admission import admission_webhook_server


async def webhookfn(*_, **__):
    pass


async def test_requires_webserver_if_webhooks_are_defined(
        settings, registry, insights, resource):

    @kopf.on.validate(*resource, registry=registry)
    def fn_v(**_): pass

    @kopf.on.mutate(*resource, registry=registry)
    def fn_m(**_): pass

    container = Container()
    with pytest.raises(Exception) as err:
        settings.admission.server = None
        await admission_webhook_server(
            registry=registry,
            settings=settings,
            insights=insights,
            container=container,
            webhookfn=webhookfn,
        )

    assert "Admission handlers exist, but no admission server/tunnel" in str(err.value)


async def test_configures_client_configs(
        settings, registry, insights):

    async def server(_):
        yield {'url': 'https://hostname/'}

    container = Container()
    settings.admission.server = server
    await admission_webhook_server(
        registry=registry,
        settings=settings,
        insights=insights,
        container=container,
        webhookfn=webhookfn,
    )

    assert container.get_nowait() == {'url': 'https://hostname/'}


async def test_contextmanager_class(
        settings, registry, insights):

    aenter_mock = Mock()
    aexit_mock = Mock()
    container = Container()

    class CtxMgrServer:
        async def __aenter__(self) -> "CtxMgrServer":
            aenter_mock()
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
            aexit_mock()

        async def __call__(self, _):
            yield {'url': 'https://hostname/'}

    settings.admission.server = CtxMgrServer()
    await admission_webhook_server(
        registry=registry,
        settings=settings,
        insights=insights,
        container=container,
        webhookfn=webhookfn,
    )

    assert aenter_mock.call_count == 1
    assert aexit_mock.call_count == 1
    assert container.get_nowait() == {'url': 'https://hostname/'}


async def test_contextmanager_decorator(
        settings, registry, insights):

    aenter_mock = Mock()
    aexit_mock = Mock()
    container = Container()

    async def server(_):
        yield {'url': 'https://hostname/'}

    @contextlib.asynccontextmanager
    async def server_manager():
        aenter_mock()
        try:
            yield server
        finally:
            aexit_mock()

    settings.admission.server = server_manager()  # can be entered only once
    await admission_webhook_server(
        registry=registry,
        settings=settings,
        insights=insights,
        container=container,
        webhookfn=webhookfn,
    )

    assert aenter_mock.call_count == 1
    assert aexit_mock.call_count == 1
    assert container.get_nowait() == {'url': 'https://hostname/'}
