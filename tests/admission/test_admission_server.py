import pytest

import kopf
from kopf.reactor.admission import admission_webhook_server
from kopf.structs.primitives import Container


async def webhookfn(*_, **__):
    pass


async def test_requires_webserver_if_webhooks_are_defined(
        settings, registry, insights, resource, k8s_mocked):

    @kopf.on.validate(*resource, registry=registry)
    def fn_v(**_): ...

    @kopf.on.mutate(*resource, registry=registry)
    def fn_m(**_): ...

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
        settings, registry, insights, resource, k8s_mocked):

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
