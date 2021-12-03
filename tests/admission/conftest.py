import asyncio
import dataclasses
import gc
import warnings

import pyngrok.conf
import pyngrok.ngrok
import pytest

from kopf._cogs.structs.references import Insights, Resource
from kopf._cogs.structs.reviews import CreateOptions, Request, RequestKind, RequestPayload, \
                                       RequestResource, UserInfo, WebhookFn
from kopf._core.engines.indexing import OperatorIndexers
from kopf._kits.webhooks import WebhookServer


# TODO: LATER: Fix this issue some day later.
@pytest.fixture()
def no_serverside_resource_warnings():
    """
    Hide an irrelevant ResourceWarning on the server side:

    It happens when a client disconnects from the webhook server,
    and the server closes the transport for that client. The garbage
    collector calls ``__del__()`` on the SSL proto object, despite
    it is not close to the moment.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore',
                                category=ResourceWarning,
                                module='asyncio.sslproto',
                                message='unclosed transport')
        yield

        # Provoke the garbage collection of SSL sockets to trigger the warnings.
        # Otherwise, in PyPy, these warnings leak to other tests due to delayed gc.
        gc.collect()


# TODO: LATER: Fix this issue after aiohttp 4.0.0 is used.
@pytest.fixture()
async def no_clientside_resource_warnings():
    """
    Hide an irrelevant ResourceWarning on the client side.

    https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
    """
    yield
    await asyncio.sleep(0.100)


@pytest.fixture()
async def no_sslproto_warnings(no_serverside_resource_warnings, no_clientside_resource_warnings):
    pass


# cert generation is somewhat slow (~1s)
@pytest.fixture(scope='module')
def certpkey():
    cert, pkey = WebhookServer.build_certificate(['localhost', '127.0.0.1'])
    return cert, pkey


@pytest.fixture()
def certfile(tmpdir, certpkey):
    path = tmpdir.join('cert.pem')
    path.write_binary(certpkey[0])
    return str(path)


@pytest.fixture()
def pkeyfile(tmpdir, certpkey):
    path = tmpdir.join('pkey.pem')
    path.write_binary(certpkey[1])
    return str(path)


@pytest.fixture()
def adm_request(resource, namespace):
    return Request(
        apiVersion='admission.k8s.io/v1',
        kind='AdmissionReview',
        request=RequestPayload(
            uid='uid1',
            kind=RequestKind(group=resource.group, version=resource.version, kind=resource.kind),
            resource=RequestResource(group=resource.group, version=resource.version, resource=resource.plural),
            subResource=None,
            requestKind=RequestKind(group=resource.group, version=resource.version, kind=resource.kind),
            requestResource=RequestResource(group=resource.group, version=resource.version, resource=resource.plural),
            requestSubResource=None,
            userInfo=UserInfo(username='user1', uid='useruid1', groups=['group1']),
            name='name1',
            namespace=namespace,
            operation='CREATE',
            options=CreateOptions(apiVersion='meta.k8s.io/v1', kind='CreateOptions'),
            object={'spec': {'field': 'value'}},
            oldObject=None,
            dryRun=False,
        ))


@dataclasses.dataclass(frozen=True)
class Responder:
    fn: WebhookFn
    fut: asyncio.Future  # asyncio.Future[Response]


@pytest.fixture()
async def responder() -> Responder:
    fut = asyncio.Future()
    async def fn(*_, **__):
        return await fut
    return Responder(fn=fn, fut=fut)


@pytest.fixture()
async def insights(settings, resource):
    val_resource = Resource('admissionregistration.k8s.io', 'v1', 'validatingwebhookconfigurations')
    mut_resource = Resource('admissionregistration.k8s.io', 'v1', 'mutatingwebhookconfigurations')
    insights = Insights()
    insights.watched_resources.add(resource)
    insights.webhook_resources.add(resource)
    await insights.backbone.fill(resources=[val_resource, mut_resource])
    insights.ready_resources.set()
    return insights


@pytest.fixture()
def indices():
    indexers = OperatorIndexers()
    return indexers.indices


@pytest.fixture(autouse=True)
def pyngrok_mock(mocker):
    mocker.patch.object(pyngrok.conf, 'get_default')
    mocker.patch.object(pyngrok.ngrok, 'set_auth_token')
    mocker.patch.object(pyngrok.ngrok, 'connect')
    mocker.patch.object(pyngrok.ngrok, 'disconnect')
    pyngrok.ngrok.connect.return_value.public_url = 'https://nowhere'
    return pyngrok
