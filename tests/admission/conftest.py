import asyncio
import dataclasses
from unittest.mock import Mock

import pytest

from kopf.reactor.indexing import OperatorIndexers
from kopf.structs.references import Insights, Resource
from kopf.structs.reviews import CreateOptions, Request, RequestKind, RequestPayload, \
                                 RequestResource, UserInfo, WebhookFn


@pytest.fixture()
def adm_request(resource):
    return Request(
        apiVersion='admission.k8s.io/v1',
        kind='AdmissionReview',
        request=RequestPayload(
            uid='uid1',
            kind=RequestKind(group=resource.group, version=resource.version, kind=resource.kind),
            resource=RequestResource(group=resource.group, version=resource.version, resource=resource.plural),
            requestKind=RequestKind(group=resource.group, version=resource.version, kind=resource.kind),
            requestResource=RequestResource(group=resource.group, version=resource.version, resource=resource.plural),
            userInfo=UserInfo(username='user1', uid='useruid1', groups=['group1']),
            name='name1',
            namespace='ns1',
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
    insights.resources.add(resource)
    await insights.backbone.fill(resources=[val_resource, mut_resource])
    insights.ready_resources.set()
    return insights


@pytest.fixture()
def indices():
    indexers = OperatorIndexers()
    return indexers.indices


@dataclasses.dataclass(frozen=True, eq=False)
class K8sMocks:
    patch_obj: Mock
    create_obj: Mock
    post_event: Mock
    sleep_or_wait: Mock


@pytest.fixture(autouse=True)
def k8s_mocked(mocker):
    # We mock on the level of our own K8s API wrappers, not the K8s client.
    return K8sMocks(
        patch_obj=mocker.patch('kopf.clients.patching.patch_obj', return_value={}),
        create_obj=mocker.patch('kopf.clients.creating.create_obj', return_value={}),
        post_event=mocker.patch('kopf.clients.events.post_event'),
        sleep_or_wait=mocker.patch('kopf.structs.primitives.sleep_or_wait', return_value=None),
    )
