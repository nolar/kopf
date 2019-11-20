import enum
from typing import TypeVar, Optional, Union, Collection, List, Tuple, cast

import aiohttp

from kopf.clients import auth
from kopf.clients import discovery
from kopf.structs import bodies
from kopf.structs import resources

_T = TypeVar('_T')

CRD_CRD = resources.Resource('apiextensions.k8s.io', 'v1beta1', 'customresourcedefinitions')


class _UNSET(enum.Enum):
    token = enum.auto()


@auth.reauthenticated_request
async def read_crd(
        *,
        resource: resources.Resource,
        default: Union[_T, _UNSET] = _UNSET.token,
        session: Optional[auth.APISession] = None,  # injected by the decorator
) -> Union[bodies.Body, _T]:
    if session is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    try:
        response = await session.get(
            url=CRD_CRD.get_url(server=session.server, name=resource.name),
        )
        response.raise_for_status()
        respdata = await response.json()
        return cast(bodies.Body, respdata)

    except aiohttp.ClientResponseError as e:
        if e.status in [403, 404] and not isinstance(default, _UNSET):
            return default
        raise


@auth.reauthenticated_request
async def read_obj(
        *,
        resource: resources.Resource,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        default: Union[_T, _UNSET] = _UNSET.token,
        session: Optional[auth.APISession] = None,  # injected by the decorator
) -> Union[bodies.Body, _T]:
    if session is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    is_namespaced = await discovery.is_namespaced(resource=resource, session=session)
    namespace = namespace if is_namespaced else None

    try:
        response = await session.get(
            url=resource.get_url(server=session.server, namespace=namespace, name=name),
        )
        response.raise_for_status()
        respdata = await response.json()
        return cast(bodies.Body, respdata)

    except aiohttp.ClientResponseError as e:
        if e.status in [403, 404] and not isinstance(default, _UNSET):
            return default
        raise


@auth.reauthenticated_request
async def list_objs_rv(
        *,
        resource: resources.Resource,
        namespace: Optional[str] = None,
        session: Optional[auth.APISession] = None,  # injected by the decorator
) -> Tuple[Collection[bodies.Body], str]:
    """
    List the objects of specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes not sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.
    """
    if session is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    is_namespaced = await discovery.is_namespaced(resource=resource, session=session)
    namespace = namespace if is_namespaced else None

    response = await session.get(
        url=resource.get_url(server=session.server, namespace=namespace),
    )
    response.raise_for_status()
    rsp = await response.json()

    items: List[bodies.Body] = []
    resource_version = rsp.get('metadata', {}).get('resourceVersion', None)
    for item in rsp['items']:
        if 'kind' in rsp:
            item.setdefault('kind', rsp['kind'][:-4] if rsp['kind'][-4:] == 'List' else rsp['kind'])
        if 'apiVersion' in rsp:
            item.setdefault('apiVersion', rsp['apiVersion'])
        items.append(item)

    return items, resource_version
