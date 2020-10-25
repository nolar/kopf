import enum
from typing import Collection, List, Optional, Tuple, TypeVar, Union, cast

from kopf.clients import auth, discovery, errors
from kopf.structs import bodies, references

_T = TypeVar('_T')

CRD_CRD = references.Resource('apiextensions.k8s.io', 'v1beta1', 'customresourcedefinitions')


class _UNSET(enum.Enum):
    token = enum.auto()


@auth.reauthenticated_request
async def read_obj(
        *,
        resource: references.Resource,
        namespace: Optional[str],
        name: Optional[str],
        default: Union[_T, _UNSET] = _UNSET.token,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Union[bodies.RawBody, _T]:
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    is_namespaced = await discovery.is_namespaced(resource=resource, context=context)
    namespace = namespace if is_namespaced else None

    try:
        url = resource.get_url(server=context.server, namespace=namespace, name=name)
        rsp = await errors.parse_response(await context.session.get(url))
        return cast(bodies.RawBody, rsp)

    except (errors.APINotFoundError, errors.APIForbiddenError):
        if not isinstance(default, _UNSET):
            return default
        raise


@auth.reauthenticated_request
async def list_objs_rv(
        *,
        resource: references.Resource,
        namespace: Optional[str] = None,
        context: Optional[auth.APIContext] = None,  # injected by the decorator
) -> Tuple[Collection[bodies.RawBody], str]:
    """
    List the objects of specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes not sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.
    """
    if context is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    is_namespaced = await discovery.is_namespaced(resource=resource, context=context)
    namespace = namespace if is_namespaced else None

    url = resource.get_url(server=context.server, namespace=namespace)
    rsp = await errors.parse_response(await context.session.get(url))

    items: List[bodies.RawBody] = []
    resource_version = rsp.get('metadata', {}).get('resourceVersion', None)
    for item in rsp['items']:
        if 'kind' in rsp:
            item.setdefault('kind', rsp['kind'][:-4] if rsp['kind'][-4:] == 'List' else rsp['kind'])
        if 'apiVersion' in rsp:
            item.setdefault('apiVersion', rsp['apiVersion'])
        items.append(item)

    return items, resource_version
