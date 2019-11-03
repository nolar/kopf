import asyncio
import enum
import functools
from typing import TypeVar, Optional, Union, Collection, List, Tuple, cast

import pykube
import requests

from kopf import config
from kopf.clients import auth
from kopf.clients import classes
from kopf.structs import bodies
from kopf.structs import resources

_T = TypeVar('_T')


class _UNSET(enum.Enum):
    token = enum.auto()


@auth.reauthenticated_request
async def read_crd(
        *,
        resource: resources.Resource,
        default: Union[_T, _UNSET] = _UNSET.token,
        api: Optional[pykube.HTTPClient] = None,  # injected by the decorator
        session: Optional[auth.APISession] = None,  # injected by the decorator
) -> Union[bodies.Body, _T]:
    if api is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    try:
        loop = asyncio.get_running_loop()
        cls = pykube.CustomResourceDefinition
        qry = cls.objects(api, namespace=None)
        fn = functools.partial(qry.get_by_name, name=resource.name)
        obj = await loop.run_in_executor(config.WorkersConfig.get_syn_executor(), fn)
        return cast(bodies.Body, obj.obj)
    except pykube.ObjectDoesNotExist:
        if not isinstance(default, _UNSET):
            return default
        raise
    except requests.exceptions.HTTPError as e:
        if not isinstance(default, _UNSET) and e.response.status_code in [403, 404]:
            return default
        raise


@auth.reauthenticated_request
async def read_obj(
        *,
        resource: resources.Resource,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        default: Union[_T, _UNSET] = _UNSET.token,
        api: Optional[pykube.HTTPClient] = None,  # injected by the decorator
        session: Optional[auth.APISession] = None,  # injected by the decorator
) -> Union[bodies.Body, _T]:
    if api is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    try:
        loop = asyncio.get_running_loop()
        cls = await classes._make_cls(api=api, resource=resource)
        namespace = namespace if issubclass(cls, pykube.objects.NamespacedAPIObject) else None
        qry = cls.objects(api, namespace=namespace)
        fn = functools.partial(qry.get_by_name, name=name)
        obj = await loop.run_in_executor(config.WorkersConfig.get_syn_executor(), fn)
        return cast(bodies.Body, obj.obj)
    except pykube.ObjectDoesNotExist:
        if not isinstance(default, _UNSET):
            return default
        raise
    except requests.exceptions.HTTPError as e:
        if not isinstance(default, _UNSET) and e.response.status_code in [403, 404]:
            return default
        raise


@auth.reauthenticated_request
async def list_objs_rv(
        *,
        resource: resources.Resource,
        namespace: Optional[str] = None,
        api: Optional[pykube.HTTPClient] = None,  # injected by the decorator
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
    if api is None:
        raise RuntimeError("API instance is not injected by the decorator.")

    loop = asyncio.get_running_loop()
    cls = await classes._make_cls(api=api, resource=resource)
    namespace = namespace if issubclass(cls, pykube.objects.NamespacedAPIObject) else None
    qry = cls.objects(api, namespace=pykube.all if namespace is None else namespace)
    fn = lambda: qry.response  # it is a property, so cannot be threaded without lambdas.
    rsp = await loop.run_in_executor(config.WorkersConfig.get_syn_executor(), fn)

    items: List[bodies.Body] = []
    resource_version = rsp.get('metadata', {}).get('resourceVersion', None)
    for item in rsp['items']:
        # FIXME: fix in pykube to inject the missing item's fields from the list's metainfo.
        if 'kind' in rsp:
            item.setdefault('kind', rsp['kind'][:-4] if rsp['kind'][-4:] == 'List' else rsp['kind'])
        if 'apiVersion' in rsp:
            item.setdefault('apiVersion', rsp['apiVersion'])
        items.append(item)

    return items, resource_version
