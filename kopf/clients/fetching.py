import enum

import pykube
import requests
from typing import TypeVar, Optional, Union, Collection, List, Tuple, cast

from kopf.clients import auth
from kopf.clients import classes
from kopf.structs import bodies
from kopf.structs import resources

_T = TypeVar('_T')


class _UNSET(enum.Enum):
    token = enum.auto()


def read_crd(
        *,
        resource: resources.Resource,
        default: Union[_T, _UNSET] = _UNSET.token,
) -> Union[bodies.Body, _T]:
    try:
        api = auth.get_pykube_api()
        cls = pykube.CustomResourceDefinition
        obj = cls.objects(api, namespace=None).get_by_name(name=resource.name)
        return cast(bodies.Body, obj.obj)

    except pykube.ObjectDoesNotExist:
        if not isinstance(default, _UNSET):
            return default
        raise
    except requests.exceptions.HTTPError as e:
        if not isinstance(default, _UNSET) and e.response.status_code in [403, 404]:
            return default
        raise


def read_obj(
        *,
        resource: resources.Resource,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        default: Union[_T, _UNSET] = _UNSET.token,
) -> Union[bodies.Body, _T]:
    try:
        api = auth.get_pykube_api()
        cls = classes._make_cls(resource=resource)
        namespace = namespace if issubclass(cls, pykube.objects.NamespacedAPIObject) else None
        obj = cls.objects(api, namespace=namespace).get_by_name(name=name)
        return cast(bodies.Body, obj.obj)
    except pykube.ObjectDoesNotExist:
        if not isinstance(default, _UNSET):
            return default
        raise
    except requests.exceptions.HTTPError as e:
        if not isinstance(default, _UNSET) and e.response.status_code in [403, 404]:
            return default
        raise


def list_objs_rv(
        *,
        resource: resources.Resource,
        namespace: Optional[str] = None,
) -> Tuple[Collection[bodies.Body], str]:
    """
    List the objects of specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes not sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.
    """
    api = auth.get_pykube_api()
    cls = classes._make_cls(resource=resource)
    namespace = namespace if issubclass(cls, pykube.objects.NamespacedAPIObject) else None
    lst = cls.objects(api, namespace=pykube.all if namespace is None else namespace)
    rsp = lst.response

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
