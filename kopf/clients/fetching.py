import pykube
import requests

from kopf.clients import auth
from kopf.clients import classes

_UNSET_ = object()


def read_crd(*, resource, default=_UNSET_):
    try:
        api = auth.get_pykube_api()
        cls = pykube.CustomResourceDefinition
        obj = cls.objects(api, namespace=None).get_by_name(name=resource.name)
        return obj.obj

    except pykube.ObjectDoesNotExist:
        if default is not _UNSET_:
            return default
        raise
    except requests.exceptions.HTTPError as e:
        if e.response.status_code in [403, 404] and default is not _UNSET_:
            return default
        raise


def read_obj(*, resource, namespace=None, name=None, default=_UNSET_):
    try:
        api = auth.get_pykube_api()
        cls = classes._make_cls(resource=resource)
        namespace = namespace if issubclass(cls, pykube.objects.NamespacedAPIObject) else None
        obj = cls.objects(api, namespace=namespace).get_by_name(name=name)
        return obj.obj
    except pykube.ObjectDoesNotExist:
        if default is not _UNSET_:
            return default
        raise
    except requests.exceptions.HTTPError as e:
        if e.response.status_code in [403, 404] and default is not _UNSET_:
            return default
        raise


def list_objs_rv(*, resource, namespace=None):
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

    items = []
    resource_version = rsp.get('metadata', {}).get('resourceVersion', None)
    for item in rsp['items']:
        # FIXME: fix in pykube to inject the missing item's fields from the list's metainfo.
        if 'kind' in rsp:
            item.setdefault('kind', rsp['kind'][:-4] if rsp['kind'][-4:] == 'List' else rsp['kind'])
        if 'apiVersion' in rsp:
            item.setdefault('apiVersion', rsp['apiVersion'])
        items.append(item)

    return items, resource_version
