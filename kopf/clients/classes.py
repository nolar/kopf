from typing import Type

import pykube

from kopf.clients import auth


def _make_cls(resource) -> Type[pykube.objects.APIObject]:
    api = auth.get_pykube_api()
    api_resources = api.resource_list(resource.api_version)['resources']
    resource_kind = next((r['kind'] for r in api_resources if r['name'] == resource.plural), None)
    is_namespaced = next((r['namespaced'] for r in api_resources if r['name'] == resource.plural), None)
    if not resource_kind:
        raise pykube.ObjectDoesNotExist(f"No such CRD: {resource.name}")

    cls_name = resource.plural
    cls_base = pykube.objects.NamespacedAPIObject if is_namespaced else pykube.objects.APIObject
    cls = type(cls_name, (cls_base,), {
        'version': resource.api_version,
        'endpoint': resource.plural,
        'kind': resource_kind,
    })
    return cls
