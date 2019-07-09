import json
from typing import Type

import pykube

from kopf.clients import auth


# TODO: this mixin has to be migrated into pykube-ng itself.
class APIObject(pykube.objects.APIObject):
    def patch(self, patch):
        '''
        Patch the Kubernetes resource by calling the API.
        '''
        r = self.api.patch(**self.api_kwargs(
            headers={"Content-Type": "application/merge-patch+json"},
            data=json.dumps(patch),
        ))
        self.api.raise_for_status(r)
        self.set_obj(r.json())


class NamespacedAPIObject(pykube.objects.NamespacedAPIObject, APIObject):
    pass


class CustomResourceDefinition(APIObject):
    version = "apiextensions.k8s.io/v1beta1"
    endpoint = "customresourcedefinitions"
    kind = "CustomResourceDefinition"


def _make_cls(resource) -> Type[APIObject]:
    api = auth.get_pykube_api()
    api_resources = api.resource_list(resource.api_version)['resources']
    resource_kind = next((r['kind'] for r in api_resources if r['name'] == resource.plural), None)
    is_namespaced = next((r['namespaced'] for r in api_resources if r['name'] == resource.plural), None)
    if not resource_kind:
        raise pykube.ObjectDoesNotExist(f"No such CRD: {resource.name}")

    cls_name = resource.plural
    cls_base = NamespacedAPIObject if is_namespaced else APIObject
    cls = type(cls_name, (cls_base,), {
        'version': resource.api_version,
        'endpoint': resource.plural,
        'kind': resource_kind,
    })
    return cls
