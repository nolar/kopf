import asyncio
import functools
from typing import Type

import pykube

from kopf import config
from kopf.structs import resources


async def _make_cls(
        *,
        api: pykube.HTTPClient,
        resource: resources.Resource,
) -> Type[pykube.objects.APIObject]:
    loop = asyncio.get_running_loop()
    fn = functools.partial(api.resource_list, resource.api_version)
    rsp = await loop.run_in_executor(config.WorkersConfig.get_syn_executor(), fn)

    api_resources = rsp['resources']
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
