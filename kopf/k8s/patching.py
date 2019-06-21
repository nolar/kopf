import asyncio
import functools

import kubernetes

from kopf import config


async def patch_obj(*, resource, patch, namespace=None, name=None, body=None):
    """
    Patch a resource of specific kind.

    Either the namespace+name should be specified, or the body,
    which is used only to get namespace+name identifiers.

    Unlike the object listing, the namespaced call is always
    used for the namespaced resources, even if the operator serves
    the whole cluster (i.e. is not namespace-restricted).
    """

    if body is not None and (name is not None or namespace is not None):
        raise TypeError("Either body, or name+namespace can be specified. Got both.")

    namespace = body.get('metadata', {}).get('namespace') if body is not None else namespace
    name = body.get('metadata', {}).get('name') if body is not None else name

    api = kubernetes.client.CustomObjectsApi()
    request_kwargs = {
        'group': resource.group,
        'version': resource.version,
        'plural': resource.plural,
        'name': name,
        'body': patch
    }
    patch_func = api.patch_cluster_custom_object
    if namespace is not None:
        request_kwargs['namespace'] = namespace
        patch_func = api.patch_namespaced_custom_object
    loop = asyncio.get_running_loop()

    await loop.run_in_executor(config.WorkersConfig.get_syn_executor(), functools.partial(patch_func, **request_kwargs))
