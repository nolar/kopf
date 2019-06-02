import asyncio

from kopf import config
from kopf.clients import auth
from kopf.clients import classes


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
    if body is None:
        nskw = {} if namespace is None else dict(namespace=namespace)
        body = {'metadata': {'name': name}}
        body['metadata'].update(nskw)

    api = auth.get_pykube_api()
    cls = classes._make_cls(resource=resource)
    obj = cls(api, body)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(config.WorkersConfig.get_syn_executor(), obj.patch, patch)
