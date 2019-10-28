import asyncio
from typing import Optional, cast

import pykube
import requests

from kopf import config
from kopf.clients import auth
from kopf.clients import classes
from kopf.structs import bodies
from kopf.structs import patches
from kopf.structs import resources


async def patch_obj(
        *,
        resource: resources.Resource,
        patch: patches.Patch,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        body: Optional[bodies.Body] = None,
) -> None:
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
        body = cast(bodies.Body, {'metadata': {'name': name}})
        if namespace is not None:
            body['metadata']['namespace'] = namespace

    api = auth.get_pykube_api()
    cls = await classes._make_cls(resource=resource)
    obj = cls(api, body)

    # The handler could delete its own object, so we have nothing to patch. It is okay, ignore.
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(config.WorkersConfig.get_syn_executor(), obj.patch, patch)
    except pykube.ObjectDoesNotExist:
        pass
    except pykube.exceptions.HTTPError as e:
        if e.code == 404:
            pass
        else:
            raise
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            pass
        else:
            raise
