import asyncio

import kopf
import pykube

from typing import Dict


tasks: Dict[str, Dict[str, asyncio.Task]] = {}  # dict{namespace: dict{name: asyncio.Task}}


@kopf.on.resume('pods')
@kopf.on.create('pods')
async def pod_in_sight(namespace, name, logger, **kwargs):
    if namespace.startswith('kube-'):
        return
    else:
        task = asyncio.create_task(pod_killer(namespace, name, logger))
        tasks.setdefault(namespace, {})
        tasks[namespace][name] = task


@kopf.on.delete('pods')
async def pod_deleted(namespace, name, **kwargs):
    if namespace in tasks and name in tasks[namespace]:
        task = tasks[namespace][name]
        task.cancel()  # it will also remove from `tasks`


async def pod_killer(namespace, name, logger, timeout=30):
    try:
        logger.info(f"=== Pod killing happens in {timeout}s.")
        await asyncio.sleep(timeout)
        logger.info(f"=== Pod killing happens NOW!")

        api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        pod = pykube.Pod.objects(api, namespace=namespace).get_by_name(name)
        pod.delete()
        api.session.close()

    except asyncio.CancelledError:
        logger.info(f"=== Pod killing is cancelled!")

    finally:
        if namespace in tasks and name in tasks[namespace]:
            del tasks[namespace][name]
