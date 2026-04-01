import asyncio
from typing import Any

import kopf
import pykube

tasks: dict[str, dict[str, asyncio.Task[None]]] = {}  # dict{namespace: dict{name: asyncio.Task}}


@kopf.on.resume('pods')
@kopf.on.create('pods')
async def pod_in_sight(namespace: str | None, name: str, logger: kopf.Logger, **_: Any) -> None:
    assert namespace is not None  # for type-checkers
    if namespace.startswith('kube-'):
        return
    else:
        task = asyncio.create_task(pod_killer(namespace, name, logger))
        tasks.setdefault(namespace, {})
        tasks[namespace][name] = task


@kopf.on.delete('pods')
async def pod_deleted(namespace: str | None, name: str, **_: Any) -> None:
    if namespace in tasks and name in tasks[namespace]:
        task = tasks[namespace][name]
        task.cancel()  # it will also remove from `tasks`


async def pod_killer(namespace: str | None, name: str, logger: kopf.Logger, timeout: float = 30) -> None:
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
