import asyncio
import random
from typing import Dict

import kopf
import pykube

E2E_STARTUP_STOP_WORDS = ['Served by the background task.']
E2E_CLEANUP_STOP_WORDS = ['Hung tasks', 'Root tasks']
E2E_SUCCESS_COUNTS = {'startup_fn_simple': 1, 'startup_fn_retried': 1, 'cleanup_fn': 1}
E2E_FAILURE_COUNTS = {}  # type: Dict[str, int]

LOCK: asyncio.Lock  # requires a loop on creation
STOPPERS: Dict[str, Dict[str, asyncio.Event]] = {}  # [namespace][name]


@kopf.on.startup()
async def startup_fn_simple(logger, **kwargs):
    logger.info("Initialising the task-lock...")
    global LOCK
    LOCK = asyncio.Lock()  # in the current asyncio loop


@kopf.on.startup()
async def startup_fn_retried(retry, logger, **kwargs):
    if retry < 3:
        raise kopf.TemporaryError(f"Going to succeed in {3-retry}s", delay=1)
    else:
        logger.info("Starting retried...")
        # raise kopf.PermanentError("Unable to start!")


@kopf.on.cleanup()
async def cleanup_fn(logger, **kwargs):
    logger.info("Cleaning up...")
    for namespace in STOPPERS.keys():
        for name, flag in STOPPERS[namespace].items():
            flag.set()
    logger.info("All pod-tasks are requested to stop...")


@kopf.on.login(errors=kopf.ErrorsMode.PERMANENT)
async def login_fn(**kwargs):
    print('Logging in in 2s...')
    await asyncio.sleep(2.0)

    # An equivalent of kopf.login_via_pykube(), but shrinked for demo purposes.
    config = pykube.KubeConfig.from_env()
    ca = config.cluster.get('certificate-authority')
    cert = config.user.get('client-certificate')
    pkey = config.user.get('client-key')
    return kopf.ConnectionInfo(
        server=config.cluster.get('server'),
        ca_path=ca.filename() if ca else None,  # can be a temporary file
        insecure=config.cluster.get('insecure-skip-tls-verify'),
        username=config.user.get('username'),
        password=config.user.get('password'),
        token=config.user.get('token'),
        certificate_path=cert.filename() if cert else None,  # can be a temporary file
        private_key_path=pkey.filename() if pkey else None,  # can be a temporary file
        default_namespace=config.namespace,
    )


@kopf.on.probe()
async def tasks_count(**kwargs):
    return sum([len(flags) for flags in STOPPERS.values()])


@kopf.on.probe()
async def monitored_objects(**kwargs):
    return {namespace: sorted([name for name in STOPPERS[namespace]]) for namespace in STOPPERS}


@kopf.on.event('pods')
async def pod_task(namespace, name, logger, **_):
    async with LOCK:
        if namespace not in STOPPERS or name not in STOPPERS[namespace]:
            flag = asyncio.Event()
            STOPPERS.setdefault(namespace, {}).setdefault(name, flag)
            asyncio.create_task(_task_fn(logger, shouldstop=flag))


async def _task_fn(logger, shouldstop: asyncio.Event):
    while not shouldstop.is_set():
        await asyncio.sleep(random.randint(1, 10))
        logger.info("Served by the background task.")
    logger.info("Serving is finished by request.")
