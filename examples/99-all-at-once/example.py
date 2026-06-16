import asyncio
import pprint
import time
from typing import Any

import kopf
import pykube
import yaml


@kopf.on.startup()
async def startup_fn_simple(logger: kopf.Logger, **_: Any) -> None:
    logger.info('Starting in 1s...')
    await asyncio.sleep(1)


@kopf.on.startup()
async def startup_fn_retried(retry: int, logger: kopf.Logger, **_: Any) -> None:
    if retry < 3:
        raise kopf.TemporaryError(f"Going to succeed in {3-retry}s", delay=1)
    else:
        logger.info('Starting retried...')
        # raise kopf.PermanentError("Unable to start!")


@kopf.on.cleanup()
async def cleanup_fn(logger: kopf.Logger, **_: Any) -> None:
    logger.info('Cleaning up in 3s...')
    await asyncio.sleep(3)


@kopf.on.create('kopfexamples')
def create_1(body: kopf.Body, **_: Any) -> Any:
    children = _create_children(owner=body)

    kopf.info(body, reason='AnyReason')
    kopf.event(body, type='Warning', reason='SomeReason', message="Cannot do something")
    kopf.event(children, type='Normal', reason='SomeReason', message="Created as part of the job1step")

    return {'job1-status': 100}


@kopf.on.create('kopfexamples', backoff=1)
def create_2(retry: int, **_: Any) -> Any:
    wait_for_something()  # specific for job2, e.g. an external API poller

    if not retry:
        # will be retried by the framework, even if it has been restarted
        raise Exception("Whoops!")

    return {'job2-status': 100}


@kopf.on.update('kopfexamples')
def update(diff: kopf.Diff, **_: Any) -> None:
    print('Handling the diff')
    pprint.pprint(list(diff))


@kopf.on.field('kopfexamples', field='spec.lst')
def update_lst(old: Any, new: Any, **_: Any) -> None:
    print(f'Handling the FIELD = {old} -> {new}')


@kopf.on.delete('kopfexamples')
def delete(**_: Any) -> None:
    pass


def _create_children(owner: kopf.Body) -> list[kopf.Body]:
    return []


def wait_for_something() -> None:
    # Note: intentionally blocking from the asyncio point of view.
    time.sleep(1)


@kopf.on.create('kopfexamples')
def create_pod(**_: Any) -> None:

    # Render the pod yaml with some spec fields used in the template.
    pod_data = yaml.safe_load(f"""
        apiVersion: v1
        kind: Pod
        spec:
          containers:
          - name: the-only-one
            image: busybox
            command: ["sh", "-x", "-c", "sleep 1"]
    """)

    # Make it our child: assign the namespace, name, labels, owner references, etc.
    kopf.adopt(pod_data)
    kopf.label(pod_data, {'application': 'kopf-example-10'})

    # Actually create an object by requesting the Kubernetes API.
    api = pykube.HTTPClient(pykube.KubeConfig.from_env())
    pod = pykube.Pod(api, pod_data)
    pod.create()
    api.session.close()


@kopf.on.event('pods', labels={'application': 'kopf-example-10'})
def example_pod_change(logger: kopf.Logger, **_: Any) -> None:
    logger.info("This pod is special for us.")


# Marks for the e2e tests (see tests/e2e/test_examples.py):
E2E_ALLOW_TRACEBACKS = True
E2E_STARTUP_STOP_WORDS = ['Served by the background task.']
E2E_CLEANUP_STOP_WORDS = ['Hung tasks', 'Root tasks']
E2E_CREATION_STOP_WORDS = ['Creation is processed:']
E2E_DELETION_STOP_WORDS = ['Deleted, really deleted']
E2E_CREATION_TIME_LIMIT = 10
E2E_DELETION_TIME_LIMIT = 10
E2E_SUCCESS_COUNTS = {'create_1': 1, 'create_2': 1, 'create_pod': 1, 'delete': 1,
                      'startup_fn_simple': 1, 'startup_fn_retried': 1, 'cleanup_fn': 1}
