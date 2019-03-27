"""
Kubernetes operator example: all the features at once (for debugging & testing).
"""

import pprint
import time

import kopf


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_1(body, meta, spec, status, **kwargs):
    children = _create_children(owner=body)

    kopf.info(body, reason='AnyReason')
    kopf.event(body, type='Warning', reason='SomeReason', message="Cannot do something")
    kopf.event(children, type='Normal', reason='SomeReason', message="Created as part of the job1step")

    return {'job1-status': 100}


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_2(body, meta, spec, status, retry=None, **kwargs):
    wait_for_something()  # specific for job2, e.g. an external API poller

    if not retry:
        # will be retried by the framework, even if it has been restarted
        raise Exception("Whoops!")

    return {'job2-status': 100}


@kopf.on.update('zalando.org', 'v1', 'kopfexamples')
def update(body, meta, spec, status, old, new, diff, **kwargs):
    print('Handling the diff')
    pprint.pprint(list(diff))


@kopf.on.field('zalando.org', 'v1', 'kopfexamples', field='spec.lst')
def update_lst(body, meta, spec, status, old, new, **kwargs):
    print(f'Handling the FIELD = {old} -> {new}')


@kopf.on.delete('zalando.org', 'v1', 'kopfexamples')
def delete(body, meta, spec, status, **kwargs):
    pass


def _create_children(owner):
    return []


def wait_for_something():
    # Note: intentionally blocking from the asyncio point of view.
    time.sleep(1)
