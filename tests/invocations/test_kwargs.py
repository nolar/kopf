import logging

import pytest

from kopf.reactor.causation import ActivityCause, DaemonCause, ResourceChangingCause, \
                                   ResourceSpawningCause, ResourceWatchingCause
from kopf.reactor.invocation import build_kwargs
from kopf.structs.bodies import Body, BodyEssence
from kopf.structs.configuration import OperatorSettings
from kopf.structs.containers import Memo
from kopf.structs.diffs import Diff
from kopf.structs.handlers import Activity, Reason
from kopf.structs.patches import Patch
from kopf.structs.primitives import DaemonStopper


@pytest.mark.parametrize('activity', set(Activity) - {Activity.STARTUP})
def test_activity_kwargs(resource, activity):
    cause = ActivityCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        activity=activity,
        settings=OperatorSettings(),
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'logger', 'activity'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['logger'] is cause.logger
    assert kwargs['activity'] is activity


@pytest.mark.parametrize('activity', {Activity.STARTUP})
def test_startup_kwargs(resource, activity):
    cause = ActivityCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        activity=activity,
        settings=OperatorSettings(),
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'logger', 'activity', 'settings'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['logger'] is cause.logger
    assert kwargs['activity'] is activity
    assert kwargs['settings'] is cause.settings


def test_resource_watching_kwargs(resource):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = ResourceWatchingCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body(body),
        type='ADDED',
        raw={'type': 'ADDED', 'object': {}},
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'logger', 'resource', 'patch', 'event', 'type', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['resource'] is cause.resource
    assert kwargs['logger'] is cause.logger
    assert kwargs['patch'] is cause.patch
    assert kwargs['event'] is cause.raw
    assert kwargs['memo'] is cause.memo
    assert kwargs['type'] is cause.type
    assert kwargs['body'] is cause.body
    assert kwargs['spec'] is cause.body.spec
    assert kwargs['meta'] is cause.body.metadata
    assert kwargs['status'] is cause.body.status
    assert kwargs['labels'] is cause.body.metadata.labels
    assert kwargs['annotations'] is cause.body.metadata.annotations
    assert kwargs['uid'] == cause.body.metadata.uid
    assert kwargs['name'] == cause.body.metadata.name
    assert kwargs['namespace'] == cause.body.metadata.namespace


def test_resource_changing_kwargs(resource):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = ResourceChangingCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        resource=resource,
        patch=Patch(),
        initial=False,
        reason=Reason.NOOP,
        memo=Memo(),
        body=Body(body),
        diff=Diff([]),
        old=BodyEssence(),
        new=BodyEssence(),
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'logger', 'resource', 'patch', 'reason', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations', 'diff', 'old', 'new'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['resource'] is cause.resource
    assert kwargs['reason'] is cause.reason
    assert kwargs['logger'] is cause.logger
    assert kwargs['patch'] is cause.patch
    assert kwargs['memo'] is cause.memo
    assert kwargs['diff'] is cause.diff
    assert kwargs['old'] is cause.old
    assert kwargs['new'] is cause.new
    assert kwargs['body'] is cause.body
    assert kwargs['spec'] is cause.body.spec
    assert kwargs['meta'] is cause.body.metadata
    assert kwargs['status'] is cause.body.status
    assert kwargs['labels'] is cause.body.metadata.labels
    assert kwargs['annotations'] is cause.body.metadata.annotations
    assert kwargs['uid'] == cause.body.metadata.uid
    assert kwargs['name'] == cause.body.metadata.name
    assert kwargs['namespace'] == cause.body.metadata.namespace


def test_resource_spawning_kwargs(resource):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = ResourceSpawningCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body(body),
        reset=False,
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'logger', 'resource', 'patch', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['resource'] is cause.resource
    assert kwargs['logger'] is cause.logger
    assert kwargs['patch'] is cause.patch
    assert kwargs['memo'] is cause.memo
    assert kwargs['body'] is cause.body
    assert kwargs['spec'] is cause.body.spec
    assert kwargs['meta'] is cause.body.metadata
    assert kwargs['status'] is cause.body.status
    assert kwargs['labels'] is cause.body.metadata.labels
    assert kwargs['annotations'] is cause.body.metadata.annotations
    assert kwargs['uid'] == cause.body.metadata.uid
    assert kwargs['name'] == cause.body.metadata.name
    assert kwargs['namespace'] == cause.body.metadata.namespace


def test_daemon_kwargs(resource):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = DaemonCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body(body),
        stopper=DaemonStopper(),
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'logger', 'resource', 'patch', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['resource'] is cause.resource
    assert kwargs['logger'] is cause.logger
    assert kwargs['patch'] is cause.patch
    assert kwargs['memo'] is cause.memo
    assert kwargs['body'] is cause.body
    assert kwargs['spec'] is cause.body.spec
    assert kwargs['meta'] is cause.body.metadata
    assert kwargs['status'] is cause.body.status
    assert kwargs['labels'] is cause.body.metadata.labels
    assert kwargs['annotations'] is cause.body.metadata.annotations
    assert kwargs['uid'] == cause.body.metadata.uid
    assert kwargs['name'] == cause.body.metadata.name
    assert kwargs['namespace'] == cause.body.metadata.namespace
    assert 'stopped' not in kwargs


def test_daemon_sync_stopper(resource):
    cause = DaemonCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body({}),
        stopper=DaemonStopper(),
    )
    kwargs = build_kwargs(cause=cause, _sync=True)
    assert kwargs['stopped'] is cause.stopper.sync_checker


def test_daemon_async_stopper(resource):
    cause = DaemonCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body({}),
        stopper=DaemonStopper(),
    )
    kwargs = build_kwargs(cause=cause, _sync=False)
    assert kwargs['stopped'] is cause.stopper.async_checker
