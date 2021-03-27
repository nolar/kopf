import logging

import pytest

from kopf.reactor.causation import ActivityCause, DaemonCause, \
                                   ResourceChangingCause, ResourceSpawningCause, \
                                   ResourceWatchingCause, ResourceWebhookCause
from kopf.reactor.indexing import OperatorIndexer, OperatorIndexers
from kopf.reactor.invocation import build_kwargs
from kopf.structs.bodies import Body, BodyEssence
from kopf.structs.configuration import OperatorSettings
from kopf.structs.diffs import Diff
from kopf.structs.ephemera import Memo
from kopf.structs.handlers import Activity, Reason
from kopf.structs.patches import Patch
from kopf.structs.primitives import DaemonStopper


@pytest.fixture()
def indices():
    indexers = OperatorIndexers()
    indexers['index1'] = OperatorIndexer()
    indexers['index2'] = OperatorIndexer()
    return indexers.indices


@pytest.mark.parametrize('activity', set(Activity) - {Activity.STARTUP})
def test_activity_kwargs(resource, activity, indices):
    cause = ActivityCause(
        memo=Memo(),
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=indices,
        activity=activity,
        settings=OperatorSettings(),
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'memo', 'logger', 'index1', 'index2', 'activity'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['index1'] is indices['index1']
    assert kwargs['index2'] is indices['index2']
    assert kwargs['logger'] is cause.logger
    assert kwargs['activity'] is activity


@pytest.mark.parametrize('activity', {Activity.STARTUP})
def test_startup_kwargs(resource, activity, indices):
    cause = ActivityCause(
        memo=Memo(),
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=indices,
        activity=activity,
        settings=OperatorSettings(),
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'memo', 'logger', 'index1', 'index2',
                           'activity', 'settings'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['index1'] is indices['index1']
    assert kwargs['index2'] is indices['index2']
    assert kwargs['logger'] is cause.logger
    assert kwargs['activity'] is activity
    assert kwargs['settings'] is cause.settings


def test_resource_admission_kwargs(resource, indices):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = ResourceWebhookCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body(body),
        dryrun=False,
        headers={'k1': 'v1'},
        sslpeer={'k2': 'v2'},
        userinfo={'k3': 'v3'},
        warnings=['w1'],
        webhook=None,
        reason=None,
        operation=None,
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'logger', 'index1', 'index2', 'resource',
                           'dryrun', 'headers', 'sslpeer', 'userinfo', 'warnings',
                           'patch', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['resource'] is cause.resource
    assert kwargs['index1'] is indices['index1']
    assert kwargs['index2'] is indices['index2']
    assert kwargs['logger'] is cause.logger
    assert kwargs['dryrun'] is cause.dryrun
    assert kwargs['headers'] is cause.headers
    assert kwargs['sslpeer'] is cause.sslpeer
    assert kwargs['userinfo'] is cause.userinfo
    assert kwargs['warnings'] is cause.warnings
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


def test_resource_watching_kwargs(resource, indices):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = ResourceWatchingCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body(body),
        type='ADDED',
        raw={'type': 'ADDED', 'object': {}},
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'logger', 'index1', 'index2', 'resource',
                           'patch', 'event', 'type', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['resource'] is cause.resource
    assert kwargs['index1'] is indices['index1']
    assert kwargs['index2'] is indices['index2']
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


def test_resource_changing_kwargs(resource, indices):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = ResourceChangingCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=indices,
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
    assert set(kwargs) == {'extrakwarg', 'logger', 'index1', 'index2', 'resource',
                           'patch', 'reason', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations', 'diff', 'old', 'new'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['resource'] is cause.resource
    assert kwargs['index1'] is indices['index1']
    assert kwargs['index2'] is indices['index2']
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


def test_resource_spawning_kwargs(resource, indices):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = ResourceSpawningCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body(body),
        reset=False,
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'logger', 'index1', 'index2',
                           'resource', 'patch', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['resource'] is cause.resource
    assert kwargs['index1'] is indices['index1']
    assert kwargs['index2'] is indices['index2']
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


def test_daemon_kwargs(resource, indices):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = DaemonCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body(body),
        stopper=DaemonStopper(),
    )
    kwargs = build_kwargs(cause=cause, extrakwarg=123)
    assert set(kwargs) == {'extrakwarg', 'logger', 'index1', 'index2',
                           'resource', 'patch', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations'}
    assert kwargs['extrakwarg'] == 123
    assert kwargs['resource'] is cause.resource
    assert kwargs['index1'] is indices['index1']
    assert kwargs['index2'] is indices['index2']
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


def test_daemon_sync_stopper(resource, indices):
    cause = DaemonCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body({}),
        stopper=DaemonStopper(),
    )
    kwargs = build_kwargs(cause=cause, _sync=True)
    assert kwargs['stopped'] is cause.stopper.sync_checker


def test_daemon_async_stopper(resource, indices):
    cause = DaemonCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body({}),
        stopper=DaemonStopper(),
    )
    kwargs = build_kwargs(cause=cause, _sync=False)
    assert kwargs['stopped'] is cause.stopper.async_checker
