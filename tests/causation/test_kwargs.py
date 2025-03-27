import dataclasses
import logging
from unittest.mock import Mock

import pytest

from kopf._cogs.configs.configuration import OperatorSettings
from kopf._cogs.structs import diffs
from kopf._cogs.structs.bodies import Body, BodyEssence
from kopf._cogs.structs.diffs import Diff
from kopf._cogs.structs.ephemera import Memo
from kopf._cogs.structs.patches import Patch
from kopf._core.engines.indexing import OperatorIndexer, OperatorIndexers
from kopf._core.intents.causes import Activity, ActivityCause, BaseCause, ChangingCause, \
                                      DaemonCause, IndexingCause, Reason, ResourceCause, \
                                      SpawningCause, WatchingCause, WebhookCause
from kopf._core.intents.stoppers import DaemonStopper

ALL_CAUSES = [
    BaseCause, ActivityCause, ResourceCause,
    WatchingCause, SpawningCause,
    ChangingCause, IndexingCause,
    WebhookCause, DaemonCause,
]
ALL_FIELDS = {
    field.name
    for cause_cls in ALL_CAUSES
    for field in dataclasses.fields(cause_cls)
} | {'stopped', 'body', 'spec', 'meta', 'status', 'name', 'namespace', 'labels', 'annotations'}


@pytest.mark.parametrize('cls', ALL_CAUSES)
@pytest.mark.parametrize('name', ALL_FIELDS)
@pytest.mark.parametrize('attr', ['kwargs', 'sync_kwargs', 'async_kwargs'])
def test_indices_overwrite_kwargs(cls: type[BaseCause], name, attr):
    indexers = OperatorIndexers()
    indexers['index1'] = OperatorIndexer()
    indexers['index2'] = OperatorIndexer()
    indexers[name] = OperatorIndexer()
    mocks = {field.name: Mock() for field in dataclasses.fields(cls)}
    mocks['indices'] = indexers.indices
    cause = cls(**mocks)
    kwargs = getattr(cause, attr)  # cause.kwargs / cause.sync_kwargs / cause.async_kwargs
    assert kwargs['index1'] is indexers['index1'].index
    assert kwargs['index2'] is indexers['index2'].index
    assert kwargs[name] is indexers[name].index


@pytest.mark.parametrize('activity', set(Activity))
@pytest.mark.parametrize('attr', ['kwargs', 'sync_kwargs', 'async_kwargs'])
def test_activity_kwargs(resource, activity, attr):
    cause = ActivityCause(
        memo=Memo(),
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=OperatorIndexers().indices,
        activity=activity,
        settings=OperatorSettings(),
    )
    kwargs = getattr(cause, attr)  # cause.kwargs / cause.sync_kwargs / cause.async_kwargs
    assert set(kwargs) == {'memo', 'logger', 'activity', 'settings'}
    assert kwargs['logger'] is cause.logger
    assert kwargs['activity'] is activity
    assert kwargs['settings'] is cause.settings


@pytest.mark.parametrize('attr', ['kwargs', 'sync_kwargs', 'async_kwargs'])
def test_admission_kwargs(resource, attr):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = WebhookCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=OperatorIndexers().indices,
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
        subresource=None,
        new=BodyEssence(body),
        old=None,
        diff=diffs.diff(BodyEssence(body), None),
    )
    kwargs = getattr(cause, attr)  # cause.kwargs / cause.sync_kwargs / cause.async_kwargs
    assert set(kwargs) == {'logger', 'resource',
                           'dryrun', 'headers', 'sslpeer', 'userinfo', 'warnings', 'subresource',
                           'patch', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations', 'old', 'new', 'diff', 'operation'}
    assert kwargs['resource'] is cause.resource
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
    assert kwargs['operation'] == cause.operation
    assert kwargs['new'] == cause.new
    assert kwargs['old'] == cause.old
    assert kwargs['diff'] == cause.diff


@pytest.mark.parametrize('attr', ['kwargs', 'sync_kwargs', 'async_kwargs'])
def test_watching_kwargs(resource, attr):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = WatchingCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=OperatorIndexers().indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body(body),
        type='ADDED',
        event={'type': 'ADDED', 'object': {}},
    )
    kwargs = getattr(cause, attr)  # cause.kwargs / cause.sync_kwargs / cause.async_kwargs
    assert set(kwargs) == {'logger', 'resource',
                           'patch', 'event', 'type', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations'}
    assert kwargs['resource'] is cause.resource
    assert kwargs['logger'] is cause.logger
    assert kwargs['patch'] is cause.patch
    assert kwargs['event'] is cause.event
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


@pytest.mark.parametrize('attr', ['kwargs', 'sync_kwargs', 'async_kwargs'])
def test_changing_kwargs(resource, attr):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = ChangingCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=OperatorIndexers().indices,
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
    kwargs = getattr(cause, attr)  # cause.kwargs / cause.sync_kwargs / cause.async_kwargs
    assert set(kwargs) == {'logger', 'resource',
                           'patch', 'reason', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations', 'diff', 'old', 'new'}
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


@pytest.mark.parametrize('attr', ['kwargs', 'sync_kwargs', 'async_kwargs'])
def test_spawning_kwargs(resource, attr):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = SpawningCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=OperatorIndexers().indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body(body),
        reset=False,
    )
    kwargs = getattr(cause, attr)  # cause.kwargs / cause.sync_kwargs / cause.async_kwargs
    assert set(kwargs) == {'logger', 'resource', 'patch', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations'}
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


@pytest.mark.parametrize('attr', ['kwargs'])
def test_daemon_kwargs(resource, attr):
    body = {'metadata': {'uid': 'uid1', 'name': 'name1', 'namespace': 'ns1',
                         'labels': {'l1': 'v1'}, 'annotations': {'a1': 'v1'}},
            'spec': {'field': 'value'},
            'status': {'info': 'payload'}}
    cause = DaemonCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=OperatorIndexers().indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body(body),
        stopper=DaemonStopper(),
    )
    kwargs = getattr(cause, attr)  # cause.kwargs
    assert set(kwargs) == {'logger', 'resource', 'patch', 'memo',
                           'body', 'spec', 'status', 'meta', 'uid', 'name', 'namespace',
                           'labels', 'annotations'}
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
    assert 'stopper' not in kwargs
    assert 'stopped' not in kwargs


@pytest.mark.parametrize('attr', ['sync_kwargs'])
def test_daemon_sync_stopper(resource, attr):
    cause = DaemonCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=OperatorIndexers().indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body({}),
        stopper=DaemonStopper(),
    )
    kwargs = getattr(cause, attr)  # cause.sync_kwargs
    assert 'stopper' not in kwargs
    assert kwargs['stopped'] is cause.stopper.sync_waiter


@pytest.mark.parametrize('attr', ['async_kwargs'])
def test_daemon_async_stopper(resource, attr):
    cause = DaemonCause(
        logger=logging.getLogger('kopf.test.fake.logger'),
        indices=OperatorIndexers().indices,
        resource=resource,
        patch=Patch(),
        memo=Memo(),
        body=Body({}),
        stopper=DaemonStopper(),
    )
    kwargs = getattr(cause, attr)  # cause.async_kwargs
    assert 'stopper' not in kwargs
    assert kwargs['stopped'] is cause.stopper.async_waiter
