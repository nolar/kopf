import pytest

import kopf
from kopf.reactor.registries import Resource, GlobalRegistry


OBJECT_BODY = {
    'apiVersion': 'group/version',
    'kind': 'singular',
    'metadata': {
        'name': 'test',
        'labels': {
            'key': 'value',
        },
        'annotations': {
            'key': 'value',
        }
    }
}


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
def test_requires_finalizer_deletion_handler(optional):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')

    @kopf.on.delete('group', 'version', 'plural',
                    registry=registry, optional=optional)
    def fn(**_):
        pass

    assert registry.requires_finalizer(resource=resource, body=OBJECT_BODY) is not optional


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
def test_requires_finalizer_multiple_handlers(optional):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')

    @kopf.on.create('group', 'version', 'plural',
                    registry=registry)
    def fn1(**_):
        pass

    @kopf.on.delete('group', 'version', 'plural',
                    registry=registry, optional=optional)
    def fn2(**_):
        pass

    assert registry.requires_finalizer(resource=resource, body=OBJECT_BODY) is not optional


def test_requires_finalizer_no_deletion_handler():
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')

    @kopf.on.create('group', 'version', 'plural',
                    registry=registry)
    def fn1(**_):
        pass

    assert registry.requires_finalizer(resource=resource, body=OBJECT_BODY) is False


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
@pytest.mark.parametrize('labels', [
    pytest.param({'key': 'value'}, id='value-matches'),
    pytest.param({'key': None}, id='key-exists'),
])
def test_requires_finalizer_deletion_handler_matches_labels(optional, labels):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')

    @kopf.on.delete('group', 'version', 'plural',
                    labels=labels,
                    registry=registry, optional=optional)
    def fn(**_):
        pass

    assert registry.requires_finalizer(resource=resource, body=OBJECT_BODY) is not optional


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
@pytest.mark.parametrize('labels', [
    pytest.param({'key': 'othervalue'}, id='value-mismatch'),
    pytest.param({'otherkey': None}, id='key-doesnt-exist'),
])
def test_requires_finalizer_deletion_handler_mismatches_labels(optional, labels):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')

    @kopf.on.delete('group', 'version', 'plural',
                    labels=labels,
                    registry=registry, optional=optional)
    def fn(**_):
        pass

    assert registry.requires_finalizer(resource=resource, body=OBJECT_BODY) is False


@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
@pytest.mark.parametrize('annotations', [
    pytest.param({'key': 'value'}, id='value-matches'),
    pytest.param({'key': None}, id='key-exists'),
])
def test_requires_finalizer_deletion_handler_matches_annotations(optional, annotations):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')

    @kopf.on.delete('group', 'version', 'plural',
                    annotations=annotations,
                    registry=registry, optional=optional)
    def fn(**_):
        pass

    assert registry.requires_finalizer(resource=resource, body=OBJECT_BODY) is not optional
    
    
@pytest.mark.parametrize('optional', [
    pytest.param(True, id='optional'),
    pytest.param(False, id='mandatory'),
])
@pytest.mark.parametrize('annotations', [
    pytest.param({'key': 'othervalue'}, id='value-mismatch'),
    pytest.param({'otherkey': None}, id='key-doesnt-exist'),
])
def test_requires_finalizer_deletion_handler_mismatches_annotations(optional, annotations):
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')

    @kopf.on.delete('group', 'version', 'plural',
                    annotations=annotations,
                    registry=registry, optional=optional)
    def fn(**_):
        pass

    assert registry.requires_finalizer(resource=resource, body=OBJECT_BODY) is False
