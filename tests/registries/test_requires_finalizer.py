import pytest

import kopf
from kopf.reactor.causation import CREATE, UPDATE, DELETE
from kopf.reactor.handling import subregistry_var
from kopf.reactor.registries import Resource, SimpleRegistry, GlobalRegistry

# labels annos optional deletion other required
# ----------------- param = optional -----------------------
# no     no    false    yes      no    true
# no     no    true     yes      no    false
# ----------------- param = optional -----------------------------
# no     no    false    yes      yes   true
# no     no    true     yes      yes   false
# ----------------------------------------------
# no     no    -        no       yes   false
# ----------------------------------------------
# match  no    false    yes      no    true
# match  no    true     yes      no    false
# ----------------------------------------------
# misma  no    false    yes      no    false
# misma  no    true     yes      no    false
# ----------------------------------------------
# no     match false    yes      no    true
# no     match true     yes      no    false
# ----------------------------------------------
# no     misma false    yes      no    false
# no     misma true     yes      no    false


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

    body = {
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

    assert registry.requires_finalizer(resource=resource, body=body) is not optional


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

    body = {
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

    assert registry.requires_finalizer(resource=resource, body=body) is not optional


def test_requires_finalizer_no_deletion_handler():
    registry = GlobalRegistry()
    resource = Resource('group', 'version', 'plural')

    @kopf.on.create('group', 'version', 'plural',
                    registry=registry)
    def fn1(**_):
        pass

    body = {
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

    assert registry.requires_finalizer(resource=resource, body=body) is False


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

    body = {
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

    assert registry.requires_finalizer(resource=resource, body=body) is not optional


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

    body = {
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

    assert registry.requires_finalizer(resource=resource, body=body) is False


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

    body = {
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

    assert registry.requires_finalizer(resource=resource, body=body) is not optional
    
    
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

    body = {
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

    assert registry.requires_finalizer(resource=resource, body=body) is False
