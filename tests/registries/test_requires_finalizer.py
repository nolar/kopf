import pytest

import kopf
from kopf.structs.filters import PRESENT

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


@pytest.mark.parametrize('optional, expected', [
    pytest.param(True, False, id='optional'),
    pytest.param(False, True, id='mandatory'),
])
def test_requires_finalizer_deletion_handler(
        optional, expected, cause_factory, resource, registry):
    cause = cause_factory(resource=resource, body=OBJECT_BODY)

    @kopf.on.delete(*resource, optional=optional)
    def fn(**_):
        pass

    requires_finalizer = registry._resource_changing.requires_finalizer(cause)
    assert requires_finalizer == expected


@pytest.mark.parametrize('optional, expected', [
    pytest.param(True, False, id='optional'),
    pytest.param(False, True, id='mandatory'),
])
def test_requires_finalizer_multiple_handlers(
        optional, expected, cause_factory, resource, registry):
    cause = cause_factory(resource=resource, body=OBJECT_BODY)

    @kopf.on.create(*resource)
    def fn1(**_):
        pass

    @kopf.on.delete(*resource, optional=optional)
    def fn2(**_):
        pass

    requires_finalizer = registry._resource_changing.requires_finalizer(cause)
    assert requires_finalizer == expected


def test_requires_finalizer_no_deletion_handler(
        cause_factory, resource, registry):
    cause = cause_factory(resource=resource, body=OBJECT_BODY)

    @kopf.on.create(*resource)
    def fn1(**_):
        pass

    requires_finalizer = registry._resource_changing.requires_finalizer(cause)
    assert requires_finalizer is False


@pytest.mark.parametrize('optional, expected', [
    pytest.param(True, False, id='optional'),
    pytest.param(False, True, id='mandatory'),
])
@pytest.mark.parametrize('labels', [
    pytest.param({'key': 'value'}, id='value-matches'),
    pytest.param({'key': PRESENT}, id='key-exists'),
])
def test_requires_finalizer_deletion_handler_matches_labels(
        labels, optional, expected, cause_factory, resource, registry):
    cause = cause_factory(resource=resource, body=OBJECT_BODY)

    @kopf.on.delete(*resource, labels=labels, optional=optional)
    def fn(**_):
        pass

    requires_finalizer = registry._resource_changing.requires_finalizer(cause)
    assert requires_finalizer == expected


@pytest.mark.parametrize('optional, expected', [
    pytest.param(True, False, id='optional'),
    pytest.param(False, False, id='mandatory'),
])
@pytest.mark.parametrize('labels', [
    pytest.param({'key': 'othervalue'}, id='value-mismatch'),
    pytest.param({'otherkey': PRESENT}, id='key-doesnt-exist'),
])
def test_requires_finalizer_deletion_handler_mismatches_labels(
        labels, optional, expected, cause_factory, resource, registry):
    cause = cause_factory(resource=resource, body=OBJECT_BODY)

    @kopf.on.delete(*resource, labels=labels, optional=optional)
    def fn(**_):
        pass

    requires_finalizer = registry._resource_changing.requires_finalizer(cause)
    assert requires_finalizer == expected


@pytest.mark.parametrize('optional, expected', [
    pytest.param(True, False, id='optional'),
    pytest.param(False, True, id='mandatory'),
])
@pytest.mark.parametrize('annotations', [
    pytest.param({'key': 'value'}, id='value-matches'),
    pytest.param({'key': PRESENT}, id='key-exists'),
])
def test_requires_finalizer_deletion_handler_matches_annotations(
        annotations, optional, expected, cause_factory, resource, registry):
    cause = cause_factory(resource=resource, body=OBJECT_BODY)

    @kopf.on.delete(*resource, annotations=annotations, optional=optional)
    def fn(**_):
        pass

    requires_finalizer = registry._resource_changing.requires_finalizer(cause)
    assert requires_finalizer == expected


@pytest.mark.parametrize('optional, expected', [
    pytest.param(True, False, id='optional'),
    pytest.param(False, False, id='mandatory'),
])
@pytest.mark.parametrize('annotations', [
    pytest.param({'key': 'othervalue'}, id='value-mismatch'),
    pytest.param({'otherkey': PRESENT}, id='key-doesnt-exist'),
])
def test_requires_finalizer_deletion_handler_mismatches_annotations(
        annotations, optional, expected, cause_factory, resource, registry):
    cause = cause_factory(resource=resource, body=OBJECT_BODY)

    @kopf.on.delete(*resource, annotations=annotations, optional=optional)
    def fn(**_):
        pass

    requires_finalizer = registry._resource_changing.requires_finalizer(cause)
    assert requires_finalizer == expected
