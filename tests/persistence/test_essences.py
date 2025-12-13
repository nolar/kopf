import pytest

from kopf._cogs.configs.diffbase import AnnotationsDiffBaseStorage, \
                                        DiffBaseStorage, StatusDiffBaseStorage
from kopf._cogs.structs.bodies import Body

ALL_STORAGES = [AnnotationsDiffBaseStorage, StatusDiffBaseStorage]


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_resource_references(
        cls: type[DiffBaseStorage],
):
    body = Body({'apiVersion': 'group/version', 'kind': 'Kind'})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {}


@pytest.mark.parametrize('field', [
    'uid',
    'name',
    'namespace',
    'selfLink',
    'generation',
    'finalizers',
    'resourceVersion',
    'creationTimestamp',
    'deletionTimestamp',
    'any-unexpected-field',
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_system_fields_and_cleans_parents(
        field: str,
        cls: type[DiffBaseStorage],
):
    body = Body({'metadata': {field: 'x'}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {}


@pytest.mark.parametrize('field', [
    'uid',
    'name',
    'namespace',
    'selfLink',
    'generation',
    'finalizers',
    'resourceVersion',
    'creationTimestamp',
    'deletionTimestamp',
    'any-unexpected-field',
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_system_fields_but_keeps_extra_fields(
        field: str,
        cls: type[DiffBaseStorage],
):
    body = Body({'metadata': {field: 'x', 'other': 'y'}})
    storage = cls()
    essence = storage.build(body=body, extra_fields=['metadata.other'])
    assert essence == {'metadata': {'other': 'y'}}


@pytest.mark.parametrize('annotation', [
    pytest.param('kubectl.kubernetes.io/last-applied-configuration', id='kubectl'),
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_garbage_annotations_and_cleans_parents(
        annotation: str,
        cls: type[DiffBaseStorage],
):
    body = Body({'metadata': {'annotations': {annotation: 'x'}}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {}


@pytest.mark.parametrize('annotation', [
    pytest.param('kubectl.kubernetes.io/last-applied-configuration', id='kubectl'),
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_garbage_annotations_but_keeps_others(
        annotation: str,
        cls: type[DiffBaseStorage],
):
    body = Body({'metadata': {'annotations': {annotation: 'x', 'other': 'y'}}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {'metadata': {'annotations': {'other': 'y'}}}


@pytest.mark.parametrize('prefix', [
    'kopf-domain.tld',
    'kopf.domain.tld',
    'domain.tld.kopf',
    'domain.tld-kopf',
    'domain.kopf.tld',
    'domain.kopf-tld',
    'domain-kopf.tld',
    'domain-kopf-tld',
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_keeps_annotations_mentioning_kopf_but_not_from_other_operators(
        prefix: str,
        cls: type[DiffBaseStorage],
):
    annotation = f'{prefix}/to-be-removed'
    body = Body({'metadata': {'annotations': {annotation: 'x'}}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {'metadata': {'annotations': {annotation: 'x'}}}


@pytest.mark.parametrize('prefix', [
    'kopf.zalando.org',
    'sub.kopf.zalando.org',
    'sub.sub.kopf.zalando.org',
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_other_operators_annotations_by_domain(
        prefix: str,
        cls: type[DiffBaseStorage],
):
    annotation = f'{prefix}/to-be-removed'
    body = Body({'metadata': {'annotations': {annotation: 'x', 'other': 'y'}}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {'metadata': {'annotations': {'other': 'y'}}}


@pytest.mark.parametrize('prefix', [
    'domain.tld',
])
@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_other_operators_annotations_by_marker(
        prefix: str,
        cls: type[DiffBaseStorage],
):
    marker = f'{prefix}/kopf-managed'
    annotation = f'{prefix}/to-be-removed'
    body = Body({'metadata': {'annotations': {annotation: 'x', marker: 'yes', 'other': 'y'}}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {'metadata': {'annotations': {'other': 'y'}}}


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_status_and_cleans_parents(
        cls: type[DiffBaseStorage],
):
    body = Body({'status': {'kopf': {'progress': 'x', 'anything': 'y'}, 'other': 'z'}})
    storage = cls()
    essence = storage.build(body=body)
    assert essence == {}


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_removes_status_but_keeps_extra_fields(
        cls: type[DiffBaseStorage],
):
    body = Body({'status': {'kopf': {'progress': 'x', 'anything': 'y'}, 'other': 'z'}})
    storage = cls()
    essence = storage.build(body=body, extra_fields=['status.other'])
    assert essence == {'status': {'other': 'z'}}


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_get_essence_clones_body(
        cls: type[DiffBaseStorage],
):
    body = Body({'spec': {'depth': {'field': 'x'}}})
    storage = cls()
    essence = storage.build(body=body)
    body['spec']['depth']['field'] = 'y'
    assert essence is not body
    assert essence['spec'] is not body['spec']
    assert essence['spec']['depth'] is not body['spec']['depth']
    assert essence['spec']['depth']['field'] == 'x'


@pytest.mark.parametrize('cls', ALL_STORAGES)
def test_status_storage_removes_ignored_fields(
        cls: type[DiffBaseStorage]):
    body = Body({
        'a': {
            'b': {
                'c': 'd',
            },
            'e': {
                'f': 'g',
            }
        },
        'h': {
            'i': 'j',
        }
    })
    storage = cls(ignored_fields=['a.b', 'a.e.f.g', 'h', 'k.l.m'])
    essence = storage.build(body=body)

    assert 'a' in essence
    assert 'e' in essence['a']
    assert 'f' in essence['a']['e']

    assert 'b' not in essence['a']
    # 'f' is the inner-most field with a string value 'g', removing field 'g' in 'a.e.f.g' should have no effect
    assert 'g' == essence['a']['e']['f']
    assert 'h' not in essence
    assert 'k' not in essence
