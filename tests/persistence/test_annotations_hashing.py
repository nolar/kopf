import json

import pytest

from kopf.storage.conventions import StorageKeyFormingConvention
from kopf.storage.diffbase import AnnotationsDiffBaseStorage
from kopf.storage.progress import AnnotationsProgressStorage, ProgressRecord, SmartProgressStorage
from kopf.structs.bodies import Body
from kopf.structs.ids import HandlerId
from kopf.structs.patches import Patch

ANNOTATIONS_POPULATING_STORAGES = [
    AnnotationsProgressStorage,
    SmartProgressStorage,
]
STORAGE_KEY_FORMING_CLASSES = [
    StorageKeyFormingConvention,
    AnnotationsProgressStorage,
    AnnotationsDiffBaseStorage,
]

CONTENT_DATA = ProgressRecord(
    started='2020-01-01T00:00:00',
    stopped='2020-12-31T23:59:59',
    delayed='3000-01-01T00:00:00',
    retries=0,
    success=False,
    failure=False,
    message=None,
)

CONTENT_JSON = json.dumps(CONTENT_DATA, separators=(',', ':'))

COMMON_KEYS = [
    # For character replacements (only those that happen in our own ids, not all of them).
    ['my-operator.example.com', 'a_b.c-d/e', 'my-operator.example.com/a_b.c-d.e'],
]

V1_KEYS = [
    # For V1 length cutting. Hint: the prefix length is 23, the remaining space is 63 - 23 - 1 = 39.
    # The suffix itself (if appended) takes 9, so it is 30 left. The same math for no prefix.
    ['my-operator.example.com', 'x', 'my-operator.example.com/x'],
    ['my-operator.example.com', 'x' * 39, 'my-operator.example.com/' + 'x' * 39],
    ['my-operator.example.com', 'x' * 40, 'my-operator.example.com/' + 'x' * 30 + 'xx-tEokcg'],
    ['my-operator.example.com', 'y' * 40, 'my-operator.example.com/' + 'y' * 30 + 'yy-VZlvhw'],
    ['my-operator.example.com', 'z' * 40, 'my-operator.example.com/' + 'z' * 30 + 'zz-LlPQyA'],

    # For special chars in base64 encoding ("+" and "/"), which are not compatible with K8s.
    # The numbers are found empirically so that both "/" and "+" are found in the base64'ed digest.
    ['my-operator.example.com', 'fn' * 323, 'my-operator.example.com/' + 'fn' * 15 + 'fn-Az-r.g'],
]

V2_KEYS = [
    # For V2 length cutting: 63 for the name part only, not the whole annotation.
    # The suffix itself (if appended) takes 9, so it is 63-9=54 left.
    ['my-operator.example.com', 'x', 'my-operator.example.com/x'],
    ['my-operator.example.com', 'x' * 63, 'my-operator.example.com/' + 'x' * 63],
    ['my-operator.example.com', 'x' * 64, 'my-operator.example.com/' + 'x' * 54 + 'xx-SItAqA'],
    ['my-operator.example.com', 'y' * 64, 'my-operator.example.com/' + 'y' * 54 + 'yy-0d251g'],
    ['my-operator.example.com', 'z' * 64, 'my-operator.example.com/' + 'z' * 54 + 'zz-E7wvIA'],

    # For special chars in base64 encoding ("+" and "/"), which are not compatible with K8s.
    # The numbers are found empirically so that both "/" and "+" are found in the base64'ed digest.
    ['my-operator.example.com', 'fn' * 323, 'my-operator.example.com/' + 'fn' * 27 + 'fn-Az-r.g'],
]


@pytest.mark.parametrize('cls', STORAGE_KEY_FORMING_CLASSES)
def test_keys_for_all_versions(cls):
    storage = cls(v1=True, prefix='kopf.zalando.org')
    v1_key = storage.make_v1_key('.' * 64)
    v2_key = storage.make_v2_key('.' * 64)
    assert v1_key != v2_key  # prerequisite
    keys = storage.make_keys('.' * 64)
    assert len(list(keys)) == 2
    assert v1_key in keys
    assert v2_key in keys


@pytest.mark.parametrize('cls', STORAGE_KEY_FORMING_CLASSES)
def test_keys_deduplication(cls):
    storage = cls(v1=True, prefix='kopf.zalando.org')
    v1_key = storage.make_v1_key('...')
    v2_key = storage.make_v2_key('...')
    assert v1_key == v2_key  # prerequisite
    keys = storage.make_keys('...')
    assert len(list(keys)) == 1
    assert v1_key in keys
    assert v2_key in keys


@pytest.mark.parametrize('kind, owners, expected', [
    pytest.param('ReplicaSet', [{'kind': 'Deployment'}], 'kopf.dev/xyz-ofDRS', id='DRS'),
    pytest.param('ReplicaSet', [{'kind': 'OtherOwner'}], 'kopf.dev/xyz', id='not-deployment'),
    pytest.param('OtherKind', [{'kind': 'Deployment'}], 'kopf.dev/xyz', id='not-replicaset'),
])
@pytest.mark.parametrize('cls', STORAGE_KEY_FORMING_CLASSES)
def test_keys_of_replicaset_owned_by_deployment(cls, kind, owners, expected):
    storage = cls(v1=True, prefix='kopf.dev')
    body = Body({'kind': kind, 'metadata': {'ownerReferences': owners}})
    keys = storage.make_keys('xyz', body=body)
    assert set(keys) == {expected}


@pytest.mark.parametrize('prefix, provided_key, expected_key', COMMON_KEYS + V1_KEYS)
@pytest.mark.parametrize('cls', STORAGE_KEY_FORMING_CLASSES)
def test_key_hashing_v1(cls, prefix, provided_key, expected_key):
    storage = cls(v1=True, prefix=prefix)
    returned_key = storage.make_v1_key(provided_key)
    assert returned_key == expected_key


@pytest.mark.parametrize('prefix, provided_key, expected_key', COMMON_KEYS + V2_KEYS)
@pytest.mark.parametrize('cls', STORAGE_KEY_FORMING_CLASSES)
def test_key_hashing_v2(cls, prefix, provided_key, expected_key):
    storage = cls(v1=True, prefix=prefix)
    returned_key = storage.make_v2_key(provided_key)
    assert returned_key == expected_key


@pytest.mark.parametrize('prefix, provided_key, expected_key', COMMON_KEYS + V1_KEYS + V2_KEYS)
@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_keys_hashed_on_fetching(cls, prefix, provided_key, expected_key):
    storage = cls(prefix=prefix)
    body = Body({'metadata': {'annotations': {expected_key: CONTENT_JSON}}})
    record = storage.fetch(body=body, key=HandlerId(provided_key))
    assert record is not None
    assert record == CONTENT_DATA


@pytest.mark.parametrize('prefix, provided_key, expected_key', COMMON_KEYS + V1_KEYS + V2_KEYS)
@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_keys_normalized_on_storing(cls, prefix, provided_key, expected_key):
    storage = cls(prefix=prefix)
    patch = Patch()
    body = Body({'metadata': {'annotations': {expected_key: 'null'}}})
    storage.store(body=body, patch=patch, key=HandlerId(provided_key), record=CONTENT_DATA)
    assert expected_key in patch.metadata.annotations


@pytest.mark.parametrize('prefix, provided_key, expected_key', COMMON_KEYS + V1_KEYS + V2_KEYS)
@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_keys_normalized_on_purging(cls, prefix, provided_key, expected_key):
    storage = cls(prefix=prefix)
    patch = Patch()
    body = Body({'metadata': {'annotations': {expected_key: 'null'}}})
    storage.purge(body=body, patch=patch, key=HandlerId(provided_key))
    assert expected_key in patch.metadata.annotations


@pytest.mark.parametrize('prefix, provided_key, expected_key', COMMON_KEYS + V1_KEYS + V2_KEYS)
@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES)
def test_keys_normalized_on_touching(cls, prefix, provided_key, expected_key):
    storage = cls(prefix=prefix, touch_key=provided_key)
    patch = Patch()
    body = Body({})
    storage.touch(body=body, patch=patch, value='irrelevant')
    assert expected_key in patch.metadata.annotations


@pytest.mark.parametrize('cls', ANNOTATIONS_POPULATING_STORAGES + STORAGE_KEY_FORMING_CLASSES)
def test_warning_on_long_prefix(cls):
    with pytest.warns(UserWarning, match=r"The annotations prefix is too long"):
        cls(v1=True, prefix='x' * (253 - 63))
