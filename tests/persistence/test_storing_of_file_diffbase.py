import yaml

import pytest

from kopf._cogs.configs.diffbase import FileDiffBaseStorage
from kopf._cogs.structs.bodies import Body, BodyEssence
from kopf._cogs.structs.patches import Patch

NAMESPACED_BODY = Body({'metadata': {'namespace': 'ns1', 'name': 'name1', 'uid': 'uid1'}})
CLUSTER_BODY = Body({'metadata': {'name': 'name1', 'uid': 'uid1'}})

ESSENCE_DATA_1 = BodyEssence(
    spec={
        'string-field': 'value1',
        'integer-field': 123,
        'float-field': 123.456,
        'false-field': False,
        'true-field': True,
    },
)

ESSENCE_DATA_2 = BodyEssence(
    spec={
        'hello': 'world',
        'the-cake': False,
    },
)


#
# Storage creation.
#


def test_file_storage_with_path(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    assert storage._path == tmp_path


def test_file_storage_with_string_path(tmp_path):
    storage = FileDiffBaseStorage(path=str(tmp_path))
    assert storage._path == tmp_path


#
# Filename building.
#


def test_filename_for_namespaced_resource(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': 'default', 'name': 'my-app', 'uid': 'abc-123'}})
    filepath = storage._build_filename(body)
    assert filepath == tmp_path / 'default-my-app-abc-123.diffbase.yaml'


def test_filename_for_cluster_resource(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    body = Body({'metadata': {'name': 'my-node', 'uid': 'abc-123'}})
    filepath = storage._build_filename(body)
    assert filepath == tmp_path / 'my-node-abc-123.diffbase.yaml'


def test_filename_for_empty_body(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    body = Body({})
    filepath = storage._build_filename(body)
    assert filepath is None


def test_filename_for_body_without_name(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    body = Body({'metadata': {'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert filepath is None


def test_filename_for_body_without_uid(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    body = Body({'metadata': {'name': 'name1'}})
    filepath = storage._build_filename(body)
    assert filepath is None


def test_filename_escapes_slashes(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': 'ns', 'name': 'a/b', 'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert '/' not in filepath.name
    assert 'a%2Fb' in filepath.name


def test_filename_escapes_double_dots(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': '..', 'name': '..', 'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert filepath.name == '%2E%2E-%2E%2E-uid1.diffbase.yaml'
    assert filepath.parent == tmp_path


def test_filename_escapes_special_characters(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': 'ns', 'name': 'a:b@c', 'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert '/' not in filepath.name
    assert ':' not in filepath.name
    assert '@' not in filepath.name


#
# Fetching.
#


def test_fetching_from_empty_body_returns_none(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    body = Body({})
    data = storage.fetch(body=body)
    assert data is None


def test_fetching_when_no_file_exists(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    data = storage.fetch(body=NAMESPACED_BODY)
    assert data is None


def test_fetching_existing_essence(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    filepath = storage._build_filename(NAMESPACED_BODY)
    filepath.write_text(yaml.safe_dump(dict(ESSENCE_DATA_1)), encoding='utf-8')

    data = storage.fetch(body=NAMESPACED_BODY)
    assert data == ESSENCE_DATA_1


#
# Storing.
#


def test_storing_creates_file(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    patch = Patch()
    body = Body({'metadata': {'namespace': 'ns1', 'name': 'name1', 'uid': 'uid1'}})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)

    filepath = storage._build_filename(body)
    assert filepath.exists()
    data = yaml.safe_load(filepath.read_text(encoding='utf-8'))
    assert data == ESSENCE_DATA_1


def test_storing_does_not_modify_patch(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)
    assert not patch


def test_storing_creates_parent_directories(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path / 'sub' / 'dir')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)

    filepath = storage._build_filename(NAMESPACED_BODY)
    assert filepath.exists()


def test_storing_overwrites_old_content(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_2)

    filepath = storage._build_filename(NAMESPACED_BODY)
    data = yaml.safe_load(filepath.read_text(encoding='utf-8'))
    assert data == ESSENCE_DATA_2


def test_storing_with_empty_body_is_noop(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)
    assert not list(tmp_path.iterdir())


def test_storing_for_cluster_resource(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=CLUSTER_BODY, patch=patch, essence=ESSENCE_DATA_1)

    filepath = storage._build_filename(CLUSTER_BODY)
    assert filepath.exists()
    assert filepath.name == 'name1-uid1.diffbase.yaml'


#
# YAML round-trip integrity.
#


def test_yaml_roundtrip_preserves_types(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    patch = Patch()
    essence = BodyEssence(
        spec={
            'string-field': 'value1',
            'integer-field': 123,
            'float-field': 123.456,
            'false-field': False,
            'true-field': True,
            'list-field': ['a', 'b', 'c'],
            'nested-field': {'key': 'value'},
        },
    )
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=essence)
    fetched = storage.fetch(body=NAMESPACED_BODY)

    assert fetched['spec']['string-field'] == 'value1'
    assert fetched['spec']['integer-field'] == 123
    assert fetched['spec']['float-field'] == 123.456
    assert fetched['spec']['false-field'] is False
    assert fetched['spec']['true-field'] is True
    assert fetched['spec']['list-field'] == ['a', 'b', 'c']
    assert fetched['spec']['nested-field'] == {'key': 'value'}


def test_fetch_and_store_roundtrip(tmp_path):
    storage = FileDiffBaseStorage(path=tmp_path)
    patch = Patch()

    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)
    fetched = storage.fetch(body=NAMESPACED_BODY)
    assert fetched == ESSENCE_DATA_1

    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_2)
    fetched = storage.fetch(body=NAMESPACED_BODY)
    assert fetched == ESSENCE_DATA_2
