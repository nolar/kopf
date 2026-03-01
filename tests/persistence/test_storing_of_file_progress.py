import yaml

import pytest

from kopf._cogs.configs.progress import FileProgressStorage, ProgressRecord
from kopf._cogs.structs.bodies import Body, BodyEssence
from kopf._cogs.structs.ids import HandlerId
from kopf._cogs.structs.patches import Patch

CONTENT_DATA_1 = ProgressRecord(
    started='2020-01-01T00:00:00',
    stopped='2020-12-31T23:59:59',
    delayed='3000-01-01T00:00:00',
    retries=123,
    success=False,
    failure=False,
    message=None,
    subrefs=None,
)

CONTENT_DATA_2 = ProgressRecord(
    started='2021-01-01T00:00:00',
    stopped='2021-12-31T23:59:59',
    delayed='3001-01-01T00:00:00',
    retries=456,
    success=False,
    failure=False,
    message="Some error.",
    subrefs=['sub1', 'sub2'],
)

NAMESPACED_BODY = Body({'metadata': {'namespace': 'ns1', 'name': 'name1', 'uid': 'uid1'}})
CLUSTER_BODY = Body({'metadata': {'name': 'name1', 'uid': 'uid1'}})


#
# Storage creation.
#


def test_file_storage_with_path(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    assert storage._path == tmp_path


def test_file_storage_with_string_path(tmp_path):
    storage = FileProgressStorage(path=str(tmp_path))
    assert storage._path == tmp_path


def test_file_storage_with_prefix(tmp_path):
    storage = FileProgressStorage(path=tmp_path, prefix='my-op.example.com')
    assert storage.prefix == 'my-op.example.com'


def test_file_storage_with_touch_key(tmp_path):
    storage = FileProgressStorage(path=tmp_path, touch_key='my-dummy')
    assert storage.touch_key == 'my-dummy'


#
# Filename building.
#


def test_filename_for_namespaced_resource(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': 'default', 'name': 'my-app', 'uid': 'abc-123'}})
    filepath = storage._build_filename(body)
    assert filepath == tmp_path / 'default-my-app-abc-123.progress.yaml'


def test_filename_for_dotted_name(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': 'default', 'name': 'my.app.v1', 'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert filepath == tmp_path / 'default-my.app.v1-uid1.progress.yaml'


def test_filename_for_cluster_resource(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({'metadata': {'name': 'my-node', 'uid': 'abc-123'}})
    filepath = storage._build_filename(body)
    assert filepath == tmp_path / 'my-node-abc-123.progress.yaml'


def test_filename_for_empty_body(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({})
    filepath = storage._build_filename(body)
    assert filepath is None


def test_filename_for_body_without_name(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({'metadata': {'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert filepath is None


def test_filename_for_body_without_uid(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({'metadata': {'name': 'name1'}})
    filepath = storage._build_filename(body)
    assert filepath is None


def test_filename_escapes_slashes(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': 'ns', 'name': 'a/b', 'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert '/' not in filepath.name
    assert 'a%2Fb' in filepath.name


def test_filename_escapes_double_dots(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': '..', 'name': '..', 'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert filepath.name == '%2E%2E-%2E%2E-uid1.progress.yaml'
    # Must stay flat in the configured directory (no path traversal).
    assert filepath.parent == tmp_path


def test_filename_escapes_double_dots_with_slashes(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': 'ns', 'name': '../path', 'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert '..' not in filepath.name
    assert '%2E%2E%2Fpath' in filepath.name
    assert filepath.parent == tmp_path


def test_filename_escapes_double_dots_in_middle(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': 'ns', 'name': 'a/../b', 'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert '..' not in filepath.name
    assert filepath.parent == tmp_path


def test_filename_escapes_special_characters(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({'metadata': {'namespace': 'ns', 'name': 'a:b@c', 'uid': 'uid1'}})
    filepath = storage._build_filename(body)
    assert '/' not in filepath.name
    assert ':' not in filepath.name
    assert '@' not in filepath.name


#
# Fetching.
#


def test_fetching_from_empty_body_returns_none(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    body = Body({})
    data = storage.fetch(body=body, key=HandlerId('id1'))
    assert data is None


def test_fetching_when_no_file_exists(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    data = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id1'))
    assert data is None


def test_fetching_existing_record(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    filepath = storage._build_filename(NAMESPACED_BODY)
    # Write without None values, as store() does.
    clean_data = {k: v for k, v in CONTENT_DATA_1.items() if v is not None}
    filepath.write_text(yaml.safe_dump({'id1': clean_data}), encoding='utf-8')

    data = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id1'))
    assert data == clean_data


def test_fetching_nonexistent_key_returns_none(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    filepath = storage._build_filename(NAMESPACED_BODY)
    filepath.write_text(yaml.safe_dump({'id1': dict(CONTENT_DATA_1)}), encoding='utf-8')

    data = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id2'))
    assert data is None


#
# Storing.
#


def test_storing_creates_file(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)

    filepath = storage._build_filename(NAMESPACED_BODY)
    assert filepath.exists()
    data = yaml.safe_load(filepath.read_text(encoding='utf-8'))
    expected = {k: v for k, v in CONTENT_DATA_1.items() if v is not None}
    assert data['id1'] == expected


def test_storing_does_not_modify_patch(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    assert not patch


def test_storing_creates_parent_directories(tmp_path):
    storage = FileProgressStorage(path=tmp_path / 'sub' / 'dir')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)

    filepath = storage._build_filename(NAMESPACED_BODY)
    assert filepath.exists()


def test_storing_appends_keys(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id2'), record=CONTENT_DATA_2)

    filepath = storage._build_filename(NAMESPACED_BODY)
    data = yaml.safe_load(filepath.read_text(encoding='utf-8'))
    assert 'id1' in data
    assert 'id2' in data


def test_storing_overwrites_existing_key(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_2)

    filepath = storage._build_filename(NAMESPACED_BODY)
    data = yaml.safe_load(filepath.read_text(encoding='utf-8'))
    expected = {k: v for k, v in CONTENT_DATA_2.items() if v is not None}
    assert data['id1'] == expected


def test_storing_filters_none_values(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    record = ProgressRecord(
        started=None,
        stopped=None,
        delayed=None,
        retries=None,
        success=None,
        failure=None,
        message=None,
        subrefs=None,
    )
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=record)

    filepath = storage._build_filename(NAMESPACED_BODY)
    data = yaml.safe_load(filepath.read_text(encoding='utf-8'))
    assert data['id1'] == {}


def test_storing_with_empty_body_is_noop(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    assert not list(tmp_path.iterdir())


def test_storing_for_cluster_resource(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=CLUSTER_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)

    filepath = storage._build_filename(CLUSTER_BODY)
    assert filepath.exists()
    assert filepath.name == 'name1-uid1.progress.yaml'


#
# Purging.
#


def test_purging_already_empty_body_does_nothing(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    body = Body({})
    storage.purge(body=body, patch=patch, key=HandlerId('id1'))
    assert not patch


def test_purging_when_no_file_exists(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    storage.purge(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'))
    assert not patch


def test_purging_removes_key(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id2'), record=CONTENT_DATA_2)

    storage.purge(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'))

    filepath = storage._build_filename(NAMESPACED_BODY)
    data = yaml.safe_load(filepath.read_text(encoding='utf-8'))
    assert 'id1' not in data
    assert 'id2' in data


def test_purging_last_key_removes_file(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)

    storage.purge(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'))

    filepath = storage._build_filename(NAMESPACED_BODY)
    assert not filepath.exists()


def test_purging_does_not_modify_patch(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)

    patch2 = Patch()
    storage.purge(body=NAMESPACED_BODY, patch=patch2, key=HandlerId('id1'))
    assert not patch2


#
# Touching.
#


@pytest.mark.parametrize('body_data', [
    pytest.param({}, id='without-data'),
    pytest.param({'metadata': {'annotations': {'my-op.example.com/my-dummy': 'something'}}}, id='with-data'),
])
def test_touching_with_payload(tmp_path, body_data):
    storage = FileProgressStorage(path=tmp_path, prefix='my-op.example.com', touch_key='my-dummy')
    patch = Patch()
    body = Body(body_data)
    storage.touch(body=body, patch=patch, value='hello')

    assert patch
    assert patch['metadata']['annotations']['my-op.example.com/my-dummy'] == 'hello'


def test_touching_with_none_when_absent(tmp_path):
    storage = FileProgressStorage(path=tmp_path, prefix='my-op.example.com', touch_key='my-dummy')
    patch = Patch()
    body = Body({})
    storage.touch(body=body, patch=patch, value=None)

    assert not patch


def test_touching_with_none_when_present(tmp_path):
    storage = FileProgressStorage(path=tmp_path, prefix='my-op.example.com', touch_key='my-dummy')
    patch = Patch()
    body = Body({'metadata': {'annotations': {'my-op.example.com/my-dummy': 'something'}}})
    storage.touch(body=body, patch=patch, value=None)

    assert patch
    assert patch['metadata']['annotations']['my-op.example.com/my-dummy'] is None


#
# Clearing.
#


def test_clearing_returns_essence_unchanged_without_annotations(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    essence = BodyEssence(
        spec={'field': 'value'},
        metadata={'labels': {'app': 'test'}},
    )
    result = storage.clear(essence=essence)
    assert result == essence
    # Must be a deep copy, not the same object.
    assert result is not essence


def test_clearing_removes_touch_annotation(tmp_path):
    storage = FileProgressStorage(path=tmp_path, prefix='my-op.example.com')
    essence = BodyEssence(
        spec={'field': 'value'},
        metadata={'annotations': {
            'my-op.example.com/touch-dummy': 'some-value',
            'unrelated-annotation': 'keep-this',
        }},
    )
    result = storage.clear(essence=essence)
    assert 'my-op.example.com/touch-dummy' not in result.get('metadata', {}).get('annotations', {})
    assert result['metadata']['annotations']['unrelated-annotation'] == 'keep-this'


#
# YAML round-trip integrity.
#


def test_yaml_roundtrip_preserves_types(tmp_path):
    storage = FileProgressStorage(path=tmp_path)
    patch = Patch()
    record = ProgressRecord(
        started='2020-01-01T00:00:00',
        stopped='2020-12-31T23:59:59',
        delayed='3000-01-01T00:00:00',
        retries=123,
        success=True,
        failure=False,
        message="Some error.",
        subrefs=['sub1', 'sub2'],
    )
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=record)
    fetched = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id1'))

    assert isinstance(fetched['started'], str)
    assert isinstance(fetched['retries'], int)
    assert isinstance(fetched['success'], bool)
    assert isinstance(fetched['failure'], bool)
    assert isinstance(fetched['message'], str)
    assert isinstance(fetched['subrefs'], list)
    assert fetched['started'] == '2020-01-01T00:00:00'
    assert fetched['retries'] == 123
    assert fetched['success'] is True
    assert fetched['failure'] is False
