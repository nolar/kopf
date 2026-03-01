import json
import sqlite3

import pytest

from kopf._cogs.configs.progress import SQLiteProgressStorage, ProgressRecord
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


def test_sqlite_storage_with_path(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    assert storage._path == tmp_path / 'db.sqlite'


def test_sqlite_storage_with_string_path(tmp_path):
    storage = SQLiteProgressStorage(path=str(tmp_path / 'db.sqlite'))
    assert storage._path == tmp_path / 'db.sqlite'


def test_sqlite_storage_with_prefix(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite', prefix='my-op.example.com')
    assert storage.prefix == 'my-op.example.com'


def test_sqlite_storage_with_touch_key(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite', touch_key='my-dummy')
    assert storage.touch_key == 'my-dummy'


#
# Fetching.
#


def test_fetching_from_empty_body_returns_none(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    body = Body({})
    data = storage.fetch(body=body, key=HandlerId('id1'))
    assert data is None


def test_fetching_when_no_table_exists(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    data = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id1'))
    assert data is None


def test_fetching_existing_record(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)

    data = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id1'))
    expected = {k: v for k, v in CONTENT_DATA_1.items() if v is not None}
    assert data == expected


def test_fetching_nonexistent_key_returns_none(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)

    data = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id2'))
    assert data is None


#
# Storing.
#


def test_storing_creates_table(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)

    conn = sqlite3.connect(str(tmp_path / 'db.sqlite'))
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    conn.close()
    assert ('progress',) in tables


def test_storing_does_not_modify_patch(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    assert not patch


def test_storing_creates_parent_directories(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'sub' / 'dir' / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    assert (tmp_path / 'sub' / 'dir' / 'db.sqlite').exists()


def test_storing_appends_keys(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id2'), record=CONTENT_DATA_2)

    data1 = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id1'))
    data2 = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id2'))
    assert data1 is not None
    assert data2 is not None


def test_storing_overwrites_existing_key(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_2)

    data = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id1'))
    expected = {k: v for k, v in CONTENT_DATA_2.items() if v is not None}
    assert data == expected


def test_storing_filters_none_values(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
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

    data = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id1'))
    assert data == {}


def test_storing_with_empty_body_is_noop(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    assert not (tmp_path / 'db.sqlite').exists()


def test_storing_for_cluster_resource(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=CLUSTER_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)

    data = storage.fetch(body=CLUSTER_BODY, key=HandlerId('id1'))
    assert data is not None


def test_storing_isolates_namespaced_and_cluster_resources(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=CLUSTER_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_2)

    data_ns = storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id1'))
    data_cl = storage.fetch(body=CLUSTER_BODY, key=HandlerId('id1'))
    expected_ns = {k: v for k, v in CONTENT_DATA_1.items() if v is not None}
    expected_cl = {k: v for k, v in CONTENT_DATA_2.items() if v is not None}
    assert data_ns == expected_ns
    assert data_cl == expected_cl


#
# Purging.
#


def test_purging_already_empty_body_does_nothing(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    body = Body({})
    storage.purge(body=body, patch=patch, key=HandlerId('id1'))
    assert not patch


def test_purging_when_no_table_exists(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.purge(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'))
    assert not patch


def test_purging_removes_key(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'), record=CONTENT_DATA_1)
    storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id2'), record=CONTENT_DATA_2)

    storage.purge(body=NAMESPACED_BODY, patch=patch, key=HandlerId('id1'))

    assert storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id1')) is None
    assert storage.fetch(body=NAMESPACED_BODY, key=HandlerId('id2')) is not None


def test_purging_does_not_modify_patch(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
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
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite', prefix='my-op.example.com', touch_key='my-dummy')
    patch = Patch()
    body = Body(body_data)
    storage.touch(body=body, patch=patch, value='hello')

    assert patch
    assert patch['metadata']['annotations']['my-op.example.com/my-dummy'] == 'hello'


def test_touching_with_none_when_absent(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite', prefix='my-op.example.com', touch_key='my-dummy')
    patch = Patch()
    body = Body({})
    storage.touch(body=body, patch=patch, value=None)

    assert not patch


def test_touching_with_none_when_present(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite', prefix='my-op.example.com', touch_key='my-dummy')
    patch = Patch()
    body = Body({'metadata': {'annotations': {'my-op.example.com/my-dummy': 'something'}}})
    storage.touch(body=body, patch=patch, value=None)

    assert patch
    assert patch['metadata']['annotations']['my-op.example.com/my-dummy'] is None


#
# Clearing.
#


def test_clearing_returns_essence_unchanged_without_annotations(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
    essence = BodyEssence(
        spec={'field': 'value'},
        metadata={'labels': {'app': 'test'}},
    )
    result = storage.clear(essence=essence)
    assert result == essence
    assert result is not essence


def test_clearing_removes_touch_annotation(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite', prefix='my-op.example.com')
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
# JSON round-trip integrity.
#


def test_json_roundtrip_preserves_types(tmp_path):
    storage = SQLiteProgressStorage(path=tmp_path / 'db.sqlite')
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
