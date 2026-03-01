import json
import sqlite3

import pytest

from kopf._cogs.configs.diffbase import SQLiteDiffBaseStorage
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


def test_sqlite_storage_with_path(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    assert storage._path == tmp_path / 'db.sqlite'


def test_sqlite_storage_with_string_path(tmp_path):
    storage = SQLiteDiffBaseStorage(path=str(tmp_path / 'db.sqlite'))
    assert storage._path == tmp_path / 'db.sqlite'


#
# Fetching.
#


def test_fetching_from_empty_body_returns_none(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    body = Body({})
    data = storage.fetch(body=body)
    assert data is None


def test_fetching_when_no_table_exists(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    data = storage.fetch(body=NAMESPACED_BODY)
    assert data is None


def test_fetching_existing_essence(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)

    data = storage.fetch(body=NAMESPACED_BODY)
    assert data == ESSENCE_DATA_1


#
# Storing.
#


def test_storing_creates_table(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)

    conn = sqlite3.connect(str(tmp_path / 'db.sqlite'))
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    conn.close()
    assert ('diffbase',) in tables


def test_storing_does_not_modify_patch(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)
    assert not patch


def test_storing_creates_parent_directories(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'sub' / 'dir' / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)
    assert (tmp_path / 'sub' / 'dir' / 'db.sqlite').exists()


def test_storing_overwrites_old_content(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_2)

    data = storage.fetch(body=NAMESPACED_BODY)
    assert data == ESSENCE_DATA_2


def test_storing_with_empty_body_is_noop(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    body = Body({})
    storage.store(body=body, patch=patch, essence=ESSENCE_DATA_1)
    assert not (tmp_path / 'db.sqlite').exists()


def test_storing_for_cluster_resource(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=CLUSTER_BODY, patch=patch, essence=ESSENCE_DATA_1)

    data = storage.fetch(body=CLUSTER_BODY)
    assert data == ESSENCE_DATA_1


def test_storing_isolates_namespaced_and_cluster_resources(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()
    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)
    storage.store(body=CLUSTER_BODY, patch=patch, essence=ESSENCE_DATA_2)

    data_ns = storage.fetch(body=NAMESPACED_BODY)
    data_cl = storage.fetch(body=CLUSTER_BODY)
    assert data_ns == ESSENCE_DATA_1
    assert data_cl == ESSENCE_DATA_2


#
# Shared database file.
#


def test_shared_database_between_storages(tmp_path):
    db_path = tmp_path / 'shared.sqlite'
    progress_storage = __import__('kopf._cogs.configs.progress', fromlist=['SQLiteProgressStorage']).SQLiteProgressStorage(path=db_path)
    diffbase_storage = SQLiteDiffBaseStorage(path=db_path)
    patch = Patch()

    from kopf._cogs.configs.progress import ProgressRecord
    from kopf._cogs.structs.ids import HandlerId

    record = ProgressRecord(started='2020-01-01T00:00:00', retries=1, success=True)
    progress_storage.store(body=NAMESPACED_BODY, patch=patch, key=HandlerId('h1'), record=record)
    diffbase_storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)

    conn = sqlite3.connect(str(db_path))
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert 'progress' in tables
    assert 'diffbase' in tables

    assert progress_storage.fetch(body=NAMESPACED_BODY, key=HandlerId('h1')) is not None
    assert diffbase_storage.fetch(body=NAMESPACED_BODY) == ESSENCE_DATA_1


#
# JSON round-trip integrity.
#


def test_json_roundtrip_preserves_types(tmp_path):
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
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
    storage = SQLiteDiffBaseStorage(path=tmp_path / 'db.sqlite')
    patch = Patch()

    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_1)
    fetched = storage.fetch(body=NAMESPACED_BODY)
    assert fetched == ESSENCE_DATA_1

    storage.store(body=NAMESPACED_BODY, patch=patch, essence=ESSENCE_DATA_2)
    fetched = storage.fetch(body=NAMESPACED_BODY)
    assert fetched == ESSENCE_DATA_2
