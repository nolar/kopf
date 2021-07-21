import pytest

from kopf._cogs.structs.bodies import Body
from kopf._core.engines.indexing import OperatorIndexer, OperatorIndexers


@pytest.fixture()
def indexers():
    return OperatorIndexers()


@pytest.fixture()
def index(indexers):
    indexer = OperatorIndexer()
    indexers['index_fn'] = indexer
    return indexer.index


@pytest.fixture()
async def indexed_123(indexers, index, namespace):
    body = {'metadata': {'namespace': namespace, 'name': 'name1'}}
    key = indexers.make_key(Body(body))
    indexers['index_fn'].replace(key, 123)
    assert set(index) == {None}
    assert set(index[None]) == {123}
