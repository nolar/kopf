import pytest

from kopf.reactor.indexing import OperatorIndexer, OperatorIndexers
from kopf.structs.bodies import Body


@pytest.fixture()
def indexers():
    return OperatorIndexers()


@pytest.fixture()
def index(indexers):
    indexer = OperatorIndexer()
    indexers['index_fn'] = indexer
    return indexer.index


@pytest.fixture()
async def indexed_123(indexers, index):
    body = {'metadata': {'namespace': 'ns1', 'name': 'name1'}}
    key = indexers.make_key(Body(body))
    indexers['index_fn'].replace(key, 123)
    assert set(index) == {None}
    assert set(index[None]) == {123}
