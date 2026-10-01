from typing import Any

import pytest

from kopf._cogs.configs.arrays import SelectorKey, SelectorMapping
from kopf._cogs.structs.references import EVERYTHING, Resource, Selector


def sample_fn(resource: Resource) -> bool:
    return False


def test_array_creation_empty():
    m = SelectorMapping()
    assert not m
    assert len(m) == 0
    assert list(m) == []
    assert repr(m) == "SelectorMapping({})"


def test_array_creation_filled():
    m = SelectorMapping({Selector('kex'): 'xyz'})
    assert m
    assert len(m) == 1
    assert list(m) == [Selector('kex')]
    assert m['kex'] == 'xyz'
    assert repr(m) == "SelectorMapping({Selector(any_name='kex'): 'xyz'})"


@pytest.mark.parametrize('key', [
    pytest.param('kopfexamples', id='str'),
    pytest.param(('kopf.dev', 'kopfexamples'), id='tuple2'),
    pytest.param(('kopf.dev', 'v1', 'kopfexamples'), id='tuple3'),
    pytest.param(sample_fn, id='fn'),
    pytest.param(EVERYTHING, id='everything'),
    pytest.param(Selector('kopfexamples'), id='selector'),
])
def test_array_on_unknown_keys(key: SelectorKey):
    m = SelectorMapping()
    with pytest.raises(KeyError):
        m[key]
    with pytest.raises(KeyError):
        del m[key]
    assert key not in m


@pytest.mark.parametrize('key', [
    object(),
    123,
    True,
    False,
    Resource('kopf.dev', 'v1', 'kopfexamples'),  # just in case; the temptation is big
])
def test_array_on_unsupported_keys(key: Any):
    m = SelectorMapping()
    with pytest.raises(TypeError, match="Unsupported selector type"):
        m[key]
    with pytest.raises(TypeError, match="Unsupported selector type"):
        m[key] = 'xyz'
    with pytest.raises(TypeError, match="Unsupported selector type"):
        del m[key]
    with pytest.raises(TypeError, match="Unsupported selector type"):
        key in m


# The detailed parsing is tested elsewhere; here, just smoke-test it is utilised.
@pytest.mark.parametrize('key, expected', [
    pytest.param('kopfexamples', Selector(any_name='kopfexamples'), id='str'),
    pytest.param(('kopf.dev', 'kopfexamples'), Selector(group='kopf.dev', any_name='kopfexamples'), id='tuple2'),
    pytest.param(('kopf.dev', 'v1', 'kopfexamples'), Selector(group='kopf.dev', version='v1', any_name='kopfexamples'), id='tuple3'),
    pytest.param(sample_fn, Selector(sample_fn), id='tuple3'),
    pytest.param(EVERYTHING, Selector(EVERYTHING), id='everything'),
    pytest.param(Selector('kex'), Selector('kex'), id='selector'),
])
def test_array_parses_selectors(key: Selector | SelectorKey, expected: Selector):
    m = SelectorMapping()
    m[key] = 'xyz'
    assert list(m) == [expected]


@pytest.mark.parametrize('resource, expected', [
    pytest.param(Resource('kopf.dev', 'v1', 'kopfexamples'), ['grp', 'vers', 'name'], id='full'),
    pytest.param(Resource('kopf.dev', 'v1', 'others'), ['grp', 'vers'], id='alt-name'),
    pytest.param(Resource('kopf.dev', 'v2', 'kopfexamples'), ['grp', 'name'], id='alt-vers'),
    pytest.param(Resource('example.com', 'v1', 'kopfexamples'), ['name'], id='alt-group'),
    pytest.param(Resource('example.com', 'v2', 'others'), [], id='others'),
])
def test_array_collection_keeps_order(resource: Resource, expected: list[str]):
    m = SelectorMapping()
    m['kopf.dev', EVERYTHING] = 'grp'
    m['kopf.dev/v1', EVERYTHING] = 'vers'
    m['kopfexamples'] = 'name'
    m[EVERYTHING] = 'always'
    results = m.collect(resource)
    assert results == expected + ['always']


def test_array_collection_keeps_duplicates():
    m = SelectorMapping()
    m['kopf.dev', EVERYTHING] = 'xyz'
    m['kopfexamples'] = 'xyz'
    m[EVERYTHING] = 'xyz'
    results = m.collect(Resource('kopf.dev', 'v1', 'kopfexamples'))
    assert results == ['xyz', 'xyz', 'xyz']
