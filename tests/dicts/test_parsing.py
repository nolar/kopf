import pytest

from kopf.structs.dicts import parse_field


def test_from_none():
    path = parse_field(None)
    assert isinstance(path, tuple)
    assert len(path) == 0


def test_from_string_one_level():
    path = parse_field('field')
    assert isinstance(path, tuple)
    assert path == ('field',)


def test_from_string_two_levels():
    path = parse_field('field.subfield')
    assert isinstance(path, tuple)
    assert path == ('field', 'subfield')


def test_from_list():
    path = parse_field(['field' , 'subfield'])
    assert isinstance(path, tuple)
    assert path == ('field', 'subfield')


def test_from_tuple():
    path = parse_field(('field' , 'subfield'))
    assert isinstance(path, tuple)
    assert path == ('field', 'subfield')


@pytest.mark.parametrize('val', [dict(), set(), frozenset()])
def test_from_others_fails(val):
    with pytest.raises(ValueError):
        parse_field(val)
