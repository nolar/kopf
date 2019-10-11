import copy

import pytest

from kopf.structs.dicts import cherrypick


def test_overrides_existing_keys():
    src = {'ignored-key': 'src-val', 'tested-key': 'src-val'}
    dst = {'ignored-key': 'dst-val', 'tested-key': 'dst-val'}
    cherrypick(src=src, dst=dst, fields=['tested-key'])
    assert dst == {'ignored-key': 'dst-val', 'tested-key': 'src-val'}


def test_adds_absent_dst_keys():
    src = {'ignored-key': 'src-val', 'tested-key': 'src-val'}
    dst = {'ignored-key': 'dst-val'}
    cherrypick(src=src, dst=dst, fields=['tested-key'])
    assert dst == {'ignored-key': 'dst-val', 'tested-key': 'src-val'}


def test_skips_absent_src_keys():
    src = {'ignored-key': 'src-val'}
    dst = {'ignored-key': 'dst-val', 'tested-key': 'dst-val'}
    cherrypick(src=src, dst=dst, fields=['tested-key'])
    assert dst == {'ignored-key': 'dst-val', 'tested-key': 'dst-val'}


def test_overrides_existing_subkeys():
    src = {'sub': {'ignored-key': 'src-val', 'tested-key': 'src-val'}}
    dst = {'sub': {'ignored-key': 'dst-val', 'tested-key': 'dst-val'}}
    cherrypick(src=src, dst=dst, fields=['sub.tested-key'])
    assert dst == {'sub': {'ignored-key': 'dst-val', 'tested-key': 'src-val'}}


def test_adds_absent_dst_subkeys():
    src = {'sub': {'ignored-key': 'src-val', 'tested-key': 'src-val'}}
    dst = {'sub': {'ignored-key': 'dst-val'}}
    cherrypick(src=src, dst=dst, fields=['sub.tested-key'])
    assert dst == {'sub': {'ignored-key': 'dst-val', 'tested-key': 'src-val'}}


def test_skips_absent_src_subkeys():
    src = {'sub': {'ignored-key': 'src-val'}}
    dst = {'sub': {'ignored-key': 'dst-val', 'tested-key': 'dst-val'}}
    cherrypick(src=src, dst=dst, fields=['sub.tested-key'])
    assert dst == {'sub': {'ignored-key': 'dst-val', 'tested-key': 'dst-val'}}


def test_ensures_dst_subdicts():
    src = {'sub': {'ignored-key': 'src-val', 'tested-key': 'src-val'}}
    dst = {}
    cherrypick(src=src, dst=dst, fields=['sub.tested-key'])
    assert dst == {'sub': {'tested-key': 'src-val'}}


def test_fails_on_nonmapping_src_key():
    src = {'sub': 'scalar-value'}
    dst = {'sub': {'ignored-key': 'src-val', 'tested-key': 'src-val'}}
    with pytest.raises(TypeError):
        cherrypick(src=src, dst=dst, fields=['sub.tested-key'])


def test_fails_on_nonmapping_dst_key():
    src = {'sub': {'ignored-key': 'src-val', 'tested-key': 'src-val'}}
    dst = {'sub': 'scalar-value'}
    with pytest.raises(TypeError):
        cherrypick(src=src, dst=dst, fields=['sub.tested-key'])


def test_exact_object_picked_by_default():
    src = {'tested-key': {'key': 'val'}}
    dst = {}
    cherrypick(src=src, dst=dst, fields=['tested-key'])

    assert dst == {'tested-key': {'key': 'val'}}
    assert dst['tested-key'] == src['tested-key']
    assert dst['tested-key'] is src['tested-key']

    src['tested-key']['key'] = 'replaced-val'
    assert dst['tested-key']['key'] == 'replaced-val'


def test_copied_object_picked_on_request():
    src = {'tested-key': {'key': 'val'}}
    dst = {}
    cherrypick(src=src, dst=dst, fields=['tested-key'], picker=copy.copy)

    assert dst == {'tested-key': {'key': 'val'}}
    assert dst['tested-key'] == src['tested-key']
    assert dst['tested-key'] is not src['tested-key']

    src['tested-key']['key'] = 'another-val'
    assert dst['tested-key']['key'] == 'val'
