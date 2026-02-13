import copy

import pytest

from kopf._cogs.structs.bodies import RawBody
from kopf._cogs.structs.patches import Patch


def test_error_when_no_body():
    patch = Patch()
    patch['xyz'] = 123
    with pytest.raises(ValueError, match="Cannot build a JSON-patch without the original body"):
        patch.as_json_patch()


def test_no_error_when_no_body_is_needed():
    patch = Patch()
    ops = patch.as_json_patch()
    assert ops == []


def test_body_argument_overrides_original():
    body1 = {'abc': 456}  # leads to ops=remove(abc) & op=add(xyz)
    body2 = {'xyz': 789}  # leads to op=replace(xyz) only
    patch = Patch(body=body1)
    patch['abc'] = None
    patch['xyz'] = 123
    ops = patch.as_json_patch(body2)
    assert ops == [
        {'op': 'replace', 'path': '/xyz', 'value': 123},
    ]


def test_noop_empty_patch():
    body = {'abc': 456}
    patch = Patch(body=body)
    ops = patch.as_json_patch()
    assert ops == []


def test_noop_merge_of_empty_dict():
    body = {'xyz': {'abc': 456}}
    patch = Patch(body=body)
    patch['xyz'] = {}  # nothing to merge here
    ops = patch.as_json_patch()
    assert ops == []


def test_noop_replacement_of_the_key():
    body = {'xyz': 123}
    patch = Patch(body=body)
    patch['xyz'] = 123
    ops = patch.as_json_patch()
    assert ops == []


def test_noop_removal_of_absent_key():
    body = {'abc': 456}
    patch = Patch(body=body)
    patch['xyz'] = None
    ops = patch.as_json_patch()
    assert ops == []


def test_addition_of_the_key():
    body = {'abc': 456}
    patch = Patch(body=body)
    patch['xyz'] = 123
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'add', 'path': '/xyz', 'value': 123},
    ]


def test_replacement_of_the_key():
    body = {'xyz': 456}
    patch = Patch(body=body)
    patch['xyz'] = 123
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'replace', 'path': '/xyz', 'value': 123},
    ]


def test_removal_of_the_key():
    body = {'xyz': 456}
    patch = Patch(body=body)
    patch['xyz'] = None
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'remove', 'path': '/xyz'},
    ]


def test_addition_of_the_subkey():
    body = {'xyz': {'def': 456}}
    patch = Patch(body=body)
    patch['xyz'] = {'abc': 123}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'add', 'path': '/xyz/abc', 'value': 123},
    ]


def test_replacement_of_the_subkey():
    body = {'xyz': {'abc': 456}}
    patch = Patch(body=body)
    patch['xyz'] = {'abc': 123}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'replace', 'path': '/xyz/abc', 'value': 123},
    ]


def test_addition_of_key_with_new_parent():
    body = {}
    patch = Patch(body=body)
    patch['xyz'] = {'abc': 123}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'add', 'path': '/xyz', 'value': {'abc': 123}},
    ]


def test_addition_of_the_sub_subkey_with_existing_parent():
    body = {'xyz': {'uvw': 123}}
    patch = Patch(body=body)
    patch['xyz'] = {'abc': {'def': {'ghi': 456}}}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'add', 'path': '/xyz/abc', 'value': {'def': {'ghi': 456}}},
    ]


def test_removal_of_the_subkey_and_remaining_parent():
    body = {'xyz': {'abc': 456, 'other': '...'}}
    patch = Patch(body=body)
    patch['xyz'] = {'abc': None}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'remove', 'path': '/xyz/abc'},
    ]


def test_removal_of_the_subkey_and_emptied_parent():
    body = {'xyz': {'abc': 456}}
    patch = Patch(body=body)
    patch['xyz'] = {'abc': None}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'remove', 'path': '/xyz'},
    ]


def test_addition_of_list_value():
    body = {'abc': 456}
    patch = Patch(body=body)
    patch['xyz'] = [1, 2, 3]
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'add', 'path': '/xyz', 'value': [1, 2, 3]},
    ]


def test_replacement_of_list_value():
    body = {'xyz': [1, 2, 3]}
    patch = Patch(body=body)
    patch['xyz'] = [4, 5]
    ops = patch.as_json_patch()
    # The jsonpatch library diffs lists element-by-element, not as a whole.
    assert sorted(ops, key=lambda op: op['path']) == [
        {'op': 'replace', 'path': '/xyz/0', 'value': 4},
        {'op': 'replace', 'path': '/xyz/1', 'value': 5},
        {'op': 'remove', 'path': '/xyz/2'},
    ]


def test_multiple_operations():
    body = {'existing': 1, 'toremove': 2}
    patch = Patch(body=body)
    patch['existing'] = 99
    patch['toremove'] = None
    patch['newkey'] = 'hello'
    ops = patch.as_json_patch()
    assert sorted(ops, key=lambda op: op['path']) == [
        {'op': 'replace', 'path': '/existing', 'value': 99},
        {'op': 'add', 'path': '/newkey', 'value': 'hello'},
        {'op': 'remove', 'path': '/toremove'},
    ]


def test_fn_only():
    body = {'items': [1, 2]}
    patch = Patch(body=body, fns=[lambda b: b['items'].append(3)])
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'add', 'path': '/items/2', 'value': 3},
    ]


def test_fn_combined_with_merge_patch():
    body = {'items': [1, 2], 'label': 'old'}
    patch = Patch(body=body, fns=[lambda b: b['items'].append(3)])
    patch['label'] = 'new'
    ops = patch.as_json_patch()
    assert sorted(ops, key=lambda op: op['path']) == [
        {'op': 'add', 'path': '/items/2', 'value': 3},
        {'op': 'replace', 'path': '/label', 'value': 'new'},
    ]


def test_fn_applied_strictly_after_merges():

    def increment(body: RawBody) -> None:
        body['xyz'] += 1

    body = {'xyz': 100}
    patch = Patch(body=body, fns=[increment])
    patch['xyz'] = 200
    ops = patch.as_json_patch()
    assert sorted(ops, key=lambda op: op['path']) == [
        {'op': 'replace', 'path': '/xyz', 'value': 201},
    ]


def test_escaping_of_key():
    body = {'~xyz/test': {'abc': '...', 'other': '...'}}
    patch = Patch(body=body)
    patch['~xyz/test'] = {'abc': None}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'remove', 'path': '/~0xyz~1test/abc'}
    ]


def test_recursive_escape_of_key():
    body = {'x/y/~z': {'a/b/~0c': '...', 'other': '...'}}
    patch = Patch(body=body)
    patch['x/y/~z'] = {'a/b/~0c': None}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'remove', 'path': '/x~1y~1~0z/a~1b~1~00c'},
    ]


def test_does_not_mutate_original_body():
    body = {'spec': {'x': 'original'}}
    patch = Patch(body=body)
    patch['spec'] = {'x': 'modified'}
    body_before = copy.deepcopy(body)
    patch.as_json_patch()
    assert body == body_before


def test_does_not_mutate_original_body_with_fns():
    body = {'spec': {'x': 'original'}}
    patch = Patch(body=body, fns=[lambda b: b.setdefault('extra', 'added')])
    patch['spec'] = {'x': 'modified'}
    body_before = copy.deepcopy(body)
    patch.as_json_patch()
    assert body == body_before
