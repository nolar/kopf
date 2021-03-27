from kopf.structs.patches import Patch


def test_addition_of_the_key():
    patch = Patch()
    patch['xyz'] = 123
    jsonpatch = patch.as_json_patch()
    assert jsonpatch == [
        {'op': 'replace', 'path': '/xyz', 'value': 123},
    ]


def test_removal_of_the_key():
    patch = Patch()
    patch['xyz'] = None
    jsonpatch = patch.as_json_patch()
    assert jsonpatch == [
        {'op': 'remove', 'path': '/xyz'},
    ]


def test_addition_of_the_subkey():
    patch = Patch()
    patch['xyz'] = {'abc': 123}
    jsonpatch = patch.as_json_patch()
    assert jsonpatch == [
        {'op': 'replace', 'path': '/xyz/abc', 'value': 123},
    ]


def test_removal_of_the_subkey():
    patch = Patch()
    patch['xyz'] = {'abc': None}
    jsonpatch = patch.as_json_patch()
    assert jsonpatch == [
        {'op': 'remove', 'path': '/xyz/abc'},
    ]
