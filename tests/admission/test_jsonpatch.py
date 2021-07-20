from kopf._cogs.structs.patches import Patch


def test_addition_of_the_key():
    patch = Patch()
    patch.original = {'abc': 456}
    patch['xyz'] = 123
    jsonpatch = patch.as_json_patch()
    assert jsonpatch == [
        {'op': 'add', 'path': '/xyz', 'value': 123},
    ]


def test_replacement_of_the_key():
    patch = Patch()
    patch.original = {'xyz': 456}
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
    patch.original = {'xyz': {'def': 456}}
    patch['xyz'] = {'abc': 123}
    jsonpatch = patch.as_json_patch()
    assert jsonpatch == [
        {'op': 'add', 'path': '/xyz/abc', 'value': 123},
    ]

def test_replacement_of_the_subkey():
    patch = Patch()
    patch.original = {'xyz': {'abc': 456}}
    patch['xyz'] = {'abc': 123}
    jsonpatch = patch.as_json_patch()
    assert jsonpatch == [
        {'op': 'replace', 'path': '/xyz/abc', 'value': 123},
    ]


def test_addition_of_the_sub_subkey():
    patch = Patch()
    patch.original = {'xyz': {'uvw': 123}}
    patch['xyz'] = {'abc': {'def': {'ghi': 456}}}
    jsonpatch = patch.as_json_patch()
    assert jsonpatch == [
        {'op': 'add', 'path': '/xyz/abc', 'value': {'def': {'ghi': 456}}},
    ]


def test_removal_of_the_subkey():
    patch = Patch()
    patch['xyz'] = {'abc': None}
    jsonpatch = patch.as_json_patch()
    assert jsonpatch == [
        {'op': 'remove', 'path': '/xyz/abc'},
    ]
