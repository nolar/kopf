from kopf._cogs.structs.patches import Patch


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
    patch = Patch()
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


def test_addition_of_the_sub_subkey():
    body = {'xyz': {'uvw': 123}}
    patch = Patch(body=body)
    patch['xyz'] = {'abc': {'def': {'ghi': 456}}}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'add', 'path': '/xyz/abc', 'value': {'def': {'ghi': 456}}},
    ]


def test_removal_of_the_subkey():
    patch = Patch()
    patch['xyz'] = {'abc': None}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'remove', 'path': '/xyz/abc'},
    ]


def test_escaping_of_key():
    patch = Patch()
    patch['~xyz/test'] = {'abc': None}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'remove', 'path': '/~0xyz~1test/abc'}
    ]


def test_recursive_escape_of_key():
    patch = Patch()
    patch['x/y/~z'] = {'a/b/~0c': None}
    ops = patch.as_json_patch()
    assert ops == [
        {'op': 'remove', 'path': '/x~1y~1~0z/a~1b~1~00c'},
    ]
