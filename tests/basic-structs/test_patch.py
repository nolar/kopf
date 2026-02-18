from kopf._cogs.structs.patches import Patch

# === Patch.__init__ ===


def test_patch_init_no_args():
    patch = Patch()
    assert dict(patch) == {}


def test_patch_init_none_src():
    patch = Patch(None)
    assert dict(patch) == {}


def test_patch_init_body_only():
    body = {'metadata': {'name': 'obj'}}
    patch = Patch(body=body)
    assert dict(patch) == {}


def test_patch_init_body_stored_for_json_patch():
    body = {'abc': 456}
    patch = Patch(body=body)
    patch['abc'] = 789
    ops = patch.as_json_patch()
    assert ops == [{'op': 'replace', 'path': '/abc', 'value': 789}]


# === Patch.__bool__ ===


def test_patch_bool_empty_is_falsy():
    patch = Patch()
    assert not patch


def test_patch_bool_with_dict_is_truthy():
    patch = Patch({'a': 'b'})
    assert patch


# === Patch.__repr__ ===


def test_patch_repr_empty():
    patch = Patch()
    assert repr(patch) == "Patch()"


def test_patch_repr_dict_only():
    patch = Patch({'a': 'b'})
    assert repr(patch) == "Patch({'a': 'b'})"
