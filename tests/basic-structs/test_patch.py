from kopf._cogs.structs.patches import Patch

# === Patch.__init__ ===


def test_patch_init_no_args():
    patch = Patch()
    assert dict(patch) == {}
    assert len(patch.fns) == 0


def test_patch_init_none_src():
    patch = Patch(None)
    assert dict(patch) == {}
    assert len(patch.fns) == 0


def test_patch_init_body_only():
    body = {'metadata': {'name': 'obj'}}
    patch = Patch(body=body)
    assert dict(patch) == {}
    assert len(patch.fns) == 0


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


def test_patch_bool_with_fns_only_is_truthy():
    patch = Patch(fns=[lambda body: None])
    assert patch


def test_patch_bool_with_dict_and_fns_is_truthy():
    patch = Patch({'a': 'b'}, fns=[lambda body: None])
    assert patch


# === Patch.__repr__ ===


def test_patch_repr_empty():
    patch = Patch()
    assert repr(patch) == "Patch()"


def test_patch_repr_dict_only():
    patch = Patch({'a': 'b'})
    assert repr(patch) == "Patch({'a': 'b'})"


def test_patch_repr_fns_only():
    patch = Patch(fns=[lambda body: None])
    r = repr(patch)
    assert r.startswith("Patch(fns=[")
    assert r.endswith("])")


def test_patch_repr_dict_and_fns():
    patch = Patch({'a': 'b'}, fns=[lambda body: None])
    r = repr(patch)
    assert r.startswith("Patch({'a': 'b'}, fns=[")
    assert r.endswith("])")


# === Patch.fns property and inheritance ===


def test_patch_fns_default_empty():
    patch = Patch()
    assert len(patch.fns) == 0


def test_patch_fns_from_constructor():
    fn1 = lambda body: None
    fn2 = lambda body: None
    patch = Patch(fns=[fn1, fn2])
    assert list(patch.fns) == [fn1, fn2]


def test_patch_inherits_fns_from_src_patch():
    fn1 = lambda body: None
    p1 = Patch(fns=[fn1])
    p2 = Patch(p1)
    assert list(p2.fns) == [fn1]


def test_patch_combines_inherited_and_own_fns():
    fn1 = lambda body: None
    fn2 = lambda body: None
    p1 = Patch(fns=[fn1])
    p2 = Patch(p1, fns=[fn2])
    assert list(p2.fns) == [fn1, fn2]


def test_patch_no_fns_from_plain_dict():
    p = Patch({'a': 'b'})
    assert len(p.fns) == 0


def test_patch_inherits_dict_and_fns():
    fn1 = lambda body: None
    p1 = Patch({'x': 'y'}, fns=[fn1])
    p2 = Patch(p1)
    assert p2['x'] == 'y'
    assert list(p2.fns) == [fn1]
