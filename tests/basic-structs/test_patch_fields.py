from kopf._cogs.structs.patches import Patch

# === spec ===


def test_spec_set_creates_key():
    patch = Patch()
    patch.spec['replicas'] = 3
    assert patch == {'spec': {'replicas': 3}}


def test_spec_get_existing_key():
    patch = Patch({'spec': {'replicas': 3}})
    assert patch.spec['replicas'] == 3


def test_spec_get_missing_key_with_default():
    patch = Patch()
    assert patch.spec.get('replicas', 42) == 42


def test_spec_is_empty_initially():
    patch = Patch()
    assert dict(patch.spec) == {}
    assert patch == {}


def test_spec_set_multiple_keys():
    patch = Patch()
    patch.spec['replicas'] = 3
    patch.spec['selector'] = {'app': 'test'}
    assert patch == {'spec': {'replicas': 3, 'selector': {'app': 'test'}}}


# === status ===


def test_status_set_creates_key():
    patch = Patch()
    patch.status['ready'] = True
    assert patch == {'status': {'ready': True}}


def test_status_get_existing_key():
    patch = Patch({'status': {'ready': True}})
    assert patch.status['ready'] is True


def test_status_get_missing_key_with_default():
    patch = Patch()
    assert patch.status.get('ready', False) is False


def test_status_is_empty_initially():
    patch = Patch()
    assert dict(patch.status) == {}
    assert patch == {}


# === metadata ===


def test_metadata_set_creates_key():
    patch = Patch()
    patch.metadata['name'] = 'obj'
    assert patch == {'metadata': {'name': 'obj'}}


def test_metadata_get_existing_key():
    patch = Patch({'metadata': {'name': 'obj'}})
    assert patch.metadata['name'] == 'obj'


def test_metadata_get_missing_key_with_default():
    patch = Patch()
    assert patch.metadata.get('name', 'default') == 'default'


def test_metadata_is_empty_initially():
    patch = Patch()
    assert dict(patch.metadata) == {}
    assert patch == {}


# === meta (alias for metadata) ===


def test_meta_is_same_as_metadata():
    patch = Patch()
    assert patch.meta is patch.metadata


def test_meta_set_reflects_in_patch():
    patch = Patch()
    patch.meta['name'] = 'obj'
    assert patch == {'metadata': {'name': 'obj'}}


# === labels ===


def test_labels_set_creates_key():
    patch = Patch()
    patch.meta.labels['app'] = 'foo'
    assert patch == {'metadata': {'labels': {'app': 'foo'}}}


def test_labels_get_existing_key():
    patch = Patch({'metadata': {'labels': {'app': 'foo'}}})
    assert patch.meta.labels['app'] == 'foo'


def test_labels_get_missing_key_with_default():
    patch = Patch()
    assert patch.meta.labels.get('app', 'default') == 'default'


def test_labels_is_empty_initially():
    patch = Patch()
    assert dict(patch.meta.labels) == {}
    assert patch == {}


def test_labels_set_multiple():
    patch = Patch()
    patch.meta.labels['app'] = 'foo'
    patch.meta.labels['env'] = 'prod'
    assert patch == {'metadata': {'labels': {'app': 'foo', 'env': 'prod'}}}


def test_labels_delete_via_none():
    patch = Patch()
    patch.meta.labels['app'] = None
    assert patch == {'metadata': {'labels': {'app': None}}}


# === annotations ===


def test_annotations_set_creates_key():
    patch = Patch()
    patch.meta.annotations['key'] = 'value'
    assert patch == {'metadata': {'annotations': {'key': 'value'}}}


def test_annotations_get_existing_key():
    patch = Patch({'metadata': {'annotations': {'key': 'value'}}})
    assert patch.meta.annotations['key'] == 'value'


def test_annotations_get_missing_key_with_default():
    patch = Patch()
    assert patch.meta.annotations.get('key', 'default') == 'default'


def test_annotations_is_empty_initially():
    patch = Patch()
    assert dict(patch.meta.annotations) == {}
    assert patch == {}


def test_annotations_set_multiple():
    patch = Patch()
    patch.meta.annotations['a'] = '1'
    patch.meta.annotations['b'] = '2'
    assert patch == {'metadata': {'annotations': {'a': '1', 'b': '2'}}}


# === cross-view consistency ===


def test_spec_and_status_are_independent():
    patch = Patch()
    patch.spec['x'] = 1
    patch.status['y'] = 2
    assert patch == {'spec': {'x': 1}, 'status': {'y': 2}}


def test_labels_and_annotations_coexist():
    patch = Patch()
    patch.meta.labels['app'] = 'foo'
    patch.meta.annotations['note'] = 'bar'
    assert patch == {'metadata': {'labels': {'app': 'foo'}, 'annotations': {'note': 'bar'}}}


def test_all_views_coexist():
    patch = Patch()
    patch.meta['name'] = 'obj'
    patch.meta.labels['app'] = 'foo'
    patch.spec['replicas'] = 3
    patch.status['ready'] = True
    assert patch == {
        'metadata': {'name': 'obj', 'labels': {'app': 'foo'}},
        'spec': {'replicas': 3},
        'status': {'ready': True},
    }
