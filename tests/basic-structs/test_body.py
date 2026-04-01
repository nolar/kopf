import pytest

from kopf._cogs.structs.bodies import Body

# === Body views ===


def test_body_metadata():
    body = Body({'metadata': {'name': 'obj', 'uid': '123'}})
    assert dict(body.metadata) == {'name': 'obj', 'uid': '123'}


def test_body_meta_is_metadata():
    body = Body({'metadata': {'name': 'obj'}})
    assert body.meta is body.metadata


def test_body_spec():
    body = Body({'spec': {'field': 'value'}})
    assert dict(body.spec) == {'field': 'value'}


def test_body_status():
    body = Body({'status': {'phase': 'Running'}})
    assert dict(body.status) == {'phase': 'Running'}


def test_body_metadata_when_missing():
    body = Body({})
    assert dict(body.metadata) == {}
    assert body == {}  # unmodified (no setdefault() used)


def test_body_spec_when_missing():
    body = Body({})
    assert dict(body.spec) == {}
    assert body == {}  # unmodified (no setdefault() used)


def test_body_status_when_missing():
    body = Body({})
    assert dict(body.status) == {}
    assert body == {}  # unmodified (no setdefault() used)


# === Meta properties ===


def test_meta_name():
    body = Body({'metadata': {'name': 'obj'}})
    assert body.metadata.name == 'obj'


def test_meta_name_when_missing():
    body = Body({'metadata': {}})
    assert body.metadata.name is None
    assert body == {'metadata': {}}  # unmodified (no setdefault() used)


def test_meta_name_when_metadata_missing():
    body = Body({})
    assert body.metadata.name is None
    assert body == {}  # unmodified (no setdefault() used)


def test_meta_uid():
    body = Body({'metadata': {'uid': 'uid-123'}})
    assert body.metadata.uid == 'uid-123'


def test_meta_uid_when_missing():
    body = Body({'metadata': {}})
    assert body.metadata.uid is None
    assert body == {'metadata': {}}  # unmodified (no setdefault() used)


def test_meta_uid_when_metadata_missing():
    body = Body({})
    assert body.metadata.uid is None
    assert body == {}  # unmodified (no setdefault() used)


def test_meta_namespace():
    body = Body({'metadata': {'namespace': 'ns'}})
    assert body.metadata.namespace == 'ns'


def test_meta_namespace_when_missing():
    body = Body({'metadata': {}})
    assert body.metadata.namespace is None
    assert body == {'metadata': {}}  # unmodified (no setdefault() used)


def test_meta_namespace_when_metadata_missing():
    body = Body({})
    assert body.metadata.namespace is None
    assert body == {}  # unmodified (no setdefault() used)


def test_meta_creation_timestamp():
    body = Body({'metadata': {'creationTimestamp': '2020-01-01T00:00:00Z'}})
    assert body.metadata.creation_timestamp == '2020-01-01T00:00:00Z'


def test_meta_creation_timestamp_when_missing():
    body = Body({'metadata': {}})
    assert body.metadata.creation_timestamp is None
    assert body == {'metadata': {}}  # unmodified (no setdefault() used)


def test_meta_deletion_timestamp():
    body = Body({'metadata': {'deletionTimestamp': '2020-01-01T00:00:00Z'}})
    assert body.metadata.deletion_timestamp == '2020-01-01T00:00:00Z'


def test_meta_deletion_timestamp_when_missing():
    body = Body({'metadata': {}})
    assert body.metadata.deletion_timestamp is None
    assert body == {'metadata': {}}  # unmodified (no setdefault() used)


# === Labels and annotations ===


def test_meta_labels():
    body = Body({'metadata': {'labels': {'app': 'test', 'env': 'dev'}}})
    assert dict(body.metadata.labels) == {'app': 'test', 'env': 'dev'}


def test_meta_labels_when_missing():
    body = Body({'metadata': {}})
    assert dict(body.metadata.labels) == {}
    assert body == {'metadata': {}}  # unmodified (no setdefault() used)


def test_meta_annotations():
    body = Body({'metadata': {'annotations': {'key': 'val'}}})
    assert dict(body.metadata.annotations) == {'key': 'val'}


def test_meta_annotations_when_missing():
    body = Body({'metadata': {}})
    assert dict(body.metadata.annotations) == {}
    assert body == {'metadata': {}}  # unmodified (no setdefault() used)


# === Live views ===


def test_meta_reflects_body_changes():
    raw = {'metadata': {'name': 'obj1', 'uid': 'uid-1'}}
    body = Body(raw)
    assert body.metadata.name == 'obj1'
    raw['metadata']['name'] = 'obj2'
    assert body.metadata.name == 'obj2'


def test_spec_reflects_body_changes():
    raw = {'spec': {'field': 'old'}}
    body = Body(raw)
    assert body.spec['field'] == 'old'
    raw['spec']['field'] = 'new'
    assert body.spec['field'] == 'new'


def test_status_reflects_body_changes():
    raw = {'status': {'phase': 'Pending'}}
    body = Body(raw)
    assert body.status['phase'] == 'Pending'
    raw['status']['phase'] = 'Running'
    assert body.status['phase'] == 'Running'


def test_labels_reflect_body_changes():
    raw = {'metadata': {'labels': {'app': 'v1'}}}
    body = Body(raw)
    assert dict(body.metadata.labels) == {'app': 'v1'}
    raw['metadata']['labels']['app'] = 'v2'
    assert dict(body.metadata.labels) == {'app': 'v2'}


def test_annotations_reflect_body_changes():
    raw = {'metadata': {'annotations': {'key': 'old'}}}
    body = Body(raw)
    assert dict(body.metadata.annotations) == {'key': 'old'}
    raw['metadata']['annotations']['key'] = 'new'
    assert dict(body.metadata.annotations) == {'key': 'new'}
