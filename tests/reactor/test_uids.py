from kopf.reactor.queueing import get_uid


def test_uid_is_used_if_present():
    raw_event = {'type': ..., 'object': {'metadata': {'uid': '123'}}}
    uid = get_uid(raw_event)

    assert isinstance(uid, str)
    assert uid == '123'


def test_uid_is_simulated_if_absent():
    raw_event = {'type': ...,
                 'object': {
                     'apiVersion': 'group/v1',
                     'kind': 'Kind1',
                     'metadata': {
                         'name': 'name1',
                         'namespace': 'namespace1',
                         'creationTimestamp': 'created1',
                     }}}
    uid = get_uid(raw_event)

    # The exact order is irrelevant.
    assert isinstance(uid, str)
    assert 'created1' in uid
    assert 'name1' in uid
    assert 'namespace' in uid
    assert 'Kind1' in uid
    assert 'group/v1' in uid
