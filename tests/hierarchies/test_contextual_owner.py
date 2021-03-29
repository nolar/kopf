import copy
import logging

import pytest

import kopf
from kopf.reactor.causation import ResourceChangingCause, ResourceWatchingCause
from kopf.reactor.handling import cause_var
from kopf.reactor.indexing import OperatorIndexers
from kopf.reactor.invocation import context
from kopf.structs.bodies import Body, RawBody, RawEvent, RawMeta
from kopf.structs.ephemera import Memo
from kopf.structs.handlers import Reason
from kopf.structs.patches import Patch

OWNER_API_VERSION = 'owner-api-version'
OWNER_NAMESPACE = 'owner-namespace'
OWNER_KIND = 'OwnerKind'
OWNER_NAME = 'owner-name'
OWNER_UID = 'owner-uid'
OWNER_LABELS = {'label-1': 'value-1', 'label-2': 'value-2'}
OWNER = RawBody(
    apiVersion=OWNER_API_VERSION,
    kind=OWNER_KIND,
    metadata=RawMeta(
        namespace=OWNER_NAMESPACE,
        name=OWNER_NAME,
        uid=OWNER_UID,
        labels=OWNER_LABELS,
    ),
)


@pytest.fixture(params=['state-changing-cause', 'event-watching-cause'])
def owner(request, resource):
    body = Body(copy.deepcopy(OWNER))
    if request.param == 'state-changing-cause':
        cause = ResourceChangingCause(
            logger=logging.getLogger('kopf.test.fake.logger'),
            indices=OperatorIndexers().indices,
            resource=resource,
            patch=Patch(),
            memo=Memo(),
            body=body,
            initial=False,
            reason=Reason.NOOP,
        )
        with context([(cause_var, cause)]):
            yield body
    elif request.param == 'event-watching-cause':
        cause = ResourceWatchingCause(
            logger=logging.getLogger('kopf.test.fake.logger'),
            indices=OperatorIndexers().indices,
            resource=resource,
            patch=Patch(),
            memo=Memo(),
            body=body,
            type='irrelevant',
            raw=RawEvent(type='irrelevant', object=OWNER),
        )
        with context([(cause_var, cause)]):
            yield body
    else:
        raise RuntimeError(f"Wrong param for `owner` fixture: {request.param!r}")


def test_when_unset_for_owner_references_appending():
    with pytest.raises(LookupError) as e:
        kopf.append_owner_reference([])
    assert 'Owner must be set explicitly' in str(e.value)


def test_when_unset_for_owner_references_removal():
    with pytest.raises(LookupError) as e:
        kopf.remove_owner_reference([])
    assert 'Owner must be set explicitly' in str(e.value)


def test_when_unset_for_name_harmonization():
    with pytest.raises(LookupError) as e:
        kopf.harmonize_naming([])
    assert 'Owner must be set explicitly' in str(e.value)


def test_when_unset_for_namespace_adjustment():
    with pytest.raises(LookupError) as e:
        kopf.adjust_namespace([])
    assert 'Owner must be set explicitly' in str(e.value)


def test_when_unset_for_labelling():
    with pytest.raises(LookupError) as e:
        kopf.label([])
    assert 'Owner must be set explicitly' in str(e.value)


def test_when_unset_for_adopting():
    with pytest.raises(LookupError) as e:
        kopf.adopt([])
    assert 'Owner must be set explicitly' in str(e.value)


def test_when_empty_for_name_harmonization(owner):
    owner._replace_with({})
    with pytest.raises(LookupError) as e:
        kopf.harmonize_naming([])
    assert 'Name must be set explicitly' in str(e.value)


def test_when_empty_for_namespace_adjustment(owner):
    # An absent namespace means a cluster-scoped resource -- a valid case.
    obj = {}
    owner._replace_with({})
    kopf.adjust_namespace(obj)
    assert obj['metadata']['namespace'] is None


def test_when_empty_for_adopting(owner):
    owner._replace_with({})
    with pytest.raises(LookupError):
        kopf.adopt([])
    # any error message: the order of functions is not specific.


def test_when_set_for_name_harmonization(owner):
    obj = {}
    kopf.harmonize_naming(obj)
    assert obj['metadata']['generateName'].startswith(OWNER_NAME)


def test_when_set_for_namespace_adjustment(owner):
    obj = {}
    kopf.adjust_namespace(obj)
    assert obj['metadata']['namespace'] == OWNER_NAMESPACE


def test_when_set_for_owner_references_appending(owner):
    obj = {}
    kopf.append_owner_reference(obj)
    assert obj['metadata']['ownerReferences']
    assert obj['metadata']['ownerReferences'][0]['uid'] == OWNER_UID


def test_when_set_for_owner_references_removal(owner):
    obj = {}
    kopf.append_owner_reference(obj)  # assumed to work, tested above
    kopf.remove_owner_reference(obj)  # this one is being tested here
    assert not obj['metadata']['ownerReferences']


def test_when_set_for_labelling(owner):
    obj = {}
    kopf.label(obj)
    assert obj['metadata']['labels'] == {'label-1': 'value-1', 'label-2': 'value-2'}
