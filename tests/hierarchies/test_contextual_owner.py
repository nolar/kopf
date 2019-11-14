import logging

import pytest

import kopf
from kopf.reactor.causation import Reason, ResourceChangingCause, ResourceWatchingCause
from kopf.reactor.handling import cause_var
from kopf.reactor.invocation import context
from kopf.structs.bodies import Body, Meta, Labels, Event
from kopf.structs.containers import ObjectDict
from kopf.structs.patches import Patch

OWNER_API_VERSION = 'owner-api-version'
OWNER_NAMESPACE = 'owner-namespace'
OWNER_KIND = 'OwnerKind'
OWNER_NAME = 'owner-name'
OWNER_UID = 'owner-uid'
OWNER_LABELS: Labels = {'label-1': 'value-1', 'label-2': 'value-2'}
OWNER = Body(
    apiVersion=OWNER_API_VERSION,
    kind=OWNER_KIND,
    metadata=Meta(
        namespace=OWNER_NAMESPACE,
        name=OWNER_NAME,
        uid=OWNER_UID,
        labels=OWNER_LABELS,
    ),
)


@pytest.fixture(params=['state-changing-cause', 'event-watching-cause'])
def owner(request, resource):
    if request.param == 'state-changing-cause':
        cause = ResourceChangingCause(
            logger=logging.getLogger('kopf.test.fake.logger'),
            resource=resource,
            patch=Patch(),
            memo=ObjectDict(),
            body=OWNER,
            initial=False,
            reason=Reason.NOOP,
        )
        with context([(cause_var, cause)]):
            yield
    elif request.param == 'event-watching-cause':
        cause = ResourceWatchingCause(
            logger=logging.getLogger('kopf.test.fake.logger'),
            resource=resource,
            patch=Patch(),
            memo=ObjectDict(),
            body=OWNER,
            type='irrelevant',
            raw=Event(type='irrelevant', object=OWNER),
        )
        with context([(cause_var, cause)]):
            yield
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


def test_when_unset_for_adopting():
    with pytest.raises(LookupError) as e:
        kopf.adopt([])
    assert 'Owner must be set explicitly' in str(e.value)


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
