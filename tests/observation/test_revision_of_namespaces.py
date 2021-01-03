from kopf.reactor.observation import revise_namespaces
from kopf.structs.bodies import RawBody, RawEvent
from kopf.structs.references import Insights


def test_bodies_for_initial_population(registry):
    b1 = RawBody(metadata={'name': 'ns1'})
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b1])
    assert insights.namespaces == {'ns1'}


def test_bodies_for_additional_population(registry):
    b1 = RawBody(metadata={'name': 'ns1'})
    b2 = RawBody(metadata={'name': 'ns2'})
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b2])
    assert insights.namespaces == {'ns1', 'ns2'}


def test_bodies_for_deletion_via_timestamp(registry):
    b1 = RawBody(metadata={'name': 'ns1'})
    b2 = RawBody(metadata={'name': 'ns1', 'deletionTimestamp': '...'})
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b2])
    assert not insights.namespaces


def test_bodies_ignored_for_mismatching(registry):
    b1 = RawBody(metadata={'name': 'def1'})
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b1])
    assert not insights.namespaces


def test_events_for_initial_population(registry):
    e1 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1'}))
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e1])
    assert insights.namespaces == {'ns1'}


def test_events_for_additional_population(registry):
    e1 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1'}))
    e2 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns2'}))
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e2])
    assert insights.namespaces == {'ns1', 'ns2'}


def test_events_for_deletion_via_timestamp(registry):
    e1 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1'}))
    e2 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1', 'deletionTimestamp': '...'}))
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e2])
    assert not insights.namespaces


def test_events_for_deletion_via_event_type(registry):
    e1 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1'}))
    e2 = RawEvent(type='DELETED', object=RawBody(metadata={'name': 'ns1'}))
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e2])
    assert not insights.namespaces


def test_events_ignored_for_mismatching(registry):
    e1 = RawEvent(type=None, object=RawBody(metadata={'name': 'def1'}))
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e1])
    assert not insights.namespaces
