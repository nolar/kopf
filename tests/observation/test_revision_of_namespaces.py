from kopf._cogs.structs.bodies import RawBody, RawEvent
from kopf._cogs.structs.references import Insights
from kopf._core.reactor.observation import revise_namespaces


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


def test_bodies_for_deletion_via_timestamp_without_conditions(registry):
    b1 = RawBody(metadata={'name': 'ns1'})
    b2 = RawBody(metadata={'name': 'ns1', 'deletionTimestamp': '...'})
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b2])
    assert insights.namespaces == {'ns1'}


def test_bodies_for_deletion_via_timestamp_with_true_conditions(registry, assert_logs):
    b1 = RawBody(metadata={'name': 'ns1'})
    b2 = RawBody(metadata={'name': 'ns1', 'deletionTimestamp': '...'},
                 status={'conditions': [{'type': 'Whatever',
                                         'status': 'True',
                                         'reason': 'SomeReason',
                                         'message': 'Some message'}]})
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b2])
    assert insights.namespaces == {'ns1'}
    assert_logs(["Namespace 'ns1' termination pending: SomeReason: Some message"])


def test_bodies_for_deletion_via_timestamp_with_false_conditions(registry, assert_logs):
    b1 = RawBody(metadata={'name': 'ns1'})
    b2 = RawBody(metadata={'name': 'ns1', 'deletionTimestamp': '...'},
                 status={'conditions': [{'type': 'Whatever',
                                         'status': 'False'}]})
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_bodies=[b2])
    assert not insights.namespaces
    assert_logs(prohibited=["Namespace '.*' termination pending:"])


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


def test_events_for_deletion_via_timestamp_without_conditions(registry):
    e1 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1'}))
    e2 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1', 'deletionTimestamp': '...'}))
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e2])
    assert insights.namespaces == {'ns1'}


def test_events_for_deletion_via_timestamp_with_true_conditions(registry, assert_logs):
    e1 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1'}))
    e2 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1', 'deletionTimestamp': '...'},
                                            status={'conditions': [{'type': 'Whatever',
                                                                    'status': 'True',
                                                                    'reason': 'SomeReason',
                                                                    'message': 'Some message'}]}))
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e2])
    assert insights.namespaces == {'ns1'}
    assert_logs(["Namespace 'ns1' termination pending: SomeReason: Some message"])


def test_events_for_deletion_via_timestamp_with_false_conditions(registry, assert_logs):
    e1 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1'}))
    e2 = RawEvent(type=None, object=RawBody(metadata={'name': 'ns1', 'deletionTimestamp': '...'},
                                            status={'conditions': [{'type': 'Whatever',
                                                                    'status': 'False'}]}))
    insights = Insights()
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e1])
    revise_namespaces(insights=insights, namespaces=['ns*'], raw_events=[e2])
    assert not insights.namespaces
    assert_logs(prohibited=["Namespace '.*' termination pending:"])


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
