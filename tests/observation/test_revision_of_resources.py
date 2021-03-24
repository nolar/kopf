import pytest

import kopf
from kopf.reactor.observation import revise_resources
from kopf.structs.references import EVERYTHING, Insights, Resource

VERBS = ['list', 'watch', 'patch']


@pytest.fixture(params=[
    kopf.on.event, kopf.daemon, kopf.timer, kopf.index,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
    kopf.on.validate, kopf.on.mutate,
])
def handlers(request, registry):
    @request.param('group1', 'version1', 'plural1')
    @request.param('group2', 'version2', 'plural2')
    def fn(**_): ...


@pytest.mark.usefixtures('handlers')
def test_initial_population(registry):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)
    insights = Insights()
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    assert insights.resources == {r1}


@pytest.mark.usefixtures('handlers')
def test_replacing_all_insights(registry):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)
    r2 = Resource(group='group2', version='version2', plural='plural2', verbs=VERBS)
    insights = Insights()
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    revise_resources(registry=registry, insights=insights, group=None, resources=[r2])
    assert insights.resources == {r2}


@pytest.mark.usefixtures('handlers')
def test_replacing_an_existing_group(registry):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)
    r2 = Resource(group='group2', version='version2', plural='plural2', verbs=VERBS)
    insights = Insights()
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    revise_resources(registry=registry, insights=insights, group='group1', resources=[r2])
    assert insights.resources == {r2}


@pytest.mark.usefixtures('handlers')
def test_replacing_a_new_group(registry):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)
    r2 = Resource(group='group2', version='version2', plural='plural2', verbs=VERBS)
    insights = Insights()
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    revise_resources(registry=registry, insights=insights, group='group2', resources=[r2])
    assert insights.resources == {r1, r2}


@pytest.mark.parametrize('decorator', [
    kopf.on.event, kopf.daemon, kopf.timer, kopf.index,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
    kopf.on.validate, kopf.on.mutate,
])
def test_ambiguity_in_specific_selectors(registry, decorator, caplog, assert_logs):
    r1 = Resource(group='g1', version='v1', plural='plural', verbs=VERBS)
    r2 = Resource(group='g2', version='v2', plural='plural', verbs=VERBS)

    @decorator(plural='plural')
    def fn(**_): ...

    insights = Insights()
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1, r2])
    assert not insights.resources
    assert_logs([r"Ambiguous resources will not be served"])


@pytest.mark.parametrize('decorator', [
    kopf.on.event, kopf.daemon, kopf.timer, kopf.index,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
    kopf.on.validate, kopf.on.mutate,
])
def test_corev1_overrides_ambuigity(registry, decorator, caplog, assert_logs):
    r1 = Resource(group='', version='v1', plural='pods', verbs=VERBS)
    r2 = Resource(group='metrics.k8s.io', version='v1', plural='pods', verbs=VERBS)

    @decorator(plural='pods')
    def fn(**_): ...

    insights = Insights()
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1, r2])
    assert insights.resources == {r1}
    assert_logs([], prohibited=[r"Ambiguous resources will not be served"])


@pytest.mark.parametrize('decorator', [
    kopf.on.event, kopf.daemon, kopf.timer, kopf.index,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
    kopf.on.validate, kopf.on.mutate,
])
def test_no_ambiguity_in_generic_selector(registry, decorator, caplog, assert_logs):
    r1 = Resource(group='g1', version='v1', plural='plural', verbs=VERBS)
    r2 = Resource(group='g2', version='v2', plural='plural', verbs=VERBS)

    @decorator(EVERYTHING)
    def fn(**_): ...

    insights = Insights()
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1, r2])
    assert insights.resources == {r1, r2}
    assert_logs([], prohibited=[r"Ambiguous resources will not be served"])


@pytest.mark.parametrize('decorator', [
    kopf.on.event, kopf.daemon, kopf.timer, kopf.index,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
    kopf.on.validate, kopf.on.mutate,
])
def test_selectors_with_no_resources(registry, decorator, caplog, assert_logs):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)
    r2 = Resource(group='group2', version='version2', plural='plural2', verbs=VERBS)

    @decorator(plural='plural3')
    def fn(**_): ...

    insights = Insights()
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1, r2])
    assert not insights.resources
    assert_logs([r"Unresolved resources cannot be served"])


@pytest.mark.parametrize('decorator', [
    kopf.daemon, kopf.timer,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
])
def test_nonwatchable_excluded(registry, decorator, caplog, assert_logs):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=[])

    @decorator('group1', 'version1', 'plural1')
    def fn(**_): ...

    insights = Insights()
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    assert not insights.resources
    assert_logs([r"Non-watchable resources will not be served: {plural1.version1.group1}"])


@pytest.mark.parametrize('decorator', [
    kopf.daemon, kopf.timer,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
])
def test_nonpatchable_excluded(registry, decorator, caplog, assert_logs):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=['watch', 'list'])

    @decorator('group1', 'version1', 'plural1')  # because it patches!
    def fn(**_): ...

    insights = Insights()
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    assert not insights.resources
    assert_logs([r"Non-patchable resources will not be served: {plural1.version1.group1}"])
