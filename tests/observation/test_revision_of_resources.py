import pytest

import kopf
from kopf._cogs.structs.references import EVERYTHING, Insights, Resource
from kopf._core.reactor.observation import revise_resources

VERBS = ['list', 'watch', 'patch']


@pytest.fixture()
async def insights():
    return Insights()


@pytest.fixture(params=[
    (kopf.on.event, 'watched_resources'),
    (kopf.daemon, 'watched_resources'),
    (kopf.timer, 'watched_resources'),
    (kopf.index, 'watched_resources'),
    (kopf.index, 'indexed_resources'),
    (kopf.on.resume, 'watched_resources'),
    (kopf.on.create, 'watched_resources'),
    (kopf.on.update, 'watched_resources'),
    (kopf.on.delete, 'watched_resources'),
    (kopf.on.validate, 'webhook_resources'),
    (kopf.on.mutate, 'webhook_resources'),
])
def insights_resources(request, registry, insights):
    decorator, insights_field = request.param

    @decorator('group1', 'version1', 'plural1')
    @decorator('group2', 'version2', 'plural2')
    def fn(**_): pass

    return getattr(insights, insights_field)


def test_initial_population(registry, insights, insights_resources):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    assert insights_resources == {r1}


def test_replacing_all_insights(registry, insights, insights_resources):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)
    r2 = Resource(group='group2', version='version2', plural='plural2', verbs=VERBS)
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    revise_resources(registry=registry, insights=insights, group=None, resources=[r2])
    assert insights_resources == {r2}


def test_replacing_an_existing_group(registry, insights, insights_resources):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)
    r2 = Resource(group='group2', version='version2', plural='plural2', verbs=VERBS)
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    revise_resources(registry=registry, insights=insights, group='group1', resources=[r2])
    assert insights_resources == {r2}


def test_replacing_a_new_group(registry, insights, insights_resources):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)
    r2 = Resource(group='group2', version='version2', plural='plural2', verbs=VERBS)
    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    revise_resources(registry=registry, insights=insights, group='group2', resources=[r2])
    assert insights_resources == {r1, r2}


@pytest.mark.parametrize('decorator', [
    kopf.on.event, kopf.daemon, kopf.timer, kopf.index,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
])
def test_ambiguity_in_specific_selectors(registry, decorator, caplog, assert_logs, insights):
    r1 = Resource(group='g1', version='v1', plural='plural', verbs=VERBS)
    r2 = Resource(group='g2', version='v2', plural='plural', verbs=VERBS)

    @decorator(plural='plural')
    def fn(**_): pass

    revise_resources(registry=registry, insights=insights, group=None, resources=[r1, r2])
    assert not insights.watched_resources
    assert not insights.webhook_resources
    assert_logs([r"Ambiguous resources will not be served"])


@pytest.mark.parametrize('decorator', [
    kopf.on.event, kopf.daemon, kopf.timer, kopf.index,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
])
def test_corev1_overrides_ambuigity(registry, decorator, caplog, assert_logs, insights):
    r1 = Resource(group='', version='v1', plural='pods', verbs=VERBS)
    r2 = Resource(group='metrics.k8s.io', version='v1', plural='pods', verbs=VERBS)

    @decorator(plural='pods')
    def fn(**_): pass

    revise_resources(registry=registry, insights=insights, group=None, resources=[r1, r2])
    assert insights.watched_resources == {r1}
    assert_logs(prohibited=[r"Ambiguous resources will not be served"])


@pytest.mark.parametrize('decorator', [
    kopf.on.event, kopf.daemon, kopf.timer, kopf.index,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
])
def test_no_ambiguity_in_generic_selector(registry, decorator, caplog, assert_logs, insights):
    r1 = Resource(group='g1', version='v1', plural='plural', verbs=VERBS)
    r2 = Resource(group='g2', version='v2', plural='plural', verbs=VERBS)

    @decorator(EVERYTHING)
    def fn(**_): pass

    revise_resources(registry=registry, insights=insights, group=None, resources=[r1, r2])
    assert insights.watched_resources == {r1, r2}
    assert_logs(prohibited=[r"Ambiguous resources will not be served"])


@pytest.mark.parametrize('decorator', [
    kopf.on.event, kopf.daemon, kopf.timer, kopf.index,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
])
def test_selectors_with_no_resources(registry, decorator, caplog, assert_logs, insights):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)
    r2 = Resource(group='group2', version='version2', plural='plural2', verbs=VERBS)

    @decorator(plural='plural3')
    def fn(**_): pass

    revise_resources(registry=registry, insights=insights, group=None, resources=[r1, r2])
    assert not insights.watched_resources
    assert_logs([r"Unresolved resources cannot be served"])


@pytest.mark.parametrize('decorator', [
    kopf.daemon, kopf.timer,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
])
def test_nonwatchable_excluded(registry, decorator, caplog, assert_logs, insights):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=[])

    @decorator('group1', 'version1', 'plural1')
    def fn(**_): pass

    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    assert not insights.watched_resources
    assert_logs([r"Non-watchable resources will not be served: {plural1.version1.group1}"])


@pytest.mark.parametrize('decorator', [
    kopf.daemon, kopf.timer,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
])
def test_nonpatchable_excluded(registry, decorator, caplog, assert_logs, insights):
    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=['watch', 'list'])

    @decorator('group1', 'version1', 'plural1')  # because it patches!
    def fn(**_): pass

    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    assert not insights.watched_resources
    assert_logs([r"Non-patchable resources will not be served: {plural1.version1.group1}"])


@pytest.mark.parametrize('decorator', [
    kopf.daemon, kopf.timer,
    kopf.on.resume, kopf.on.create, kopf.on.update, kopf.on.delete,
])
def test_watchedonly_resources_are_excluded_from_other_sets(registry, decorator, insights):

    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)

    @decorator('group1', 'version1', 'plural1')
    def fn(**_): pass

    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    assert insights.watched_resources
    assert not insights.indexed_resources
    assert not insights.webhook_resources


@pytest.mark.parametrize('decorator', [
    kopf.on.mutate, kopf.on.validate,
])
def test_webhookonly_resources_are_excluded_from_other_sets(registry, decorator, insights):

    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)

    @decorator('group1', 'version1', 'plural1')
    def fn(**_): pass

    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    assert not insights.watched_resources
    assert not insights.indexed_resources
    assert insights.webhook_resources


@pytest.mark.parametrize('decorator', [
    kopf.index,
])
def test_indexed_resources_are_duplicated_in_watched_resources(registry, decorator, insights):

    r1 = Resource(group='group1', version='version1', plural='plural1', verbs=VERBS)

    @decorator('group1', 'version1', 'plural1')
    def fn(**_): pass

    revise_resources(registry=registry, insights=insights, group=None, resources=[r1])
    assert insights.watched_resources
    assert insights.indexed_resources
    assert not insights.webhook_resources
