import logging

import pytest

import kopf

# We assume that the handler filtering is tested in details elsewhere (for all handlers).
# Here, we only test if it is applied or not applied.


async def test_daemon_filtration_satisfied(
        settings, resource, dummy, caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)

    @kopf.daemon(*resource, id='fn',
                 labels={'a': 'value', 'b': kopf.PRESENT, 'c': kopf.ABSENT},
                 annotations={'x': 'value', 'y': kopf.PRESENT, 'z': kopf.ABSENT})
    async def fn(**kwargs):
        dummy.kwargs = kwargs
        dummy.steps['called'].set()

    finalizer = settings.persistence.finalizer
    event_body = {'metadata': {'labels': {'a': 'value', 'b': '...'},
                               'annotations': {'x': 'value', 'y': '...'},
                               'finalizers': [finalizer]}}
    await simulate_cycle(event_body)

    await dummy.steps['called'].wait()
    await dummy.wait_for_daemon_done()


@pytest.mark.parametrize('labels, annotations', [
    # Annotations mismatching (but labels are matching):
    ({'a': 'value', 'b': '...'}, {'x': 'mismatching-value', 'b': '...'}, ),  # x must be "value".
    ({'a': 'value', 'b': '...'}, {'x': 'value', 'y': '...', 'z': '...'}),  # z must be absent
    ({'a': 'value', 'b': '...'}, {'x': 'value'}),  # y must be present
    # labels mismatching (but annotations are matching):
    ({'a': 'mismatching-value', 'b': '...'}, {'x': 'value', 'y': '...'}),
    ({'a': 'value', 'b': '...', 'c': '...'}, {'x': 'value', 'y': '...'}),
    ({'a': 'value'}, {'x': 'value', 'y': '...'}),
])
async def test_daemon_filtration_mismatched(
        settings, resource, mocker, labels, annotations,
        caplog, assert_logs, k8s_mocked, simulate_cycle):
    caplog.set_level(logging.DEBUG)
    spawn_resource_daemons = mocker.patch('kopf.reactor.daemons.spawn_resource_daemons')

    @kopf.daemon(*resource, id='fn',
                 labels={'a': 'value', 'b': kopf.PRESENT, 'c': kopf.ABSENT},
                 annotations={'x': 'value', 'y': kopf.PRESENT, 'z': kopf.ABSENT})
    async def fn(**kwargs):
        pass

    finalizer = settings.persistence.finalizer
    event_body = {'metadata': {'labels': labels,
                               'annotations': annotations,
                               'finalizers': [finalizer]}}
    await simulate_cycle(event_body)

    assert spawn_resource_daemons.called
    assert spawn_resource_daemons.call_args_list[0][1]['handlers'] == []
