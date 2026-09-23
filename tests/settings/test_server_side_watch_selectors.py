import kopf
from kopf._cogs.structs.references import Resource


def test_server_side_selectors_resolve_by_concrete_resource_identity() -> None:
    settings = kopf.OperatorSettings()
    pods_selector = kopf.WatchListSelector(
        label_selector='prefect.io/flow-run-id',
        field_selector='status.phase!=Succeeded,status.phase!=Failed',
    )
    jobs_selector = kopf.WatchListSelector(
        label_selector='prefect.io/flow-run-id',
    )

    settings.watching.server_side_selectors[('', 'v1', 'pods')] = pods_selector
    settings.watching.server_side_selectors[('batch', 'v1', 'jobs')] = jobs_selector

    assert settings.watching.resolve_server_side_selector(
        Resource('', 'v1', 'pods', namespaced=True)) is pods_selector
    assert settings.watching.resolve_server_side_selector(
        Resource('batch', 'v1', 'jobs', namespaced=True)) is jobs_selector


def test_server_side_selectors_ignore_unknown_resources() -> None:
    settings = kopf.OperatorSettings()
    settings.watching.server_side_selectors[('', 'v1', 'pods')] = kopf.WatchListSelector(
        label_selector='prefect.io/flow-run-id',
    )

    assert settings.watching.resolve_server_side_selector(
        Resource('', 'v1', 'services', namespaced=True)) is None
    assert settings.watching.resolve_server_side_selector(
        Resource('batch', 'v1', 'jobs', namespaced=True)) is None
