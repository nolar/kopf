import asyncio

import pytest

import kopf
from kopf._core.reactor.running import spawn_tasks


@pytest.fixture(autouse=True)
def _safe_tasks(registry, settings):
    """Block startup to keep guarded tasks idle; disable SIGKILL scheduling."""
    settings.process.ultimate_exiting_timeout = None

    @kopf.on.startup(registry=registry)
    async def block_startup(**_):
        await asyncio.sleep(10)


async def _cancel_all(tasks):
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


# --- Argument validation ---


async def test_namespace_and_namespaces_cannot_coexist(registry, settings):
    with pytest.raises(TypeError, match="Either namespaces= or namespace= can be passed"):
        await spawn_tasks(
            registry=registry,
            settings=settings,
            namespaces=['ns1'],
            namespace='ns2',
        )


async def test_clusterwide_and_namespaces_cannot_coexist(registry, settings):
    with pytest.raises(TypeError, match="cluster-wide or namespaced, not both"):
        await spawn_tasks(
            registry=registry,
            settings=settings,
            clusterwide=True,
            namespaces=['ns1'],
        )


async def test_deprecated_namespace_kwarg(registry, settings):
    with pytest.warns(DeprecationWarning, match="namespace= is deprecated"):
        tasks = await spawn_tasks(
            registry=registry,
            settings=settings,
            namespace='ns1',
        )
    await _cancel_all(tasks)


async def test_missing_scope_defaults_to_clusterwide(registry, settings):
    with pytest.warns(FutureWarning, match="Absence of either namespaces or cluster-wide"):
        tasks = await spawn_tasks(
            registry=registry,
            settings=settings,
        )
    assert settings.peering.clusterwide is True
    await _cancel_all(tasks)


# --- Settings mapping ---


async def test_clusterwide_mapped_to_settings(registry, settings):
    assert settings.peering.clusterwide is False
    tasks = await spawn_tasks(
        registry=registry,
        settings=settings,
        clusterwide=True,
    )
    assert settings.peering.clusterwide is True
    await _cancel_all(tasks)


async def test_peering_name_mapped_to_settings(registry, settings):
    assert settings.peering.mandatory is False
    tasks = await spawn_tasks(
        registry=registry,
        settings=settings,
        clusterwide=True,
        peering_name='my-peering',
    )
    assert settings.peering.mandatory is True
    assert settings.peering.name == 'my-peering'
    await _cancel_all(tasks)


async def test_standalone_mapped_to_settings(registry, settings):
    assert settings.peering.standalone is False
    tasks = await spawn_tasks(
        registry=registry,
        settings=settings,
        clusterwide=True,
        standalone=True,
    )
    assert settings.peering.standalone is True
    await _cancel_all(tasks)


async def test_priority_mapped_to_settings(registry, settings):
    assert settings.peering.priority == 0
    tasks = await spawn_tasks(
        registry=registry,
        settings=settings,
        clusterwide=True,
        priority=100,
    )
    assert settings.peering.priority == 100
    await _cancel_all(tasks)


# --- Task composition ---


async def test_always_present_tasks(registry, settings):
    tasks = await spawn_tasks(
        registry=registry,
        settings=settings,
        clusterwide=True,
    )
    task_names = {t.get_name() for t in tasks}

    assert "stop-flag checker" in task_names
    assert "ultimate termination" in task_names
    assert "startup/cleanup activities" in task_names
    assert "daemon killer" in task_names
    assert "credentials retriever" in task_names
    assert "poster of events" in task_names
    assert "admission insights chain" in task_names
    assert "admission validating configuration manager" in task_names
    assert "admission mutating configuration manager" in task_names
    assert "admission webhook server" in task_names
    assert "resource observer" in task_names
    assert "namespace observer" in task_names
    assert "multidimensional multitasker" in task_names
    assert "health reporter" not in task_names

    await _cancel_all(tasks)


async def test_liveness_endpoint_adds_health_reporter(registry, settings):
    tasks = await spawn_tasks(
        registry=registry,
        settings=settings,
        clusterwide=True,
        liveness_endpoint="/health",
    )
    task_names = {t.get_name() for t in tasks}
    assert "health reporter" in task_names
    await _cancel_all(tasks)


async def test_command_replaces_orchestrator(registry, settings):
    async def my_command():
        await asyncio.Event().wait()

    tasks = await spawn_tasks(
        registry=registry,
        settings=settings,
        clusterwide=True,
        _command=my_command(),
    )
    task_names = {t.get_name() for t in tasks}
    assert "the command" in task_names
    assert "multidimensional multitasker" not in task_names
    await _cancel_all(tasks)
