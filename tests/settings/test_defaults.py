import logging

import pytest

import kopf


async def test_declared_public_interface_and_promised_defaults():
    settings = kopf.OperatorSettings()
    assert settings.posting.level == logging.INFO
    assert settings.peering.name == "default"
    assert settings.peering.stealth == False
    assert settings.peering.priority == 0
    assert settings.peering.lifetime == 60
    assert settings.peering.mandatory == False
    assert settings.peering.standalone == False
    assert settings.peering.namespaced == True
    assert settings.peering.clusterwide == False
    assert settings.watching.reconnect_backoff == 0.1
    assert settings.watching.connect_timeout is None
    assert settings.watching.server_timeout is None
    assert settings.watching.client_timeout is None
    assert settings.queueing.worker_limit is None
    assert settings.queueing.idle_timeout == 5.0
    assert settings.queueing.exit_timeout == 2.0
    assert settings.queueing.error_delays == (1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610)
    assert settings.scanning.disabled == False
    assert settings.admission.server is None
    assert settings.admission.managed is None
    assert settings.execution.executor is not None
    assert settings.execution.max_workers is None
    assert settings.networking.request_timeout == 5 * 60
    assert settings.networking.connect_timeout is None
    assert settings.persistence.consistency_timeout == 5.0


async def test_peering_namespaced_is_modified_by_clusterwide():
    settings = kopf.OperatorSettings()
    assert settings.peering.namespaced == True
    settings.peering.clusterwide = not settings.peering.clusterwide
    assert settings.peering.namespaced == False


async def test_peering_clusterwide_is_modified_by_namespaced():
    settings = kopf.OperatorSettings()
    assert settings.peering.clusterwide == False
    settings.peering.namespaced = not settings.peering.namespaced
    assert settings.peering.clusterwide == True


async def test_deprecated_batching_settings():
    settings = kopf.OperatorSettings()
    with pytest.warns(DeprecationWarning, match=r"Batching settings are now queueing settings."):
        assert settings.batching.worker_limit is None


async def test_deprecated_batch_window_is_still_persisted():
    settings = kopf.OperatorSettings()
    with pytest.warns(DeprecationWarning, match=r"Time-based event batching was removed."):
        assert settings.queueing.batch_window == 0.1
    with pytest.warns(DeprecationWarning, match=r"Time-based event batching was removed."):
        settings.queueing.batch_window = 123
    with pytest.warns(DeprecationWarning, match=r"Time-based event batching was removed."):
        assert settings.queueing.batch_window == 123  # persisted as set above
