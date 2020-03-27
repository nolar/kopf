import logging

import kopf


async def test_declared_public_interface_and_promised_defaults():
    settings = kopf.OperatorSettings()
    assert settings.posting.level == logging.INFO
    assert settings.watching.reconnect_backoff == 0.1
    assert settings.watching.connect_timeout is None
    assert settings.watching.server_timeout is None
    assert settings.watching.client_timeout is None
    assert settings.batching.worker_limit is None
    assert settings.batching.idle_timeout == 5.0
    assert settings.batching.exit_timeout == 2.0
    assert settings.batching.batch_window == 0.1
    assert settings.execution.executor is not None
    assert settings.execution.max_workers is None
