import logging

import kopf


async def test_declared_public_interface_and_promised_defaults():
    settings = kopf.OperatorSettings()
    assert settings.posting.level == logging.INFO
    assert settings.watching.retry_delay == 0.1
    assert settings.watching.stream_timeout is None
    assert settings.watching.session_timeout is None
    assert settings.batching.worker_limit is None
    assert settings.batching.idle_timeout == 5.0
    assert settings.batching.exit_timeout == 2.0
    assert settings.batching.batch_window == 0.1
