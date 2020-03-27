import concurrent.futures
import threading
from unittest.mock import MagicMock

import kopf
from kopf.engines.posting import settings_var
from kopf.reactor.invocation import invoke


class CatchyExecutor(concurrent.futures.ThreadPoolExecutor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = []

    def submit(self, fn, *args, **kwargs):
        self.calls.append(fn)
        return super().submit(fn, *args, **kwargs)


async def test_synchronous_calls_are_threaded():
    settings = kopf.OperatorSettings()
    thread = None

    def fn():
        nonlocal thread
        thread = threading.current_thread()

    mock = MagicMock(wraps=fn)
    await invoke(fn=mock, settings=settings)

    assert mock.called
    assert thread is not None  # remembered from inside fn()
    assert thread is not threading.current_thread()  # not in the main thread


async def test_synchronous_calls_use_replaced_executor():
    settings = kopf.OperatorSettings()
    executor = CatchyExecutor()
    settings.execution.executor = executor

    mock = MagicMock()
    await invoke(fn=mock, settings=settings)

    assert mock.called
    assert len(executor.calls) == 1


async def test_synchronous_executor_limit_is_applied():
    settings = kopf.OperatorSettings()
    assert hasattr(settings.execution.executor, '_max_workers')  # prerequisite

    assert settings.execution.max_workers is None  # as in "unset by us, assume defaults"
    assert settings.execution.executor._max_workers is not None  # usually CPU count * N.

    settings.execution.max_workers = 123456

    assert settings.execution.max_workers == 123456
    assert settings.execution.executor._max_workers == 123456


async def test_synchronous_executor_limit_is_applied_legacy_way():
    settings = kopf.OperatorSettings()
    assert hasattr(settings.execution.executor, '_max_workers')  # prerequisite

    assert settings.execution.max_workers is None  # as in "unset by us, assume defaults"
    assert settings.execution.executor._max_workers is not None  # usually CPU count * N.

    settings_var.set(settings)  # an assumption on the implementation
    kopf.config.WorkersConfig.set_synchronous_tasks_threadpool_limit(123456)

    assert settings.execution.max_workers == 123456
    assert settings.execution.executor._max_workers == 123456
