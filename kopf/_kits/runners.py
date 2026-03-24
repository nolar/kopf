import asyncio
import concurrent.futures
import contextlib
import threading
import types
from collections.abc import Collection
from typing import TYPE_CHECKING, Any, Literal, cast

import click.testing

from kopf import cli
from kopf._cogs.aiokits import aioadapters
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import aiohttpcaps
from kopf._cogs.structs import credentials, references
from kopf._core.actions import execution
from kopf._core.engines import indexing, peering
from kopf._core.intents import registries
from kopf._core.reactor import inventory, running

_ExcType = BaseException
_ExcInfo = tuple[type[_ExcType], _ExcType, types.TracebackType]

if TYPE_CHECKING:
    ResultFuture = concurrent.futures.Future[click.testing.Result]
    class _AbstractKopfRunner(contextlib.AbstractContextManager["_AbstractKopfRunner"]):
        pass
else:
    ResultFuture = concurrent.futures.Future
    class _AbstractKopfRunner(contextlib.AbstractContextManager):
        pass


class KopfRunner(_AbstractKopfRunner):
    """
    A context manager to run a Kopf-based operator in parallel with the tests.

    Usage:

    .. code-block:: python

        from kopf.testing import KopfRunner

        def test_operator():
            with KopfRunner(['run', '-A', '--verbose', 'examples/01-minimal/example.py']) as runner:
                # do something while the operator is running.
                time.sleep(3)

            assert runner.exit_code == 0
            assert runner.exception is None
            assert 'And here we are!' in runner.output

    All the args & kwargs are passed directly to Click's invocation method.
    See: :class:`click.testing.CliRunner`.
    All properties proxy directly to Click's :class:`click.testing.Result`
    when it is available (i.e. after the context manager exits).

    CLI commands have to be invoked in parallel threads, never in processes:

    First, with multiprocessing, they are unable to pickle and pass
    exceptions (specifically, their traceback objects)
    from a child thread (Kopf's CLI) to the parent thread (pytest).

    Second, mocking works within one process (all threads),
    but not across processes --- the mock's calls (counts, args) are lost.
    """
    _future: ResultFuture

    def __init__(
            self,
            *args: Any,
            reraise: bool = True,
            timeout: float | None = None,
            registry: registries.OperatorRegistry | None = None,
            settings: configuration.OperatorSettings | None = None,
            **kwargs: Any,
    ):
        super().__init__()
        self.args = args
        self.kwargs = kwargs
        self.reraise = reraise
        self.timeout = timeout
        self.registry = registry
        self.settings = settings
        self._stop = threading.Event()
        self._ready = threading.Event()  # NB: not asyncio.Event!
        self._thread = threading.Thread(target=self._target)
        self._future = concurrent.futures.Future()

    def __enter__(self) -> "KopfRunner":
        self._thread.start()
        self._ready.wait()  # should be nanosecond-fast
        return self

    def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: types.TracebackType | None,
    ) -> Literal[False]:

        # When the `with` block ends, shut down the parallel thread & loop
        # by cancelling all the tasks. Do not wait for the tasks to finish,
        # but instead wait for the thread+loop (CLI command) to finish.
        self._stop.set()
        self._thread.join(timeout=self.timeout)

        # If the thread is not finished, it is a bigger problem than exceptions.
        if self._thread.is_alive():
            raise Exception("The operator didn't stop, still running.")

        # Re-raise the exceptions of the threading & invocation logic.
        e = self._future.exception()
        if e is not None:
            if exc_val is None:
                raise e
            else:
                raise e from exc_val
        e = self._future.result().exception
        if e is not None and self.reraise:
            if exc_val is None:
                raise e
            else:
                raise e from exc_val

        return False

    def _target(self) -> None:

        # Every thread must have its own loop. The parent thread (pytest)
        # needs to know when the loop is set up, to be able to shut it down.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._ready.set()

        # Execute the requested CLI command in the thread & thread's loop.
        # Remember the result & exception for re-raising in the parent thread.
        try:
            ctxobj = cli.CLIControls(
                registry=self.registry,
                settings=self.settings,
                stop_flag=self._stop,
                loop=loop)
            runner = click.testing.CliRunner()
            result = runner.invoke(cli.main, *self.args, **self.kwargs, obj=ctxobj)
        except BaseException as e:
            self._future.set_exception(e)
        else:
            self._future.set_result(result)
        finally:

            # Shut down the API-watching streams.
            loop.run_until_complete(loop.shutdown_asyncgens())

            # Shut down the transports and prevent ResourceWarning: unclosed transport.
            # See: https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
            # Fixed in aiohttp 3.12.4; the sleep is only needed for older versions.
            if not aiohttpcaps.AIOHTTP_HAS_GRACEFUL_SHUTDOWN:
                loop.run_until_complete(asyncio.sleep(1.0))

            loop.close()

    @property
    def future(self) -> ResultFuture:
        return self._future

    @property
    def output(self) -> str:
        return self.future.result().output

    @property
    def stdout(self) -> str:
        return self.future.result().stdout

    @property
    def stdout_bytes(self) -> bytes:
        return self.future.result().stdout_bytes

    @property
    def stderr(self) -> str:
        return self.future.result().stderr

    @property
    def stderr_bytes(self) -> bytes:
        return self.future.result().stderr_bytes or b''

    @property
    def exit_code(self) -> int:
        return self.future.result().exit_code

    @property
    def exception(self) -> _ExcType:
        return cast(_ExcType, self.future.result().exception)

    @property
    def exc_info(self) -> _ExcInfo:
        return cast(_ExcInfo, self.future.result().exc_info)


class KopfThread:
    """
    A context manager to run a Kopf operator in a background thread.

    Unlike :class:`KopfRunner`, this enters the operator programmatically
    via :func:`kopf.operator` rather than through the CLI.

    Usage:

    .. code-block:: python

        import kopf
        from kopf.testing import KopfThread

        def test_operator():
            settings = kopf.OperatorSettings()
            settings.scanning.disabled = True
            with KopfThread(namespaces=['ns1'], settings=settings):
                # do something while the operator is running.
                time.sleep(3)
    """
    _future: concurrent.futures.Future[None]

    def __init__(
            self,
            *,
            # All operator() kwargs, explicitly enumerated with types.
            # TODO: Switch to Unpack[OperatorParams] when Python 3.10 is dropped (Oct 2026).
            lifecycle: execution.LifeCycleFn | None = None,
            indexers: indexing.OperatorIndexers | None = None,
            registry: registries.OperatorRegistry | None = None,
            settings: configuration.OperatorSettings | None = None,
            memories: inventory.ResourceMemories | None = None,
            insights: references.Insights | None = None,
            identity: peering.Identity | None = None,
            standalone: bool | None = None,
            priority: int | None = None,
            peering_name: str | None = None,
            liveness_endpoint: str | None = None,
            clusterwide: bool = False,
            namespaces: Collection[references.NamespacePattern] = (),
            namespace: references.NamespacePattern | None = None,
            stop_flag: aioadapters.Flag | None = None,
            ready_flag: aioadapters.Flag | None = None,
            vault: credentials.Vault | None = None,
            memo: object | None = None,
            # KopfThread's own kwargs:
            timeout: float | None = None,
            reraise: bool = True,
    ) -> None:
        super().__init__()
        self._lifecycle = lifecycle
        self._indexers = indexers
        self._registry = registry
        self._settings = settings
        self._memories = memories
        self._insights = insights
        self._identity = identity
        self._standalone = standalone
        self._priority = priority
        self._peering_name = peering_name
        self._liveness_endpoint = liveness_endpoint
        self._clusterwide = clusterwide
        self._namespaces = namespaces
        self._namespace = namespace
        self._stop_flag: aioadapters.Flag = stop_flag if stop_flag is not None else threading.Event()
        self._ready_flag = ready_flag
        self._vault = vault
        self._memo = memo
        self.timeout = timeout
        self.reraise = reraise
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread = threading.Thread(target=self._target)
        self._future: concurrent.futures.Future[None] = concurrent.futures.Future()

        # Internal sync of the caller's thread and the operator thread.
        self._init_flag = threading.Event()
        self._exit_flag = threading.Event()
        self._exit_lock = threading.Lock()

    def __enter__(self) -> "KopfThread":
        self._thread.start()
        self._init_flag.wait()
        return self

    def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: types.TracebackType | None,
    ) -> Literal[False]:
        assert self._loop is not None

        # Check if our coroutine will run at all — in case the operator failed/exited much earlier.
        # Otherwise, we get `RuntimeWarning: coroutine 'raise_flag' was never awaited` in CI.
        # The lock protects against the flag externally set between our check and coro injection.
        future: concurrent.futures.Future[None] | None = None
        with self._exit_lock:
            if not self._exit_flag.is_set():
                coro = aioadapters.raise_flag(self._stop_flag)
                future = asyncio.run_coroutine_threadsafe(coro, self._loop)

        # Not really needed, but for extra code safety: wait that the instant coroutine is finished.
        # The wait is outside the lock; otherwise, it blocks the `finally` block in the thread
        # when the loop surely does not run, so the future will never be set.
        if future is not None:
            future.result()  # usually instant

        # Regardless of the exiting reason (success or failure), let the thread finish properly.
        self._thread.join(timeout=self.timeout)

        # If the thread is not finished, it is a bigger problem than exceptions.
        if self._thread.is_alive():
            raise Exception("The operator didn't stop, still running.")

        # Re-raise the exceptions of the underlying operator.
        e = self._future.exception()
        if e is not None:
            if self.reraise:
                if exc_val is None:
                    raise e
                else:
                    raise e from exc_val
        return False

    def _target(self) -> None:
        # TODO: Switch to asyncio.Runner when Python 3.10 is dropped (Oct 2026).
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._init_flag.set()
        try:
            loop.run_until_complete(running.operator(
                lifecycle=self._lifecycle,
                indexers=self._indexers,
                registry=self._registry,
                settings=self._settings,
                memories=self._memories,
                insights=self._insights,
                identity=self._identity,
                standalone=self._standalone,
                priority=self._priority,
                peering_name=self._peering_name,
                liveness_endpoint=self._liveness_endpoint,
                clusterwide=self._clusterwide,
                namespaces=self._namespaces,
                namespace=self._namespace,
                stop_flag=self._stop_flag,
                ready_flag=self._ready_flag,
                vault=self._vault,
                memo=self._memo,
            ))
        except BaseException as e:
            self._future.set_exception(e)
        else:
            self._future.set_result(None)
        finally:
            # The caller's thread injects a coroutine that we want to surely await.
            # But only if the caller injected it before us here. If after, skip the injection there.
            with self._exit_lock:
                self._exit_flag.set()
                loop.run_until_complete(asyncio.sleep(0))

            # The usual event loop cleanup routines.
            loop.run_until_complete(loop.shutdown_asyncgens())

            # Shut down the transports and prevent ResourceWarning: unclosed transport.
            # See: https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
            # Fixed in aiohttp 3.12.4; the sleep is only needed for older versions.
            if not aiohttpcaps.AIOHTTP_HAS_GRACEFUL_SHUTDOWN:
                loop.run_until_complete(asyncio.sleep(1.0))
            loop.close()


class KopfTask:
    """
    An async context manager to run a Kopf operator as a background asyncio task.

    Unlike :class:`KopfRunner`, this enters the operator programmatically
    via :func:`kopf.operator` rather than through the CLI.

    Usage:

    .. code-block:: python

        import kopf
        from kopf.testing import KopfTask

        async def test_operator():
            settings = kopf.OperatorSettings()
            settings.scanning.disabled = True
            async with KopfTask(namespaces=['ns1'], settings=settings):
                # do something while the operator is running.
                pass
    """

    def __init__(
            self,
            *,
            # All operator() kwargs, explicitly enumerated with types.
            # TODO: Switch to Unpack[OperatorParams] when Python 3.10 is dropped (Oct 2026).
            lifecycle: execution.LifeCycleFn | None = None,
            indexers: indexing.OperatorIndexers | None = None,
            registry: registries.OperatorRegistry | None = None,
            settings: configuration.OperatorSettings | None = None,
            memories: inventory.ResourceMemories | None = None,
            insights: references.Insights | None = None,
            identity: peering.Identity | None = None,
            standalone: bool | None = None,
            priority: int | None = None,
            peering_name: str | None = None,
            liveness_endpoint: str | None = None,
            clusterwide: bool = False,
            namespaces: Collection[references.NamespacePattern] = (),
            namespace: references.NamespacePattern | None = None,
            stop_flag: aioadapters.Flag | None = None,
            ready_flag: aioadapters.Flag | None = None,
            vault: credentials.Vault | None = None,
            memo: object | None = None,
            # KopfTask's own kwargs:
            timeout: float | None = None,
            reraise: bool = True,
    ) -> None:
        super().__init__()
        self._lifecycle = lifecycle
        self._indexers = indexers
        self._registry = registry
        self._settings = settings
        self._memories = memories
        self._insights = insights
        self._identity = identity
        self._standalone = standalone
        self._priority = priority
        self._peering_name = peering_name
        self._liveness_endpoint = liveness_endpoint
        self._clusterwide = clusterwide
        self._namespaces = namespaces
        self._namespace = namespace
        self._stop_flag: aioadapters.Flag = stop_flag if stop_flag is not None else asyncio.Event()
        self._ready_flag = ready_flag
        self._vault = vault
        self._memo = memo
        self.timeout = timeout
        self.reraise = reraise
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> "KopfTask":
        self._task = asyncio.create_task(running.operator(
            lifecycle=self._lifecycle,
            indexers=self._indexers,
            registry=self._registry,
            settings=self._settings,
            memories=self._memories,
            insights=self._insights,
            identity=self._identity,
            standalone=self._standalone,
            priority=self._priority,
            peering_name=self._peering_name,
            liveness_endpoint=self._liveness_endpoint,
            clusterwide=self._clusterwide,
            namespaces=self._namespaces,
            namespace=self._namespace,
            stop_flag=self._stop_flag,
            ready_flag=self._ready_flag,
            vault=self._vault,
            memo=self._memo,
        ))
        return self

    async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: types.TracebackType | None,
    ) -> Literal[False]:
        assert self._task is not None
        await aioadapters.raise_flag(self._stop_flag)
        _, pending = await asyncio.wait({self._task}, timeout=self.timeout)

        # If the thread is not finished, it is a bigger problem than exceptions.
        if pending:
            raise Exception("The operator didn't stop, still running.")

        # Re-raise the exceptions of the underlying operator.
        e = self._task.exception() if not self._task.cancelled() else None
        if e is not None:
            if self.reraise:
                if exc_val is None:
                    raise e
                else:
                    raise e from exc_val
        return False
