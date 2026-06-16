import asyncio
import collections.abc
import concurrent.futures
import contextlib
import io
import multiprocessing.connection
import multiprocessing.synchronize
import os
import re
import shlex
import signal
import sys
import threading
import types
import warnings
from collections.abc import Callable, Collection
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

    .. deprecated:: 1.45.0

        ``KopfRunner`` is deprecated and discouraged: see :doc:`/testing`.

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
        warnings.warn(
            "KopfRunner is deprecated due to a design flaw (lack of import isolation). "
            "Use KopfCLI, KopfTask, or KopfThread as appropriate. "
            "See https://docs.kopf.dev/en/stable/testing/#legacy-runner for details.",
            DeprecationWarning,
            stacklevel=2,
        )
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


class KopfCLI:
    """
    A context manager to run a Kopf-based operator in a subprocess.

    Unlike :class:`KopfRunner` (which uses a thread + Click's CliRunner),
    this spawns a real child process, providing true import isolation.
    Modules are imported fresh, decorators fire, handlers register from scratch.
    This tests the operator exactly as it runs in production via ``kopf run``.

    Usage:

    .. code-block:: python

        from kopf.testing import KopfCLI

        def test_operator():
            with KopfCLI(['run', '-m', 'myoperator', '-A', '--verbose']) as runner:
                time.sleep(3)

            assert runner.exit_code == 0
            assert 'Operator started.' in runner.output
    """

    def __init__(
            self,
            args: str | list[str],
            /, *,
            signal: signal.Signals | int | None = None,
            reraise: bool = True,
            timeout: float = 10,
            spawn_timeout: float = 10,
    ) -> None:
        super().__init__()
        cli_args = shlex.split(args) if isinstance(args, str) else list(args)

        ctx = multiprocessing.get_context('spawn')
        self._signal = signal
        self._timeout = timeout
        self._reraise = reraise
        self._spawn_timeout = spawn_timeout
        self._stop_flag = ctx.Event()
        self._spawned_flag = ctx.Event()
        self._buffer = b''
        self._buffer_cond = threading.Condition()

        # Pipe for output capture. duplex=False: parent reads, child writes.
        self._parent_conn, self._child_conn = ctx.Pipe(duplex=False)

        # Pass stop_flag only in default mode; signal mode doesn't use it.
        stop_flag = self._stop_flag if self._signal is None else None
        self._process = ctx.Process(
            target=self._run_operator,
            args=(cli_args, stop_flag, self._spawned_flag, self._child_conn),
        )
        self._reader_thread = threading.Thread(target=self._read_output)

    def __enter__(self) -> "KopfCLI":
        self._process.start()

        # The parent must close the write-end so the reader sees EOF when the child exits.
        self._child_conn.close()
        self._reader_thread.start()

        # Spawning is costly (~1s): wait until the child has booted and is about to call main(),
        # so that subsequent tests' timing is not polluted by Python interpreter spawning.
        # If the child crashes or hangs before signalling, clean up and raise rather than
        # block the caller forever.
        if not self._spawned_flag.wait(timeout=self._spawn_timeout):
            if self._process.is_alive():
                self._process.kill()
                self._process.join()
            self._reader_thread.join()
            self._parent_conn.close()
            raise RuntimeError(
                f"The operator subprocess did not finish spawning within "
                f"{self._spawn_timeout}s. "
                f"Check runner.output for details."
            )

        return self

    def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: types.TracebackType | None,
    ) -> Literal[False]:

        # Request shutdown: always set the stop flag for parent-side consistency.
        self._stop_flag.set()
        signalled = False
        if self._process.pid is not None and self._signal is not None:
            if self._process.is_alive():
                os.kill(self._process.pid, self._signal)
                signalled = True

        # Wait for the process to exit gracefully.
        self._process.join(timeout=self._timeout)

        # Force-kill if still alive after timeout.
        killed = False
        if self._process.is_alive():
            self._process.kill()
            self._process.join()
            killed = True

        # Wait for the reader thread to drain remaining output.
        self._reader_thread.join()

        # Clean up the read-end fd.
        self._parent_conn.close()

        # An exit by signal only counts as normal if we are the ones who sent it.
        # An externally-delivered signal (OOM killer, shell kill, self-signal from
        # the operator, etc.) that matches self._signal by coincidence is abnormal.
        exit_code = self._process.exitcode
        is_normal_exit = exit_code == 0 or (
            signalled and self._signal is not None and exit_code == -self._signal
        )

        # Reraise if the process had to be force-killed or exited abnormally.
        if self._reraise and killed:
            raise RuntimeError(
                f"The operator did not exit gracefully within "
                f"{self._timeout}s and was force-killed. "
                f"Check runner.output for details."
            ) from exc_val
        if self._reraise and not is_normal_exit:
            raise RuntimeError(
                f"The operator process exited with code {exit_code}. "
                f"Check runner.output for details."
            ) from exc_val

        return False

    # NB: keep static, since it needs to be pickled and unpickled.
    @staticmethod
    def _run_operator(
            cli_args: list[str],
            stop_flag: multiprocessing.synchronize.Event | None,
            spawned_flag: multiprocessing.synchronize.Event,
            child_conn: multiprocessing.connection.Connection,
    ) -> None:
        # Redirect stdout and stderr to the pipe at the fd level.
        fd = child_conn.fileno()
        os.dup2(fd, sys.stdout.fileno())
        os.dup2(fd, sys.stderr.fileno())
        child_conn.close()

        # Hold explicit references to the original wrappers for the whole
        # subprocess lifetime: their underlying FileIO has closefd=True, so
        # garbage-collecting them would close fd 1/2 out from under our
        # replacements. We deliberately do not rely on sys.__stdout__ pinning,
        # which is CPython-specific and not guaranteed on other interpreters.
        _original_stdout = sys.stdout  # noqa: F841
        _original_stderr = sys.stderr  # noqa: F841

        # Rebuild sys.stdout and sys.stderr as fully-unbuffered text wrappers over
        # their now-redirected fds. The default BufferedWriter under sys.stdout
        # would otherwise hold writes until its buffer fills or the process exits,
        # reordering stdout relative to the (by default) unbuffered sys.stderr on
        # the shared pipe. With buffering=0 on the binary layer and write_through
        # on the text layer, every write — including partial ones — hits the pipe
        # immediately and in chronological order.
        sys.stdout = io.TextIOWrapper(
            open(sys.stdout.fileno(), 'wb', buffering=0, closefd=False),
            encoding='utf-8', write_through=True)
        sys.stderr = io.TextIOWrapper(
            open(sys.stderr.fileno(), 'wb', buffering=0, closefd=False),
            encoding='utf-8', write_through=True)

        # Signal the parent that spawning (interpreter boot) is done and the CLI is about to start.
        spawned_flag.set()

        # Invoke the CLI as if from the command line.
        # stop_flag is a multiprocessing.Event (default mode) or None (signal mode).
        ctxobj = cli.CLIControls(stop_flag=stop_flag)
        cli.main(args=cli_args, obj=ctxobj, standalone_mode=True)

    def _read_output(self) -> None:
        # No need to be thrifty: we keep the whole output in memory anyway, not streaming it.
        # But beware of full-size duplicates in memory: one in `data`, another in the buffer.
        chunk_size: int = 10240
        fd = self._parent_conn.fileno()
        while True:
            data: bytes = os.read(fd, chunk_size)

            # Even with EOF (empty data), notify the waiters for their final check on process exit.
            with self._buffer_cond:
                self._buffer += data
                self._buffer_cond.notify_all()

            # On EOF, stop the channeling thread.
            if not data:
                break

            # Save memory at runtime: reset before the next blocking read in case the chunk is huge.
            del data

    @property
    def exit_code(self) -> int | None:
        """
        The final exit code of the operator subprocess; ``None`` while running.
        """
        return self._process.exitcode

    @property
    def buffer(self) -> bytes:
        """
        The currently accumulated output buffer of the operator subprocess.

        Both streams (stdout + stderr) are mixed into one to avoid chronological
        discrepancies, i.e. when lines are consumed not in the order the happen.

        Unlike :prop:`~KopfCLI.output`, the buffer can contain partial lines
        as it consumes them from the stream in fixed-size chunks.

        The subprocess runs with unbuffered stdio, so every write from the
        operator is visible here immediately, without any explicit flush.
        """
        with self._buffer_cond:
            return self._buffer

    @property
    def output(self) -> str:
        """
        The currently or finally accumulated output of the operator subprocess.

        Both streams (stdout + stderr) are mixed into one to avoid chronological
        discrepancies, i.e. when lines are consumed not in the order the happen.

        Unlike :prop:`~KopfCLI.buffer`, there is a guarantee that the output
        contains only the whole lines (ends with a newline or the process exit).

        The subprocess runs with unbuffered stdio, so every write from the
        operator is visible here immediately, without any explicit flush.
        """
        with self._buffer_cond:
            return self._output

    @property
    def _output(self) -> str:
        # If exited or not started, return all output regardless of newlines (even partial lines).
        if not self._process.is_alive():
            return self._buffer.decode()

        # If still running, return only the whole lines, keep the partial lines for self.
        parts = self._buffer.rsplit(b'\n', maxsplit=1)
        if len(parts) == 2:
            whole, tail = parts
            whole += b'\n'
            return whole.decode()

        # If started but got no output yet (at least one line), return as if no whole lines yet.
        return ''

    def wait_for(
            self,
            v: Callable[[], bool] | str | bytes | Collection[str | bytes],
            /, *,
            timeout: float | None = None,
    ) -> bool:
        """
        Wait until a pattern appears or a condition is met in the output.

        Returns ``True`` if the condition was satisfied, ``False`` if the wait
        timed out **or** the subprocess exited without the condition becoming
        satisfied (in which case no new data can ever arrive).

        If a callable (no arguments), then it must return true when satisfied.
        It can safely use :prop:`~KopfCLI.output` and :prop:`~KopfCLI.buffer`.

        If a ``str`` or ``bytes``, then this is a regular expression matching
        the expected string (the wait compiles it internally for speed).
        Note the subtle difference: strings match against a whole-line output,
        while bytes match against the full buffer, including the partial lines.
        It is a rough shortcut for ``lambda: re.search(pattern, runner.output)``
        for ``str`` or the same with ``runner.buffer`` for ``bytes``.

        If a collection (tuple, list, set), then **all** of the patterns
        must match to wake up from the wait. Items may be a mix of strings
        and bytes; each item is matched with its own scope as above.
        An empty collection is considered matched immediately (vacuous truth).
        """
        matches: Callable[[], bool]
        match v:
            case str():
                pattern_s: re.Pattern[str] = re.compile(v)
                matches = lambda: bool(pattern_s.search(self._output))
            case bytes():
                pattern_b: re.Pattern[bytes] = re.compile(v)
                matches = lambda: bool(pattern_b.search(self._buffer))
            case collections.abc.Collection():
                patterns_s: set[re.Pattern[str]] = {re.compile(p) for p in v if isinstance(p, str)}
                patterns_b: set[re.Pattern[bytes]] = {re.compile(p) for p in v if isinstance(p, bytes)}
                matches = lambda: (
                    all(p.search(self._buffer) for p in patterns_b) and
                    all(p.search(self._output) for p in patterns_s)
                )
            case _ if callable(v):
                matches = v
            case _:
                raise ValueError(f"Unsupported pattern type: {v!r}")

        # Also wake up when the process is no longer alive — no more data can arrive,
        # so a predicate that has not matched by now will never match. After the wait
        # unblocks, we return whether the pattern actually matched (matches == True)
        # or we were released for another reason: timeout or process exit.
        with self._buffer_cond:
            self._buffer_cond.wait_for(lambda: matches() or not self._process.is_alive(), timeout=timeout)
            return matches()


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
