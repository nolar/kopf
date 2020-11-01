import asyncio
import concurrent.futures
import contextlib
import threading
import types
from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, cast

import click.testing
from typing_extensions import Literal

from kopf import cli
from kopf.reactor import registries
from kopf.structs import configuration

_ExcType = BaseException
_ExcInfo = Tuple[Type[_ExcType], _ExcType, types.TracebackType]

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

    Usage::

        from kopf.testing import KopfRunner

        with KopfRunner(['run', '-A', '--verbose', 'examples/01-minimal/example.py']) as runner:
            # do something while the operator is running.
            time.sleep(3)

        assert runner.exit_code == 0
        assert runner.exception is None
        assert 'And here we are!' in runner.stdout

    All the args & kwargs are passed directly to Click's invocation method.
    See: `click.testing.CliRunner`.
    All properties proxy directly to Click's `click.testing.Result` object
    when it is available (i.e. after the context manager exits).

    CLI commands have to be invoked in parallel threads, never in processes:

    First, with multiprocessing, they are unable to pickle and pass
    exceptions (specifically, their traceback objects)
    from a child thread (Kopf's CLI) to the parent thread (pytest).

    Second, mocking works within one process (all threads),
    but not across processes --- the mock's calls (counts, arrgs) are lost.
    """
    _future: ResultFuture

    def __init__(
            self,
            *args: Any,
            reraise: bool = True,
            timeout: Optional[float] = None,
            registry: Optional[registries.OperatorRegistry] = None,
            settings: Optional[configuration.OperatorSettings] = None,
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
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[types.TracebackType],
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
        if self._future.exception() is not None:
            if exc_val is None:
                raise self._future.exception()  # type: ignore
            else:
                raise self._future.exception() from exc_val  # type: ignore
        if self._future.result().exception is not None and self.reraise:
            if exc_val is None:
                raise self._future.result().exception
            else:
                raise self._future.result().exception from exc_val

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
                stop_flag=self._stop)
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
            # TODO: Try a hack: https://github.com/aio-libs/aiohttp/issues/1925#issuecomment-575754386
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
        return self.future.result().stderr_bytes

    @property
    def exit_code(self) -> int:
        return self.future.result().exit_code

    @property
    def exception(self) -> _ExcType:
        return cast(_ExcType, self.future.result().exception)

    @property
    def exc_info(self) -> _ExcInfo:
        return cast(_ExcInfo, self.future.result().exc_info)
