import asyncio
import concurrent.futures
import threading

import click.testing

from kopf import cli


class KopfRunner:
    """
    A context manager to run a Kopf-based operator in parallel with the tests.

    Usage::

        from kopf.testing import KopfRunner

        with KopfRunner(['run', '--verbose', 'examples/01-minimal/example.py']) as runner:
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

    def __init__(self, *args, reraise=True, timeout=None, **kwargs):
        super().__init__()
        self.args = args
        self.kwargs = kwargs
        self.reraise = reraise
        self.timeout = timeout
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()  # NB: not asyncio.Event!
        self._thread = threading.Thread(target=self._target)
        self._future = concurrent.futures.Future()

    def __enter__(self):
        self._thread.start()
        self._ready.wait()  # should be nanosecond-fast
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        # A coroutine that is injected into the loop to cancel everything in it.
        # Cancellations are caught in `run`, so that it exits gracefully.
        # TODO: also cancel/stop the streaming API calls in the thread executor.
        async def shutdown():
            current_task = asyncio.current_task()
            tasks = [task for task in asyncio.all_tasks() if task is not current_task]
            for task in tasks:
                task.cancel()

        # When the `with` block ends, shut down the parallel thread & loop
        # by cancelling all the tasks. Do not wait for the tasks to finish,
        # but instead wait for the thread+loop (CLI command) to finish.
        if self._loop.is_running():
            asyncio.run_coroutine_threadsafe(shutdown(), self._loop)
        self._thread.join(timeout=self.timeout)

        # If the thread is not finished, it is a bigger problem than exceptions.
        if self._thread.is_alive():
            raise Exception("The operator didn't stop, still running.")

        # Re-raise the exceptions of the threading & invocation logic.
        if self._future.exception() is not None:
            if exc_val is None:
                raise self._future.exception()
            else:
                raise self._future.exception() from exc_val
        if self._future.result().exception is not None and self.reraise:
            if exc_val is None:
                raise self._future.result().exception
            else:
                raise self._future.result().exception from exc_val

    def _target(self):

        # Every thread must have its own loop. The parent thread (pytest)
        # needs to know when the loop is set up, to be able to shut it down.
        asyncio.set_event_loop(self._loop)
        self._ready.set()

        # Execute the requested CLI command in the thread & thread's loop.
        # Remember the result & exception for re-raising in the parent thread.
        try:
            runner = click.testing.CliRunner()
            result = runner.invoke(cli.main, *self.args, **self.kwargs)
        except BaseException as e:
            self._future.set_exception(e)
        else:
            self._future.set_result(result)

    @property
    def future(self):
        return self._future

    @property
    def output(self):
        return self.future.result().output

    @property
    def stdout(self):
        return self.future.result().stdout

    @property
    def stdout_bytes(self):
        return self.future.result().stdout_bytes

    @property
    def stderr(self):
        return self.future.result().stderr

    @property
    def stderr_bytes(self):
        return self.future.result().stderr_bytes

    @property
    def exit_code(self):
        return self.future.result().exit_code

    @property
    def exception(self):
        return self.future.result().exception

    @property
    def exc_info(self):
        return self.future.result().exc_info
